import duckdb
import pandas as pd
import os
import subprocess
from datetime import datetime
import json
import re
import requests
from dotenv import load_dotenv
import sys
from typing import Union, List, Dict
import csv

# --- Determine script's directory for robust pathing ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))

# Load environment variables from .env file in the script folder
dotenv_path = os.path.join(SCRIPT_DIR, '.env')
load_dotenv(dotenv_path=dotenv_path)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_APP_NAME = "Estadisticas del Circuito de Carreras Populares de Cuenca"
OPENROUTER_APP_URL = "http://circuitocarrerascuenca.franloza.com"

# --- Configuration (paths relative to project root) ---
CIRCUIT_RACES_PATH = os.path.join(PROJECT_ROOT, 'data', 'circuit_races.csv')
RAW_RESULTS_PARQUET_PATH = os.path.join(PROJECT_ROOT, 'data', 'raw', 'race_results.parquet') 
ELT_EXTRACT_SCRIPT_PATH = os.path.join(PROJECT_ROOT, 'elt', 'extract.py')
ELT_TRANSFORM_SCRIPT_PATH = os.path.join(PROJECT_ROOT, 'elt', 'transform.py')

OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
LLM_MODEL = "google/gemini-2.0-flash-001" # Faster, but not free. Use for development
RACES_FROM_YEAR = datetime.now().year 

def run_elt_scripts():
    """Runs the extract and transform scripts."""
    try:
        print(f"Running {ELT_EXTRACT_SCRIPT_PATH}...")
        subprocess.run([sys.executable, ELT_EXTRACT_SCRIPT_PATH], check=True, capture_output=True, text=True, cwd=PROJECT_ROOT)
        print(f"{ELT_EXTRACT_SCRIPT_PATH} completed.")
        
        print(f"Running {ELT_TRANSFORM_SCRIPT_PATH}...")
        subprocess.run([sys.executable, ELT_TRANSFORM_SCRIPT_PATH], check=True, capture_output=True, text=True, cwd=PROJECT_ROOT)
        print(f"{ELT_TRANSFORM_SCRIPT_PATH} completed.")
    except subprocess.CalledProcessError as e:
        print(f"Error running ELT scripts: {e}", file=sys.stderr)
        print(f"Stdout: {e.stdout}", file=sys.stderr)
        print(f"Stderr: {e.stderr}", file=sys.stderr)
        raise

def get_placeholders_from_csv(year: int) -> pd.DataFrame:
    """Loads circuit_races.csv and identifies placeholders for the given year."""
    try:
        df = pd.read_csv(CIRCUIT_RACES_PATH, dtype={'race_name': str, 'race_number': 'Int64'}) # Keep NA for numbers
        placeholders = df[
            (df['race_year'] == year) & 
            (df['race_name'].isnull() | (df['race_name'].str.strip() == ''))
        ].copy()
        if placeholders.empty:
            print(f"No placeholder rows found in {CIRCUIT_RACES_PATH} for year {year} with empty race_name.")
        else:
            # Create a unique identifier for each placeholder for matching LLM response
            placeholders['placeholder_identifier'] = (
                placeholders['race_slug'].astype(str) + "_" + 
                placeholders['race_year'].astype(str) + "_" + 
                placeholders['race_number'].astype(str)
            )
            print(f"Found {len(placeholders)} placeholder row(s) for year {year}.")
        return placeholders
    except FileNotFoundError:
        print(f"{CIRCUIT_RACES_PATH} not found. Cannot identify placeholders.", file=sys.stderr)
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading or processing {CIRCUIT_RACES_PATH}: {e}", file=sys.stderr)
        return pd.DataFrame()

def get_actual_races_from_parquet(year: int) -> pd.DataFrame:
    """Fetches distinct race names and dates from the raw parquet file for the given year."""
    if not os.path.exists(RAW_RESULTS_PARQUET_PATH):
        print(f"Raw results parquet not found at {RAW_RESULTS_PARQUET_PATH}.", file=sys.stderr)
        return pd.DataFrame()
    
    con = None
    try:
        con = duckdb.connect()
        query = f"""
        SELECT DISTINCT
            regexp_replace(NombreCarreraT01, '\\s+', ' ', 'g') AS actual_race_name, 
            MAX(FechaCarreraT01) AS actual_race_date 
        FROM read_parquet('{RAW_RESULTS_PARQUET_PATH}')
        WHERE strftime(FechaCarreraT01, '%Y')::INTEGER = {year}
          AND try_cast(DistanciaMetrosTotalT01 as int) > 5000
        GROUP BY 1;
        """
        actual_races_df = con.execute(query).fetchdf()
        actual_races_df.dropna(subset=['actual_race_name', 'actual_race_date'], inplace=True)
        if actual_races_df.empty:
            print(f"No actual races found in {RAW_RESULTS_PARQUET_PATH} for year {year}.")
        else:
            print(f"Found {len(actual_races_df)} distinct actual races in parquet for year {year}.")
        return actual_races_df
    except Exception as e:
        print(f"Error querying races from {RAW_RESULTS_PARQUET_PATH}: {e}", file=sys.stderr)
        return pd.DataFrame()
    finally:
        if con:
            con.close()

def get_historical_races_from_csv(current_year: int, years_to_go_back: int = 2) -> pd.DataFrame:
    """Loads circuit_races.csv and retrieves named races from the specified previous years."""
    try:
        df = pd.read_csv(CIRCUIT_RACES_PATH, dtype={'race_name': str, 'race_number': 'Int64'})
        
        historical_races_list = []
        for i in range(1, years_to_go_back + 1):
            year_to_fetch = current_year - i
            year_df = df[
                (df['race_year'] == year_to_fetch) &
                (df['race_name'].notnull()) & 
                (df['race_name'].str.strip() != '')
            ].copy() # Ensure we are working with a copy
            if not year_df.empty:
                historical_races_list.append(year_df)
        
        if not historical_races_list:
            print(f"No historical race data found in {CIRCUIT_RACES_PATH} for the {years_to_go_back} previous year(s) from {current_year}.")
            return pd.DataFrame()
            
        historical_races_df = pd.concat(historical_races_list)
        print(f"Found {len(historical_races_df)} historical race entries from the past {years_to_go_back} year(s).")
        return historical_races_df[['race_year', 'race_number', 'race_name', 'race_location', 'race_slug']] # Select relevant columns
        
    except FileNotFoundError:
        print(f"{CIRCUIT_RACES_PATH} not found. Cannot retrieve historical races.", file=sys.stderr)
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading or processing {CIRCUIT_RACES_PATH} for historical data: {e}", file=sys.stderr)
        return pd.DataFrame()

def format_placeholders_for_prompt(placeholders_df: pd.DataFrame) -> str:
    if placeholders_df.empty:
        return "No placeholders to format."
    prompt_list = []
    for _, row in placeholders_df.iterrows():
        prompt_list.append(
            f"- Placeholder ID: \"{row['placeholder_identifier']}\", Race Number: {row['race_number']}, Location: \"{row['race_location']}\", Slug: \"{row['race_slug']}\", Year: {row['race_year']}"
        )
    return "\n".join(prompt_list)

def format_actual_races_for_prompt(actual_races_df: pd.DataFrame) -> str:
    if actual_races_df.empty:
        return "No actual races from this year available for comparison."
    context_list = []
    for _, row in actual_races_df.iterrows():
        date_str = pd.to_datetime(row['actual_race_date']).strftime('%Y-%m-%d')
        print(row['actual_race_name'])
        context_list.append(f"- Official Name: \"{row['actual_race_name']}\", Date: {date_str}")
    return "\n".join(context_list)

def format_historical_races_for_prompt(historical_races_df: pd.DataFrame) -> str:
    if historical_races_df.empty:
        return "No historical race data available for context."
    prompt_list = []
    # Sort by year (desc) then race_number (asc) for consistent prompting
    historical_races_df = historical_races_df.sort_values(by=['race_year', 'race_number'], ascending=[False, True])
    for _, row in historical_races_df.iterrows():
        prompt_list.append(
            f"- Year: {row['race_year']}, Race Number: {row['race_number']}, Name: \"{row['race_name']}\", Location: \"{row['race_location']}\", Slug: \"{row['race_slug']}\""
        )
    return "\n".join(prompt_list)

def call_llm_for_bulk_race_name_matching(placeholders_prompt_list: str, actual_races_prompt_list: str, historical_races_prompt_list: str, year: int) -> Union[List[Dict[str, str]], None]:
    """Calls LLM once to match all placeholders with actual race names."""
    if not OPENROUTER_API_KEY:
        raise ValueError("OPENROUTER_API_KEY not found. Cannot call LLM.")

    prompt = f'''
You are an expert assistant for managing running race data for a circuit schedule. Your task is to accurately match scheduled race placeholders with official race names from a list of races that have actually occurred.

**CRITICAL MATCHING RULES - FOLLOW THESE STRICTLY:**
1.  **Prioritize Location:** The `race_location` from the 'Scheduled Race Placeholders' is the MOST IMPORTANT factor. The `matched_race_name` you select MUST correspond to this location. If an actual race name clearly indicates it's for a different town/city than the placeholder\'s `race_location`, DO NOT match them.
2.  **Use Slugs:** The `race_slug` from the placeholder is the second most important factor. Use it to confirm matches.
3.  **Verbatim Names Only:** The `matched_race_name` MUST be an EXACT, VERBATIM copy of an 'Official Name' from the 'List of Actual Races that Occurred in {{year}}'. Do NOT alter, abbreviate, or invent names. If you see minor variations (e.g. 'KID' vs 'KIDS'), only select the name if it\'s an exact match from the provided list.
4.  **Consider Race Number for Order:** The `race_number` suggests a general order but is less critical than location and slug for direct name matching.
5.  **Historical Data for Context:** The 'Historical Race Data' is provided for context on naming patterns but the primary matching MUST be against the 'List of Actual Races that Occurred in {{year}}'.

**DATA SECTIONS:**

1.  **Historical Race Data (from previous years):**
{historical_races_prompt_list}

2.  **Scheduled Race Placeholders for {{year}} (Pay close attention to `race_location`, `race_slug`, and `placeholder_identifier`):**
{placeholders_prompt_list}

3.  **List of Actual Races that Occurred in {{year}} (This is your ONLY source for `matched_race_name`):**
{actual_races_prompt_list}

**Your Task:**
For each placeholder in 'Scheduled Race Placeholders for {{year}}', identify the single best `matched_race_name` from the 'List of Actual Races that Occurred in {{year}}', strictly following the CRITICAL MATCHING RULES above.

**Output Format (JSON Array Only):**
Please provide your response STRICTLY as a JSON array of objects. The entire response should be this JSON array and nothing else.
Each object in the array MUST contain exactly these two keys:
1.  "placeholder_identifier": The exact `placeholder_identifier` from the list of placeholders.
2.  "matched_race_name": The verbatim 'Official Name' selected from the 'List of Actual Races that Occurred in {{year}}'.

Example of the exact expected JSON output format:
```json
[
  {{
    "placeholder_identifier": "mota-del-cuervo_2024_3",
    "matched_race_name": "OFFICIAL NAME FROM ACTUAL RACES LIST FOR MOTA DEL CUERVO"
  }},
  {{
    "placeholder_identifier": "tarancon_2024_8",
    "matched_race_name": "OFFICIAL NAME FROM ACTUAL RACES LIST FOR TARANCON"
  }}
  // ... and so on for all placeholders you can confidently and accurately match
]
```

**Handling No Match:**
If you cannot confidently match a placeholder to an *existing and appropriate* race name from the 'List of Actual Races that Occurred in {{year}}' according to ALL the rules (especially location and verbatim name), then DO NOT include that placeholder in your JSON response. It is better to omit a placeholder than to provide an incorrect match. If no placeholders can be matched, return an empty JSON array: `[]`.

Do NOT include any other text, explanations, or apologies before or after the JSON array.
'''

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}", 
        "Content-Type": "application/json",
        "HTTP-Referer": OPENROUTER_APP_URL, 
        "X-Title": OPENROUTER_APP_NAME 
    }
    data = {
        "model": LLM_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1, 
        "max_tokens": 3000, # Adjust if necessary, for potentially long lists
    }

    try:
        # print(f"LLM Bulk Prompt:\n{prompt}...") # Debug
        response = requests.post(OPENROUTER_API_URL, headers=headers, json=data, timeout=180) # Increased timeout for potentially larger payload
        response.raise_for_status()
        completion = response.json()
        ai_response_content = completion.get('choices', [{}])[0].get('message', {}).get('content', '{}')
        # print(f"LLM Bulk Raw Response: {ai_response_content}") # Debug

        match_json = re.search(r'```json\n(.*?)\n```', ai_response_content, re.DOTALL)
        json_str = match_json.group(1) if match_json else ai_response_content
        
        matched_details_list = json.loads(json_str)
        if isinstance(matched_details_list, list):
            # Basic validation of list elements
            validated_list = []
            for item in matched_details_list:
                if isinstance(item, dict) and 'placeholder_identifier' in item and 'matched_race_name' in item:
                    validated_list.append(item)
                else:
                    print(f"Warning: LLM returned a structurally malformed item (e.g., not a dict, or missing 'placeholder_identifier'/'matched_race_name' keys): {item}", file=sys.stderr)
            return validated_list
        else:
            print(f"LLM bulk response was not a list as expected: {matched_details_list}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"Error during LLM bulk call: {e}", file=sys.stderr)
        return None

def main():
    changes_made = False
    try:
        run_elt_scripts() # User has this commented out, acknowledging for now.

        print(f"Identifying placeholder races for year {RACES_FROM_YEAR}...")
        placeholders_df = get_placeholders_from_csv(RACES_FROM_YEAR)

        if placeholders_df.empty:
            print(f"No placeholder rows (empty race_name) found in {CIRCUIT_RACES_PATH} for year {RACES_FROM_YEAR}.")
            print("This may mean the CSV needs to be updated with the new year's race schedule placeholders.")
            print("changes_made=false")
            return

        print(f"Fetching actual races for year {RACES_FROM_YEAR} from parquet...")
        actual_races_df = get_actual_races_from_parquet(RACES_FROM_YEAR)

        if actual_races_df.empty:
            print("No actual races data found from parquet. Cannot match placeholders.")
            print("changes_made=false")
            return
        
        placeholders_prompt_list = format_placeholders_for_prompt(placeholders_df)
        actual_races_prompt_list = format_actual_races_for_prompt(actual_races_df)

        print("Calling LLM for bulk race name matching...")
        historical_races_df = get_historical_races_from_csv(RACES_FROM_YEAR)
        historical_races_prompt_list = format_historical_races_for_prompt(historical_races_df)
        llm_matched_races = call_llm_for_bulk_race_name_matching(placeholders_prompt_list, actual_races_prompt_list, historical_races_prompt_list, RACES_FROM_YEAR)

        if not llm_matched_races:
            print("LLM did not return any matches or an error occurred.")
            print("changes_made=false")
            return
        
        print(f"LLM returned {len(llm_matched_races)} matches.")

        # Load the full circuit_races.csv to update it
        try:
            full_circuit_df = pd.read_csv(CIRCUIT_RACES_PATH, dtype={'race_name': str, 'race_number': 'Int64'})
        except FileNotFoundError:
            print(f"Critical error: {CIRCUIT_RACES_PATH} not found at update stage.", file=sys.stderr)
            print("changes_made=false")
            return
        except Exception as e:
            print(f"Error reading {CIRCUIT_RACES_PATH} for update: {e}", file=sys.stderr)
            print("changes_made=false")
            return

        # Create a set of valid actual race names for quick validation
        valid_actual_race_names = set(actual_races_df['actual_race_name'].unique())

        # Create a mapping from placeholder_identifier to its original index for quick updates
        # Ensure placeholders_df index is the same as in full_circuit_df for these rows
        placeholder_map = {row['placeholder_identifier']: index for index, row in placeholders_df.iterrows()}
        
        # Create a set of existing race names to prevent duplicates
        # We'll check for duplicates based on race_name only (case-insensitive)
        existing_race_names = set()
        for _, row in full_circuit_df.iterrows():
            if pd.notna(row['race_name']) and str(row['race_name']).strip():
                race_name_normalized = str(row['race_name']).strip().upper()
                existing_race_names.add(race_name_normalized)

        for match in llm_matched_races:
            placeholder_id = match.get('placeholder_identifier')
            matched_name = match.get('matched_race_name') # This can be None if the LLM explicitly sets it so

            if not placeholder_id or not isinstance(placeholder_id, str) or not placeholder_id.strip():
                print(f"Skipping LLM response item due to missing or invalid placeholder_identifier: {match}", file=sys.stderr)
                continue

            # Case 1: LLM explicitly stated no match was found for this placeholder
            if matched_name is None:
                print(f"  Info: LLM found no match for placeholder ID {placeholder_id}. No update will be made for this placeholder.")
                continue
            
            # Case 2: matched_name is present but might be an empty string or not a string
            if not isinstance(matched_name, str) or not matched_name.strip():
                print(f"  Warning: LLM returned an empty or invalid matched_race_name for placeholder ID {placeholder_id}: '{matched_name}'. Skipping this match.", file=sys.stderr)
                continue
            
            cleaned_matched_name = str(matched_name).strip()

            # --- Crucial Validation Step (against actual races from parquet) ---
            if cleaned_matched_name not in valid_actual_race_names:
                print(f"  Warning: LLM returned a matched_race_name \"{cleaned_matched_name}\" for placeholder ID {placeholder_id} that is NOT in the list of actual races from parquet. Skipping this match.", file=sys.stderr)
                continue
            # --- End Validation Step ---
            
            # --- Duplicate Prevention Check ---
            matched_name_normalized = cleaned_matched_name.strip().upper()
            if matched_name_normalized in existing_race_names:
                print(f"  Warning: Race name \"{cleaned_matched_name}\" for placeholder ID {placeholder_id} already exists in the CSV. Skipping to avoid duplicate.", file=sys.stderr)
                continue
            # --- End Duplicate Prevention Check ---

            if placeholder_id in placeholder_map:
                original_df_index = placeholder_map[placeholder_id]
                
                current_name_in_csv = full_circuit_df.loc[original_df_index, 'race_name']
                if pd.isnull(current_name_in_csv) or str(current_name_in_csv).strip() == '':
                    full_circuit_df.loc[original_df_index, 'race_name'] = cleaned_matched_name
                    # Add the new race name to existing_race_names to prevent duplicates in subsequent updates
                    existing_race_names.add(matched_name_normalized)
                    print(f"  Successfully updated placeholder ID {placeholder_id} with name: \"{cleaned_matched_name}\"")
                    changes_made = True
                else:
                    print(f"  Skipping update for placeholder ID {placeholder_id} (index {original_df_index}). It was already filled: '{current_name_in_csv}'")
            else:
                print(f"  Warning: LLM returned a match for an unknown placeholder_identifier: '{placeholder_id}'. This match will be skipped.", file=sys.stderr)

        if changes_made:
            print(f"Saving updated {CIRCUIT_RACES_PATH}...")
            full_circuit_df.sort_values(by=['race_year', 'race_number'], ascending=[True, True], inplace=True)

            # Strip whitespace from all object columns before saving to prevent unintended quoting
            for col in full_circuit_df.columns:
                if full_circuit_df[col].dtype == 'object':
                    full_circuit_df[col] = full_circuit_df[col].str.strip()
            
            full_circuit_df.to_csv(CIRCUIT_RACES_PATH, index=False, quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            print(f"{CIRCUIT_RACES_PATH} saved.")
        else:
            print("No changes were made to race names in circuit_races.csv based on LLM response.")

    except Exception as e:
        print(f"An error occurred in the main process: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
    finally:
        if changes_made:
            print("changes_made=true")
        else:
            print("changes_made=false")

if __name__ == "__main__":
    main() 