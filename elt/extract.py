import requests
import pandas as pd
import numpy as np
import zipfile
from tqdm import tqdm
import io
import os


def extract() -> pd.DataFrame:
    url = 'https://resultados.cronomancha.com/data/'
    response = requests.get(url)
    races = response.json()

    results_df = pd.DataFrame()

    for race in tqdm(races):
        if race.get("Modalidad") in ("A PIE", "A PIE (TRAIL/RUNNING)"):
            dir_field = race.get("dir")
            ts_field = race.get("TS")

            if dir_field and ts_field:
                result_url = f'{url}{dir_field}/{ts_field}_result.json.zip'
                result_response = requests.get(result_url)
                if result_response.status_code == 200:
                    with zipfile.ZipFile(io.BytesIO(result_response.content)) as z:
                        for file_name in z.namelist():
                            with z.open(file_name) as file:
                                race_results = pd.read_json(file)

                                # Race columns
                                race_results['NombreCarreraT01'] = race.get("NombreCarreraT01")
                                race_results['FechaCarreraT01'] = pd.to_datetime(race.get("FechaCarreraT01"), dayfirst=True)
                                race_results['DistanciaMetrosTotalT01'] = race.get("DistanciaMetrosTotalT01")

                                results_df = pd.concat([results_df, race_results], ignore_index=True)
                else:
                    print(f"{result_url} didn't return data")

    # Drop/cast problematic columns
    results_df.replace('', np.nan, inplace=True)
    object_columns = results_df.select_dtypes(include=['object']).columns

    # Convert these columns to strings
    results_df[object_columns] = results_df[object_columns].astype(str)

    return results_df


def load(data: pd.DataFrame):
    output_dir = 'data/raw'
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'race_results.parquet')
    data.to_parquet(output_file, index=False, engine='pyarrow', compression='lz4')
    print(f"Results saved to {output_file}")


if __name__ == '__main__':
    data = extract()
    load(data)
