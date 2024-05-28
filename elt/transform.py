import duckdb

con = duckdb.connect('sources/race_results/race_results.duckdb')

# Create or replace the table and insert some data
con.execute("""
CREATE OR REPLACE TABLE race_results AS (
            
    with results as (

        select 
            case nullif(SexoT10, 'nan') when 'M' then 'Male' when 'F' then 'Female' end as gender,
            try_cast(nullif(MiliSegCarreraT05, 'nan') as int) as time_ms,
            to_milliseconds(time_ms) as time_interval,
            printf(
                '%02d:%02d:%02d', 
                extract(hours from time_interval), 
                extract(minutes from time_interval), 
                extract(seconds from time_interval)
            ) AS formatted_time,
            year(FechaCarreraT01) as race_year,
            rtrim(ltrim(regexp_replace(NombreCarreraT01, '\s+', ' ', 'g'))) as race_name,
            md5(rtrim(ltrim(lower(regexp_replace(NombreT05 || ' ' || ApellidosT05 || coalesce(gender, 'X'), '\s+', ' ', 'g'))))) as runner_id,
            FechaCarreraT01 as race_date,
            try_cast(DistanciaMetrosTotalT01 as int) as race_distance,
            time_ms / race_distance as pace_ms_per_m,
            to_milliseconds(pace_ms_per_m * 1000 )as pace_per_km,
            printf('%02d:%02d', extract(minutes from pace_per_km),  extract(seconds from pace_per_km)) AS pace_min_per_km_formatted,
            strptime(left(nullif(FechaNacimientoT05, 'nan'), 10), '%d/%m/%Y') as dob,
            datediff('year', dob, FechaCarreraT01) - 1 as age,
            time_ms is not null as is_finisher,
            case 
                when age < 18 then '<18'
                when age between 18 and 24 then '18-24'
                when age between 25 and 39 then '26-39'
                when age between 40 and 49 then '40-49'
                when age between 50 and 59 then '50-59'
                when age between 60 and 69 then '60-69'
                when age > 69 then '>69'
            end as age_bucket,
            case 
                when age < 18 then 1
                when age between 18 and 24 then 2
                when age between 25 and 39 then 3
                when age between 40 and 49 then 4
                when age between 50 and 59 then 5
                when age between 60 and 69 then 6
                when age > 69 then 7
            end as age_bucket_order,
            NombreT11 as team_name,
            NombreT10 as category,


        from "data/raw/race_results.parquet"
            
    ),
            
    circuit_races as (

        select * from read_csv('data/circuit_races.csv')

    )

    select 
        results.*,
        circuit_races.race_number,
        circuit_races.race_location,
        circuit_races.race_slug

    from results
    left join circuit_races using (race_name, race_year)
    where race_number is not null
    
);
""")

con.commit()
con.close()
