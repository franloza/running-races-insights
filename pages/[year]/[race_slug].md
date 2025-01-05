---
title: Estadísticas del Circuito de Carreras Populares de Cuenca
hide_title: true
---

```sql race_name
select
    distinct race_name

from results
where race_year=${params.year} and race_slug='${params.race_slug}'
```

# <Value data={race_name} />

```sql participants
with current_year as (

    select 

    count(distinct runner_id) as participantes,
    count(distinct case when gender = 'Male' then runner_id end) as participantes_hombres,
    count(distinct case when gender = 'Female' then runner_id end) as participantes_mujeres,
    participantes_hombres / participantes as hombres_pct,
    participantes_mujeres / participantes as mujeres_pct

    from results
    where race_year=${params.year} and race_slug='${params.race_slug}'

),

prev_year as (

    select 
        count(distinct runner_id) as participantes,
        count(distinct case when gender = 'Male' then runner_id end) as participantes_hombres,
        count(distinct case when gender = 'Female' then runner_id end) as participantes_mujeres,

from results
where race_year=${params.year}-1 and race_slug='${params.race_slug}'


)

select 
    current_year.participantes,
    current_year.participantes_hombres as "Participantes (Hombres)",
    current_year.participantes_mujeres  as "Participantes (Mujeres)",
    current_year.participantes_hombres / current_year.participantes as hombres_pct,
    current_year.participantes_mujeres / current_year.participantes as mujeres_pct,
    prev_year.participantes as prev_participantes,
    case when prev_participantes = 0 then null else
    current_year.participantes - prev_participantes end as diff_praticipantes,
    (current_year.participantes / prev_year.participantes) - 1 as crecimiento_participantes,
    (current_year.participantes_hombres / prev_year.participantes_hombres) - 1  as crecimiento_hombres,
    (current_year.participantes_mujeres / prev_year.participantes_mujeres) - 1  as crecimiento_mujeres

from current_year
cross join prev_year

```

<Grid cols=3>

<BigValue 
    data={participants} 
    value=participantes
    comparison=diff_praticipantes
    comparisonTitle="vs año anterior"
    comparisonDelta=true
    downIsGood=false
/>

<BigValue 
    data={participants} 
    value="Participantes (Hombres)"
    comparison=hombres_pct
    comparisonTitle="del Total"
    comparisonDelta=false
/>

<BigValue 
    data={participants} 
    value="Participantes (Mujeres)"
    comparison=mujeres_pct
    comparisonTitle="del Total"
    comparisonDelta=false
/>


<Delta data={participants} column=crecimiento_participantes fmt=pct1 chip=true text="Participantes vs año anterior" />
<Delta data={participants} column=crecimiento_hombres fmt=pct1 chip=true text="Participantes (Hombres) vs año anterior"/>
<Delta data={participants} column=crecimiento_mujeres fmt=pct1 chip=true text="Participante (Mujeres) vs año anterior"/>

</Grid>

```sql participants_by_age
    select 
        age_bucket,
        case gender 
            when 'Male' then 'Hombre'
            when 'Female' then 'Mujer'
            else 'Otro'
        end as genero,
        count(distinct runner_id) as participantes,

    from results
    where race_year=${params.year} and race_slug='${params.race_slug}'
    group by 1, age_bucket_order,2
    order by age_bucket_order, genero
```

<BarChart 
    data={participants_by_age} 
    x=age_bucket 
    y=participantes
    sort=false
    series=genero
    title="Participantes por edad y género. {inputs.category.label}"
    colorPalette={SiteColors}
    labels=true
    stackTotalLabel=true
    labelSize=11
    labelPosition=inside
}
/>

```sql participants_by_year

select
    race_year as año,
    case gender 
        when 'Male' then 'Hombre'
        when 'Female' then 'Mujer'
        else 'Otro'
    end as genero,
    count(distinct runner_id) as participantes

from results
where race_slug='${params.race_slug}'
group by 1, 2
order by 1
```

<BarChart 
    data={participants_by_year} 
    x=año 
    y=participantes
    sort=false
    series=genero
    title="Evolución de participación por año."
    colorPalette={SiteColors}
    labels=true
    stackTotalLabel=true
    labelSize=11
    labelPosition=inside
    xFmt=0000
/>

### Mejores tiempos - Año {params.year}

```sql best_times_men_year
select 
  formatted_time as tiempo,
  pace_min_per_km_formatted as ritmo,
  race_distance as distancia

from results
where race_slug='${params.race_slug}' and gender = 'Male' and race_year = ${params.year}
order by time_ms
limit 3
```

```sql best_times_women_year
select 
  formatted_time as tiempo,
  pace_min_per_km_formatted as ritmo,
  race_distance as distancia

from results
where race_slug='${params.race_slug}' and gender = 'Female' and race_year = ${params.year}
order by time_ms
limit 3
```

<Grid cols=2>
<Group>
    <b>Hombres</b>
    <DataTable data={best_times_men_year}/>
</Group>
<Group>
    <b>Mujeres</b>
    <DataTable data={best_times_women_year}/>
</Group>
</Grid>

### Mejores tiempos - Todos los años

```sql best_times_men_all_years
select 
  formatted_time as tiempo,
  pace_min_per_km_formatted as ritmo,
  race_distance as distancia,
  race_year as año

from results
where race_slug='${params.race_slug}' and gender = 'Male'
order by time_ms
limit 3
```

```sql best_times_women_all_years
select 
  formatted_time as tiempo,
  pace_min_per_km_formatted as ritmo,
  race_distance as distancia,
  race_year as año

from results
where race_slug='${params.race_slug}' and gender = 'Female'
order by time_ms
limit 3
```

<Grid cols=2>
<Group>
    <b>Hombres</b>
    <DataTable data={best_times_men_all_years}/>
</Group>
<Group>
    <b>Mujeres</b>
    <DataTable data={best_times_women_all_years}/>
</Group>
</Grid>


Estadísticas del Circuito de Carreras Populares de Cuenca © 2024 by [Fran Lozano](https://www.franloza.com/) is licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
[![CC BY-SA 4.0](https://licensebuttons.net/l/by-sa/4.0/80x15.png)](https://creativecommons.org/licenses/by-sa/4.0/).

<script>

let SiteColors = [
    '#FCC80A',
    '#0A0E1E',
    '#A6A6A6',
]
</script>
