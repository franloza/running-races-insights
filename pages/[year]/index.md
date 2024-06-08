---
title: Estadísticas del Circuito de Carreras Populares de Cuenca
hide_title: true
---


{#if params.year == 2023 || params.year==2024 ||  params.year == 2022}


# Circuito de Carreras Populares {params.year}

## General

```sql categories
select
    category
from results
where race_year=${params.year}
group by category
```

<Dropdown data={categories} name=category value=category>
    <DropdownOption value="%" valueLabel="Todas las categorías"/>
</Dropdown>

```sql participants
with current_year as (

    select 

    count(distinct runner_id) as participantes,
    count(distinct case when gender = 'Male' then runner_id end) as participantes_hombres,
    count(distinct case when gender = 'Female' then runner_id end) as participantes_mujeres,
    participantes_hombres / participantes as hombres_pct,
    participantes_mujeres / participantes as mujeres_pct

    from results
    where race_year=${params.year}
),

prev_year as (

    select 
        count(distinct runner_id) as participantes,
        count(distinct case when gender = 'Male' then runner_id end) as participantes_hombres,
        count(distinct case when gender = 'Female' then runner_id end) as participantes_mujeres,

from results
where race_year=${params.year}-1


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

```sql participants_by_race
    select 
        race_number,
        race_number::int || '. ' || race_location as carrera,
        case gender 
            when 'Male' then 'Hombre'
            when 'Female' then 'Mujer'
            else 'Otro'
        end as genero,
        count(distinct runner_id) as participantes,

    from results
    where category like '${inputs.category.value}'
    and race_year=${params.year}
    group by 1,2,3
    order by race_number
```


<BarChart
    data={participants_by_race}
    title="Participantes por carrera y género. {inputs.category.label}"
    x=carrera
    y=participantes
    series=genero
    sort=false,
    swapXY=true
    colorPalette={SiteColors}
    labels=true
    stackTotalLabel=true
    labelSize=14
    labelPosition=inside
/>

```sql finish_rate_by_race

select 
    race_number,
    race_number::int || '. ' || race_location as carrera,
    case when is_finisher then 'Finaliza' else 'No finaliza' end as finaliza,
    count(distinct runner_id) as participantes

from results
where category like '${inputs.category.value}'
and race_year=${params.year}
group by 1,2,3
order by race_number, 3

```

<BarChart 
    data={finish_rate_by_race} 
    x=carrera 
    y=participantes 
    series=finaliza
    colorPalette={SiteColors}
    sort=false
    swapXY=true
    labels=true
    title="Participantes que finalizan por carrera. {inputs.category.label}"
/>

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
    where category like '${inputs.category.value}'
    and race_year=${params.year}
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
where category like '${inputs.category.value}'
group by 1, 2
order by 1
```

<BarChart 
    data={participants_by_year} 
    x=año 
    y=participantes
    sort=false
    series=genero
    title="Evolución de participación por año. {inputs.category.label}"
    colorPalette={SiteColors}
    labels=true
    stackTotalLabel=true
    labelSize=11
    labelPosition=inside
    xFmt=0000
/>


## Carreras

```sql all_races

select
    distinct 
        race_number::int || '. ' || race_location as carrera,
        race_slug,
        race_year

from results
where race_year=${params.year}
order by race_number
```

{#each all_races as all_races}

<BigLink href='/{all_races.race_year}/{all_races.race_slug}'>
    {all_races.carrera}
</BigLink>

{/each}





{:else }

![Circuito de Carreras Populares](/logo.png)

# Ediciones

```sql years
    select 
        distinct race_year
    from results
    order by 1 desc
```


{#each years as years}

## [Circuito de Carreras Populares {years.race_year}](/{years.race_year})

{/each}

‎    

{/if}

Estadísticas del Circuito de Carreras Populares de Cuenca © 2024 by Fran Lozano is licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
[![CC BY-SA 4.0](https://licensebuttons.net/l/by-sa/4.0/80x15.png)](https://creativecommons.org/licenses/by-sa/4.0/).

<script>

let SiteColors = [
    '#FCC80A',
    '#0A0E1E',
    '#A6A6A6',
]
</script>
