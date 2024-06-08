---
title: Estadísticas del Circuito de Carreras Populares de Cuenca
hide_title: true
---

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
Estadísticas del Circuito de Carreras Populares de Cuenca © 2024 by [Fran Lozano](https://www.franloza.com/) is licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).
[![CC BY-SA 4.0](https://licensebuttons.net/l/by-sa/4.0/80x15.png)](https://creativecommons.org/licenses/by-sa/4.0/).
