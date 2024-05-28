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
© 2024 [Fran Lozano](https://www.franloza.com). Todos los derechos reservados.