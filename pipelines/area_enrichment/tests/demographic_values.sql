select * from {{ ref('fact_area_demographics') }}
where area_km2 <= 0 or population < 0
   or ((area_km2 is null or population is null) and population_density is not null)
   or (area_km2 is not null and population is not null and
       (population_density is null or abs(population_density - population / area_km2) > 0.0000001))
