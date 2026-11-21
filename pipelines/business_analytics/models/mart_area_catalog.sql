-- Grain: supported area or explicit outside/missing-coordinate bucket.
select area_id, area_name, spatial_status,
       count(*) as store_count,
       count(*) filter (where valid_coordinates) as valid_coordinate_count,
       count(*) filter (where analysis_category = 'unknown') as unknown_category_count,
       cast(null as double) as trusted_area_km2,
       cast(null as double) as observed_stores_per_km2,
       'UNVERIFIED_AREA_DENOMINATOR' as density_status
from {{ ref('mart_store_business') }}
group by area_id, area_name, spatial_status
