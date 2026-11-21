select 1 as failure
where (select sum(poi_anchor_count) from {{ ref('mart_fnb_by_area') }}) <>
      (select count(*) from {{ ref('store_area_mapping') }} where area_id is not null)
union all
select 2 from {{ ref('mart_fnb_by_area') }}
where (area_km2 is null and poi_anchors_per_km2 is not null)
   or ((population is null or population = 0) and poi_anchors_per_1000_population is not null)
   or is_verified_business_count
