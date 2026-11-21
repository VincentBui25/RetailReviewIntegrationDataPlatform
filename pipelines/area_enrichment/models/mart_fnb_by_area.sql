with counts as (
    select area_id, count(*) as poi_anchor_count,
           count(*) filter (where assignment_method = 'ADDRESS') as address_assigned_anchor_count,
           count(*) filter (where assignment_method = 'SPATIAL') as spatial_assigned_anchor_count
    from {{ ref('store_area_mapping') }}
    where assignment_status = 'RESOLVED' and area_id is not null
    group by area_id
)
select d.area_id, d.ward_code, d.ward_name, d.city_code, d.city_name,
       f.reference_year, f.source_version, f.area_km2, f.population, f.population_basis,
       coalesce(c.poi_anchor_count, 0) as poi_anchor_count,
       coalesce(c.address_assigned_anchor_count, 0) as address_assigned_anchor_count,
       coalesce(c.spatial_assigned_anchor_count, 0) as spatial_assigned_anchor_count,
       coalesce(c.poi_anchor_count, 0) / nullif(f.area_km2, 0) as poi_anchors_per_km2,
       coalesce(c.poi_anchor_count, 0) * 1000.0 / nullif(f.population, 0) as poi_anchors_per_1000_population,
       f.provenance_json as demographics_provenance_json,
       'Khánh Hội candidate catalog anchors; not complete ward/city business coverage' as catalog_scope,
       false as is_verified_business_count, true as staging_only
from {{ ref('dim_area') }} d
join {{ ref('fact_area_demographics') }} f on d.area_id = f.area_id
left join counts c on d.area_id = c.area_id
