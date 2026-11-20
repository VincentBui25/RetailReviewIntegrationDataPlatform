-- Grain: one existing canonical store_id. No matching, source expansion or review join.
with projected as (
    select g.*,
           g.canonical_category as gold_canonical_category,
           case when g.canonical_category <> 'unknown' then g.canonical_category
                when g.identity_status = 'SOURCE_ONLY' and g.google_identity_value is null
                     and len(n.normalized_categories) = 1 and len(n.dq_flags) = 0
                     and n.normalized_categories[1] in ('restaurant','cafe_coffee','beverage',
                         'bakery_dessert','fast_food','bar','other_food')
                then n.normalized_categories[1] else 'unknown' end as analysis_category,
           n.source_provenance_json as analysis_category_provenance_json,
           a.area_id, a.area_name, a.spatial_status, a.valid_coordinates
    from {{ source('pinned','gold_stores') }} g
    left join {{ source('pinned','category_normalization') }} n
      on n.source = 'overture' and n.source_id = g.overture_id
    join {{ source('pinned','store_area') }} a using (store_id)
)
select *,
       case when gold_canonical_category <> 'unknown' then 'SOURCE_AGREEMENT'
            when analysis_category <> 'unknown' then 'OVERTURE_SOURCE_PROVISIONAL'
            when category_relation in ('SUBCATEGORY_DIFFERENCE','CATEGORY_CONFLICT')
            then 'SOURCE_DISAGREEMENT' else 'UNKNOWN' end as analysis_category_basis,
       case when is_identity_human_verified then 'HUMAN_VERIFIED_LINK'
            when identity_status = 'AUTO_ACCEPTED' then 'MACHINE_ACCEPTED_LINK'
            else 'UNVERIFIED_SOURCE_ONLY' end as verification_status
from projected
