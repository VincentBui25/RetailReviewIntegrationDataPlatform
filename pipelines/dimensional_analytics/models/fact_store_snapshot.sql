select d.store_key, c.category_key, a.area_key,
       '{{ var("gold_release_id") }}' as gold_release_id, 1 as store_count,
       case when s.identity_status in ('AUTO_ACCEPTED','HUMAN_ACCEPTED') then 1 else 0 end as accepted_link_flag,
       case when s.identity_status = 'SOURCE_ONLY' then 1 else 0 end as source_only_flag,
       case when p.rating between 1 and 5 then p.rating end as google_rating,
       case when p.review_count >= 0 then p.review_count end as google_rating_count,
       case when p.rating is null then 'MISSING' when p.rating between 1 and 5 then 'VALID' else 'INVALID' end as google_rating_status,
       '{{ var("places_release_id") }}' as places_release_id,
       s.category_validation_status as gold_category_evidence_status,
       s.canonical_category as gold_canonical_category,
       b.verification_status, b.analysis_category_basis,
       b.analysis_category_provenance_json
from {{ source('published','stores') }} s
join {{ ref('dim_store') }} d on d.canonical_store_id = s.store_id
join {{ source('published','business') }} b on b.store_id = s.store_id
join {{ ref('dim_category') }} c on c.category_business_key = b.analysis_category
join {{ ref('dim_area') }} a on a.area_business_key = b.area_id
left join {{ ref('bridge_store_source') }} m on m.store_key = d.store_key and m.source = 'google'
left join {{ source('published','places') }} p on p.google_id_kind = m.source_id_kind and p.google_identity_value = m.source_id
