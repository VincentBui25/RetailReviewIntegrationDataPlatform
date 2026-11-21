{{ config(materialized='ephemeral') }}
select o.*, r.store_id, n.normalized_categories,
       n.source_provenance_json as category_source_provenance,
       n.dq_flags as category_dq_flags
from {{ source('pinned', 'overture') }} o
join {{ source('pinned', 'category_normalization') }} n
  on n.source = 'overture' and n.source_id = o.source_id
join {{ source('pinned', 'store_registry') }} r on r.overture_id = o.source_id
where n.baseline_fnb_candidate
