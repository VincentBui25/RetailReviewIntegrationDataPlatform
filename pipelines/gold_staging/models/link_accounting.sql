select e.*, o.store_id as overture_store_id,
       case when e.effective_status in ('AUTO_ACCEPTED', 'HUMAN_ACCEPTED') and not e.shadow_only
            then o.store_id else null end as merged_store_id
from {{ source('pinned', 'effective_mapping') }} e
left join {{ ref('stg_overture_candidates') }} o on o.source_id = e.overture_id
