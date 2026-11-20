select m.source_id as bad_id
from {{ ref('store_source_mapping') }} m
where m.source = 'google' and not exists (
    select 1 from {{ source('pinned', 'effective_mapping') }} e
    where e.google_id_kind = m.source_id_kind and e.google_identity_value = m.source_id
      and e.effective_status in ('AUTO_ACCEPTED', 'HUMAN_ACCEPTED')
      and not e.shadow_only and e.activation_scope = 'GOLD_STAGING_ONLY'
)
union all
select overture_id from {{ ref('link_accounting') }}
where merged_store_id is not null and effective_status not in ('AUTO_ACCEPTED', 'HUMAN_ACCEPTED')
