select 'overture' as source, 'overture_id' as source_id_kind, o.source_id,
       o.store_id, true as in_catalog, 'SOURCE_ANCHOR_UNVERIFIED' as accounting_status
from {{ ref('stg_overture_candidates') }} o
union all
select 'google', g.google_id_kind, g.google_identity_value,
       m.store_id, m.store_id is not null,
       case when m.store_id is not null then 'ACCEPTED_LINK'
            when exists(select 1 from {{ source('pinned', 'effective_mapping') }} e
                        where e.google_id_kind = g.google_id_kind and e.google_identity_value = g.google_identity_value)
            then 'UNASSIGNED_CANDIDATE' else 'UNASSIGNED_SOURCE_OBSERVATION' end
from {{ source('pinned', 'google') }} g
left join {{ ref('store_source_mapping') }} m
  on m.source = 'google' and m.source_id_kind = g.google_id_kind and m.source_id = g.google_identity_value
