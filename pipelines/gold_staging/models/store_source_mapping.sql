select o.store_id, 'overture' as source, 'overture_id' as source_id_kind,
       o.source_id as source_id, 'SOURCE_ANCHOR_UNVERIFIED' as link_status,
       'OVERTURE_SOURCE' as decision_source, null::varchar as policy_version,
       null::varchar as evidence_hash, o.category_source_provenance as provenance_json
from {{ ref('stg_overture_candidates') }} o
union all
select o.store_id, 'google', a.google_id_kind, a.google_identity_value,
       a.effective_status, a.decision_source, a.policy_version, a.evidence_hash,
       a.source_provenance_json
from {{ ref('stg_accepted_links') }} a
join {{ ref('stg_overture_candidates') }} o on o.source_id = a.overture_id
