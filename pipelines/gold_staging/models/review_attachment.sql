{{ config(enabled=var('reviews_enabled', false)) }}
select r.*, d.store_id, a.overture_id,
       a.effective_status as mapping_status, a.evidence_hash as mapping_evidence_hash,
       a.decision_source as mapping_decision_source, a.policy_version as mapping_policy_version,
       a.source_provenance_json as mapping_provenance_json,
       '{{ var("reviews_context_json", "{}") | replace("'", "''") }}'::varchar as gold_review_lineage_json
from {{ source('pinned_reviews', 'current') }} r
join {{ ref('stg_accepted_links') }} a
  on r.google_id_kind = a.google_id_kind and r.google_identity_value = a.google_identity_value
join {{ ref('dim_store') }} d on d.overture_id = a.overture_id
