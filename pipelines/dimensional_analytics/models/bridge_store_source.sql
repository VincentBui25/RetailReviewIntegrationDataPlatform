-- Authoritative Gold source mappings only; never candidate proposals.
select md5(store_id) as store_key, store_id as canonical_store_id,
       '{{ var("gold_release_id") }}' as gold_release_id,
       source, source_id_kind, source_id, link_status, decision_source,
       policy_version, evidence_hash, provenance_json as decision_provenance
from {{ source('published','mappings') }}
