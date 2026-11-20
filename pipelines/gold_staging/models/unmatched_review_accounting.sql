{{ config(enabled=var('reviews_enabled', false)) }}
with evidence as (
    select google_id_kind, google_identity_value,
           list_sort(list(distinct effective_status)) as mapping_statuses,
           to_json(list(struct_pack(overture_id := overture_id, status := effective_status,
               shadow_only := shadow_only, activation_scope := activation_scope,
               evidence_hash := evidence_hash) order by overture_id, effective_status))::varchar as mapping_evidence_json
    from {{ source('pinned', 'effective_mapping') }}
    group by google_id_kind, google_identity_value
)
select r.*, case when e.google_identity_value is null then 'NO_MAPPING' else 'NO_ACCEPTED_MAPPING' end as unmatched_reason,
       coalesce(e.mapping_statuses, []::varchar[]) as mapping_statuses,
       coalesce(e.mapping_evidence_json, '[]') as mapping_evidence_json,
       '{{ var("reviews_context_json", "{}") | replace("'", "''") }}'::varchar as gold_review_lineage_json
from {{ source('pinned_reviews', 'current') }} r
left join evidence e using (google_id_kind, google_identity_value)
where not exists (
    select 1 from {{ ref('stg_accepted_links') }} a
    where a.google_id_kind = r.google_id_kind and a.google_identity_value = r.google_identity_value
)
