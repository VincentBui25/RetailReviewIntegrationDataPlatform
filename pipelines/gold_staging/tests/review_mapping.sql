{{ config(enabled=var('reviews_enabled', false)) }}
select r.logical_review_id
from {{ ref('review_attachment') }} r
where not exists (
    select 1 from {{ ref('stg_accepted_links') }} a
    join {{ ref('dim_store') }} d on d.overture_id = a.overture_id
    where a.google_id_kind = r.google_id_kind and a.google_identity_value = r.google_identity_value
      and a.overture_id = r.overture_id and d.store_id = r.store_id
      and a.effective_status = r.mapping_status and a.evidence_hash = r.mapping_evidence_hash
)
union all
select r.logical_review_id from {{ ref('unmatched_review_accounting') }} r
where exists (select 1 from {{ ref('stg_accepted_links') }} a
    where a.google_id_kind = r.google_id_kind and a.google_identity_value = r.google_identity_value)
