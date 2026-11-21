with candidates as (
    select * from {{ ref('stg_overture_candidates') }}
), accepted as (
    select * from {{ ref('stg_accepted_links') }}
), selected as (
    select o.store_id, o.source_id as overture_id,
           a.google_id_kind, a.google_identity_value,
           coalesce(g.name_display, o.name) as store_name,
           coalesce(g.address_display, o.address) as address,
           case when g.latitude between -90 and 90 and g.longitude between -180 and 180
                then g.latitude else o.latitude end as latitude,
           case when g.latitude between -90 and 90 and g.longitude between -180 and 180
                then g.longitude else o.longitude end as longitude,
           coalesce(a.effective_status, 'SOURCE_ONLY') as identity_status,
           coalesce(a.effective_status = 'HUMAN_ACCEPTED', false) as is_identity_human_verified,
           case when a.overture_id is not null then 'GOOGLE_LINK_ACCEPTED'
                when exists(select 1 from {{ source('pinned', 'effective_mapping') }} e where e.overture_id = o.source_id)
                then 'UNRESOLVED_CANDIDATES' else 'NO_GOOGLE_CANDIDATE' end as integration_status,
           coalesce(g.business_status, 'UNKNOWN') as business_status,
           o.source_operating_status as overture_source_operating_status,
           o.normalized_categories as overture_category_groups,
           gn.normalized_categories as google_category_groups,
           c.category_relation,
           case when a.overture_id is not null and c.category_relation = 'CONSISTENT'
                     and len(o.normalized_categories) = 1
                     and o.normalized_categories = gn.normalized_categories
                     and o.normalized_categories[1] <> 'unknown'
                     and len(o.category_dq_flags) = 0 and len(gn.dq_flags) = 0
                then o.normalized_categories[1] else 'unknown' end as canonical_category,
           case when g.name_display is not null then 'google' else 'overture' end as name_source,
           case when g.address_display is not null then 'google' else 'overture' end as address_source,
           case when g.latitude between -90 and 90 and g.longitude between -180 and 180
                then 'google' else 'overture' end as coordinate_source,
           o.category_source_provenance, gn.source_provenance_json as google_category_provenance,
           coalesce(g.field_provenance_json, '{}') as google_field_provenance_json,
           a.decision_source, a.policy_version, a.evidence_hash as identity_evidence_hash
    from candidates o
    left join accepted a on a.overture_id = o.source_id
    left join {{ source('pinned', 'google') }} g using (google_id_kind, google_identity_value)
    left join {{ source('pinned', 'category_normalization') }} gn
      on gn.source = 'google' and gn.google_id_kind = a.google_id_kind
     and gn.google_identity_value = a.google_identity_value
    left join {{ source('pinned', 'category_audit') }} c
      on c.overture_id = a.overture_id and c.google_id_kind = a.google_id_kind
     and c.google_identity_value = a.google_identity_value
)
select *, canonical_category as canonical_category_group,
       case when canonical_category = 'unknown' then 'PENDING'
            when identity_status = 'HUMAN_ACCEPTED' then 'SOURCE_AGREEMENT_HUMAN_LINK'
            else 'PROVISIONAL_SOURCE_AGREEMENT' end as category_validation_status,
       case when canonical_category <> 'unknown' then 'overture+google' else null end as category_source,
       'gold-staging-category-v1' as category_rule_version,
       json_object('overture', cast(category_source_provenance as json),
                   'google', cast(google_category_provenance as json))::varchar as category_provenance_json,
       json_object('name_source', name_source, 'address_source', address_source, 'coordinate_source', coordinate_source,
                   'input_refs', cast('{{ var("input_refs_json") | replace("'", "''") }}' as json),
                   'google_fields', cast(google_field_provenance_json as json),
                   'field_selection_policy', 'accepted-google-non-null-else-overture-v1')::varchar as field_provenance_json,
       true as staging_only
from selected
