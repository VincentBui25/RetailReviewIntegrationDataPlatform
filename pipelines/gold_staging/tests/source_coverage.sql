with expected as (
    select 'overture' as source, 'overture_id' as source_id_kind, source_id
    from {{ ref('stg_overture_candidates') }}
    union all
    select 'google', google_id_kind, google_identity_value from {{ source('pinned', 'google') }}
), accounted as (
    select source, source_id_kind, source_id from {{ ref('source_accounting') }}
)
select * from (select * from expected except all select * from accounted)
union all
select * from (select * from accounted except all select * from expected)
union all
select 'link', google_id_kind, overture_id || ':' || google_identity_value from (
    select overture_id, google_id_kind, google_identity_value from {{ source('pinned', 'effective_mapping') }}
    except all
    select overture_id, google_id_kind, google_identity_value from {{ ref('link_accounting') }}
)
