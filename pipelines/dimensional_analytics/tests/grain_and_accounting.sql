-- All denominators come from admitted publications, never pilot constants.
select 'store_grain' as error from {{ ref('fact_store_snapshot') }} group by store_key,gold_release_id having count(*) <> 1
union all
select 'mapping_grain' from {{ ref('bridge_store_source') }} group by source,source_id_kind,source_id,gold_release_id having count(*) <> 1
union all
select 'google_fanout' from {{ ref('bridge_store_source') }} where source='google' group by source_id_kind,source_id having count(distinct store_key)>1
union all
select 'review_source_grain' from {{ ref('fact_review') }} group by source,source_review_id having count(*)<>1
union all
select 'lost_stores' where (select count(*) from {{ ref('fact_store_snapshot') }}) <> (select count(*) from {{ source('published','stores') }})
union all
select 'lost_reviews' where (select count(*) from {{ ref('fact_review') }}) <> (select count(*) from {{ source('published','attached') }})+(select count(*) from {{ source('published','unmatched') }})
union all
select 'lost_mappings' where (select count(*) from {{ ref('bridge_store_source') }}) <> (select count(*) from {{ source('published','mappings') }})
union all
select 'unauthorized_review_association' from {{ ref('fact_review') }} r
where r.store_key is not null and not exists (select 1 from {{ ref('bridge_store_source') }} m where m.source='google' and m.store_key=r.store_key and m.source_id_kind=r.google_id_kind and m.source_id=r.google_identity_value)
union all
select 'invented_date' from {{ ref('fact_review') }} where (review_date_key is null) <> (review_date_status='UNKNOWN')
union all
select 'bad_flags' from {{ ref('fact_store_snapshot') }} where store_count<>1 or accepted_link_flag+source_only_flag<>1 or (accepted_link_flag=0 and (google_rating is not null or google_rating_count is not null))
union all
select 'bad_review_rating' from {{ ref('fact_review') }} where review_count<>1 or review_rating not between 1 and 5
union all
select 'unmatched_lost' where (select count(*) from {{ ref('fact_review') }} where store_key is null) <> (select count(*) from {{ source('published','unmatched') }})
union all
select 'invalid_area_reference' from {{ ref('dim_area') }}
where reference_area_km2<=0 or reference_population<0 or reference_year is null
   or reference_source_url is null or reference_source_version is null
   or reference_population_basis is null or reference_quality_status is null
   or trusted_area_km2 is not null
