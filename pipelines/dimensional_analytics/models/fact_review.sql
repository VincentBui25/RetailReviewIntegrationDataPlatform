with reviews as ({{ current_reviews() }})
select logical_review_id, source, source_review_id, google_id_kind,
       google_identity_value,
       case when store_id is not null then md5(store_id) end as store_key,
       case when published_at is not null and timestamp_status='VERIFIED' then cast(strftime(published_at,'%Y%m%d') as integer) end as review_date_key,
       case when rating in (1,2,3,4,5) then rating end as review_rating,
       case when rating is null then 'MISSING' when rating in (1,2,3,4,5) then 'VALID' else 'INVALID' end as review_rating_status,
       case when published_at is not null and timestamp_status='VERIFIED' then 'VERIFIED_SOURCE_DATE' else 'UNKNOWN' end as review_date_status,
       timestamp_source as review_date_source,
       1 as review_count,
       json_extract_string(gold_review_lineage_json,'$.reviews_release_id') as source_release_id,
       '{{ var("gold_release_id") }}' as association_gold_release_id,
       sha256(provenance_json) as source_provenance_sha256,
       gold_review_lineage_json as association_provenance
from reviews
