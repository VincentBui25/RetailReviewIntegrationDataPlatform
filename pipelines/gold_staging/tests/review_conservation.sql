{{ config(enabled=var('reviews_enabled', false)) }}
with expected as (
    select source, source_review_id, logical_review_id from {{ source('pinned_reviews', 'current') }}
), actual as (
    select source, source_review_id, logical_review_id from {{ ref('review_attachment') }}
    union all
    select source, source_review_id, logical_review_id from {{ ref('unmatched_review_accounting') }}
)
select * from (select * from expected except all select * from actual)
union all
select * from (select * from actual except all select * from expected)
union all
select source, source_review_id, min(logical_review_id) from actual
group by source, source_review_id having count(*) <> 1
union all
select min(source), min(source_review_id), logical_review_id from expected
group by logical_review_id having count(*) <> 1
