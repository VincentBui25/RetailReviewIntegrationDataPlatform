-- Business question: how many catalog anchors are in each category and verification state?
-- Grain: canonical category x identity status x category validation status.
select canonical_category, identity_status, category_validation_status,
       count(*) as catalog_store_count,
       count(*) filter (where is_identity_human_verified) as human_verified_count
from {{ ref('dim_store') }}
group by canonical_category, identity_status, category_validation_status
