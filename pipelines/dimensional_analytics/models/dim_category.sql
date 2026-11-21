-- Evidence/verification is contextual to each snapshot, never an intrinsic category label.
select distinct md5(analysis_category) as category_key,
       analysis_category as category_business_key,
       replace(analysis_category, '_', ' ') as category_name
from {{ source('published','business') }}
