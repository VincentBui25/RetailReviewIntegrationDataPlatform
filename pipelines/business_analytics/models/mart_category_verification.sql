-- Grain: analysis category x verification status x evidence basis.
select analysis_category, verification_status, analysis_category_basis,
       count(*) as store_count
from {{ ref('mart_store_business') }}
group by analysis_category, verification_status, analysis_category_basis
