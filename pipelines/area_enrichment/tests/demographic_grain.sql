select area_id, reference_year, source_version
from {{ ref('fact_area_demographics') }}
group by all having count(*) <> 1
