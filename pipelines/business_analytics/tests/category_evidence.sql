select store_id
from {{ ref('mart_store_business') }}
where (analysis_category_basis = 'OVERTURE_SOURCE_PROVISIONAL'
       and (identity_status <> 'SOURCE_ONLY' or is_identity_human_verified
            or google_identity_value is not null or analysis_category_provenance_json is null))
   or (category_relation in ('SUBCATEGORY_DIFFERENCE','CATEGORY_CONFLICT')
       and gold_canonical_category = 'unknown' and analysis_category <> 'unknown')
   or (valid_coordinates and (latitude not between -90 and 90 or longitude not between -180 and 180))
