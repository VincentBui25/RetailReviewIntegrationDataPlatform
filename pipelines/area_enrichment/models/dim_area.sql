select
    case when city_code = '79' then 'vn-hcm-ward-' || ward_code
         else 'vn-city-' || city_code || '-ward-' || ward_code end as area_id,
    ward_code, ward_name, city_code, city_name,
    legal_code_status, legal_code_provenance_json,
    source_version as reference_version, provenance_json as reference_provenance_json,
    true as staging_only
from {{ source('pinned_area', 'reference') }}
where legal_code_status = 'CODE_AND_NAME_AGREE'
