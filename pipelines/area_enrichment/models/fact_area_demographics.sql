select d.area_id, r.ward_code, r.reference_year, r.source_version,
       r.area_km2, r.population, r.population_density, r.population_basis, r.area_basis,
       r.legal_document, r.source_url, r.source_record_url, r.source_build_date, r.data_until,
       r.retrieved_at_utc, r.data_quality_status, r.dq_flags, r.provenance_json,
       r.rule_version, true as staging_only
from {{ source('pinned_area', 'reference') }} r
join {{ ref('dim_area') }} d on r.ward_code = d.ward_code
