select md5(area_id) as area_key, area_id as area_business_key, area_name,
       boundary_source, boundary_url, boundary_snapshot, boundary_license,
       boundary_sha256, boundary_geojson, cast(null as double) as trusted_area_km2,
       cast(reference_area_km2 as double) as reference_area_km2,
       cast(reference_population as bigint) as reference_population,
       cast(reference_year as integer) as reference_year,
       reference_source_version,reference_source_url,reference_record_url,
       reference_population_basis,reference_area_basis,reference_quality_status,
       reference_provenance_json,reference_release_id
from {{ source('published','areas') }}
