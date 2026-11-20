select store_id from {{ ref('dim_store') }}
where category_validation_status not in ('PENDING', 'PROVISIONAL_SOURCE_AGREEMENT', 'SOURCE_AGREEMENT_HUMAN_LINK')
   or (category_relation = 'CATEGORY_CONFLICT' and canonical_category <> 'unknown')
   or (canonical_category <> 'unknown' and (category_source is null or category_provenance_json is null))
