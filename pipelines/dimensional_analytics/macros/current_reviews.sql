{% macro current_reviews() %}
select logical_review_id, source, source_review_id, google_id_kind, google_identity_value,
       published_at, timestamp_status, timestamp_source, rating, provenance_json, gold_review_lineage_json, store_id
from {{ source('published','attached') }}
union all
select logical_review_id, source, source_review_id, google_id_kind, google_identity_value,
       published_at, timestamp_status, timestamp_source, rating, provenance_json, gold_review_lineage_json, cast(null as varchar) as store_id
from {{ source('published','unmatched') }}
{% endmacro %}
