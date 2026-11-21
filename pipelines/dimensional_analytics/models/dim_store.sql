-- Current descriptive store dimension, not a declaration of verified physical identity.
select md5(store_id) as store_key, store_id as canonical_store_id,
       store_name, address, latitude, longitude, identity_status,
       is_identity_human_verified, field_provenance_json
from {{ source('published','stores') }}
