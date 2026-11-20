-- One row per unchanged canonical source anchor; this model does not merge identities.
select a.*,
       c.identity_status, c.is_identity_human_verified, c.integration_status,
       c.category_validation_status,
       true as staging_only
from {{ source('pinned_area', 'catalog') }} c
join {{ source('pinned_area', 'assignment') }} a on c.store_id = a.store_id
