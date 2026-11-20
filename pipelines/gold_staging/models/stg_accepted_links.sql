{{ config(materialized='ephemeral') }}
select * from {{ source('pinned', 'effective_mapping') }}
where effective_status in ('AUTO_ACCEPTED', 'HUMAN_ACCEPTED')
  and not shadow_only and activation_scope = 'GOLD_STAGING_ONLY'
