-- Every aggregation preserves Gold store grain; reviews/mappings cannot inflate KPIs.
select 'gold_coverage' as failure
where (select count(*) from {{ ref('mart_store_business') }}) <>
      (select count(*) from {{ source('pinned','gold_stores') }})
union all
select 'category_partition'
where (select sum(store_count) from {{ ref('mart_category_verification') }}) <>
      (select count(*) from {{ ref('mart_store_business') }})
union all
select 'area_partition'
where (select sum(store_count) from {{ ref('mart_area_catalog') }}) <>
      (select count(*) from {{ ref('mart_store_business') }})
union all
select 'gold_identity_changes'
where exists (
  select 1 from {{ ref('mart_store_business') }} m
  join {{ source('pinned','gold_stores') }} g using (store_id)
  where m.overture_id is distinct from g.overture_id
     or m.latitude is distinct from g.latitude or m.longitude is distinct from g.longitude
     or m.identity_status is distinct from g.identity_status
     or m.is_identity_human_verified is distinct from g.is_identity_human_verified
     or m.gold_canonical_category is distinct from g.canonical_category
     or m.category_validation_status is distinct from g.category_validation_status
)
