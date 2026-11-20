-- Both aggregates must reconcile exactly to their published input grains.
select 'catalog' as failed_grain
where (select coalesce(sum(catalog_store_count),0) from {{ ref('mart_store_category_status') }})
   <> (select count(*) from {{ ref('dim_store') }})
union all
select 'source'
where (select coalesce(sum(source_observation_count),0) from {{ ref('mart_source_reconciliation') }})
   <> (select count(*) from {{ ref('source_accounting') }})
