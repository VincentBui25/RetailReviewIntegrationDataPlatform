with coverage as (
    select c.store_id, count(a.store_id) as n
    from {{ source('pinned_area', 'catalog') }} c
    left join {{ ref('store_area_mapping') }} a using (store_id)
    group by c.store_id
)
select * from coverage where n <> 1
union all
select a.store_id, -1 from {{ ref('store_area_mapping') }} a
left join {{ source('pinned_area', 'catalog') }} c using(store_id) where c.store_id is null
