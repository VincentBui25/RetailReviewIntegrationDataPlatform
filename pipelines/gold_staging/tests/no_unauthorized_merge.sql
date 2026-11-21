select source, source_id_kind, source_id
from {{ ref('store_source_mapping') }}
group by all having count(*) <> 1
union all
select m.source, m.source_id_kind, m.source_id
from {{ ref('store_source_mapping') }} m
left join {{ ref('dim_store') }} d on d.overture_id = m.source_id
where m.source = 'overture'
  and (m.source_id_kind is distinct from 'overture_id'
       or d.overture_id is null or m.store_id is distinct from d.store_id)
union all
select 'accepted_google', google_id_kind, google_identity_value
from {{ ref('dim_store') }} where google_identity_value is not null
group by all having count(*) <> 1
