select * from {{ ref('store_area_mapping') }}
where (assignment_status = 'AREA_CONFLICT' and (ward_code is not null or assignment_method <> 'UNRESOLVED'))
   or (assignment_status = 'RESOLVED' and (ward_code is null or assignment_method = 'UNRESOLVED'))
   or (assignment_method = 'ADDRESS' and ward_code is distinct from address_ward_code)
   or (assignment_method = 'SPATIAL' and ward_code is distinct from spatial_ward_code)
   or (address_ward_code is not null and spatial_ward_code is not null
       and address_ward_code <> spatial_ward_code and assignment_status <> 'AREA_CONFLICT')
