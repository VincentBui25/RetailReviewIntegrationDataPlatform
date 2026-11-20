with reviews as ({{ current_reviews() }}), dates as (
    select distinct cast(published_at as date) as full_date from reviews where published_at is not null and timestamp_status='VERIFIED'
)
select cast(strftime(full_date,'%Y%m%d') as integer) as date_key, full_date,
       day(full_date) as day, month(full_date) as month, quarter(full_date) as quarter, year(full_date) as year
from dates
