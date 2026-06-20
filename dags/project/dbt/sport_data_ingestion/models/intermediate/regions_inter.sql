select
    region_id,
    region as region_code,
    multiIf(
        region = 'eu', 'Europe',
        region = 'us', 'United States',
        region = 'au', 'Australia',
        region
    ) as region_name
from {{ ref('daily_sports_stg') }}
where region_id is not null
  and region != ''
group by
    region_id,
    region
