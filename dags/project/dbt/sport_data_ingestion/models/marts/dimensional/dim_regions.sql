{{ config(order_by='region_id') }}

select
    region_id,
    region_code,
    region_name
from {{ ref('regions_inter') }}
