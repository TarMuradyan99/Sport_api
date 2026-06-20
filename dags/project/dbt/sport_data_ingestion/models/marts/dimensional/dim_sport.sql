{{ config(order_by='sport_key') }}

select
    sport_key,
    group_name,
    sport_title,
    sport_description,
    active,
    has_outrights
from {{ ref('sport_inter') }}
