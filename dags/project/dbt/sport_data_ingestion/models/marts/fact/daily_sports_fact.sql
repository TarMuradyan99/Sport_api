{{ config(order_by=['region_id', 'sport_key', 'event_id']) }}

select
    event_region_key,
    event_id,
    region_id,
    sport_key,
    commence_time,
    home_team,
    away_team,
    bookmaker_keys,
    length(bookmaker_keys) as bookmaker_count,
    payload,
    ingested_at
from {{ ref('daily_sports_inter') }}
