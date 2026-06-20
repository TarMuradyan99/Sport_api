with ranked_events as (
    select
        concat(toString(region_id), '-', event_id) as event_region_key,
        region_id,
        region,
        event_id,
        sport_key,
        sport_title,
        commence_time,
        home_team,
        away_team,
        bookmaker_keys,
        payload,
        ingested_at,
        row_number() over (
            partition by region_id, event_id
            order by ingested_at desc
        ) as event_rank
    from {{ ref('daily_sports_stg') }}
    where event_id != ''
)

select
    event_region_key,
    region_id,
    region,
    event_id,
    sport_key,
    sport_title,
    commence_time,
    home_team,
    away_team,
    bookmaker_keys,
    payload,
    ingested_at
from ranked_events
where event_rank = 1
