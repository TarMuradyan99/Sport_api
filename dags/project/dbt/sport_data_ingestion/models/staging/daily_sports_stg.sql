select
    region_id,
    region,
    event_id,
    sport_key,
    sport_title,
    parseDateTimeBestEffortOrNull(commence_time) as commence_time,
    home_team,
    away_team,
    bookmaker_keys,
    payload,
    ingested_at
from {{ source('raw', 'daily_sports') }}
