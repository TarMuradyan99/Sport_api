select
    key as sport_key,
    group_name,
    title as sport_title,
    description as sport_description,
    active,
    has_outrights
from {{ source('raw', 'odds_api_sports') }}
