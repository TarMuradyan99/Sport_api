select
    sport_key,
    any(group_name) as group_name,
    any(sport_title) as sport_title,
    any(sport_description) as sport_description,
    max(active) as active,
    max(has_outrights) as has_outrights
from {{ ref('odds_api_sports_stg') }}
where sport_key != ''
group by sport_key
