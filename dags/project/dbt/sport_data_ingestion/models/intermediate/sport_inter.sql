with catalog_sports as (
    select
        sport_key,
        group_name,
        sport_title,
        sport_description,
        active,
        has_outrights
    from {{ ref('odds_api_sports_inter') }}
),

event_sports as (
    select
        sport_key,
        any(sport_title) as sport_title
    from {{ ref('daily_sports_stg') }}
    where sport_key != ''
    group by sport_key
)

select
    coalesce(catalog_sports.sport_key, event_sports.sport_key) as sport_key,
    coalesce(catalog_sports.group_name, '') as group_name,
    coalesce(catalog_sports.sport_title, event_sports.sport_title, '') as sport_title,
    coalesce(catalog_sports.sport_description, '') as sport_description,
    coalesce(catalog_sports.active, 0) as active,
    coalesce(catalog_sports.has_outrights, 0) as has_outrights
from catalog_sports
full outer join event_sports
    on catalog_sports.sport_key = event_sports.sport_key
