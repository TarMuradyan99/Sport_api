{% snapshot daily_sports_snapshots %}

{{
    config(
        target_schema='snapshots',
        unique_key='event_region_key',
        strategy='check',
        check_cols=[
            'region',
            'sport_key',
            'sport_title',
            'commence_time',
            'home_team',
            'away_team',
            'bookmaker_keys',
            'payload'
        ]
    )
}}

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
from {{ ref('daily_sports_inter') }}

{% endsnapshot %}
