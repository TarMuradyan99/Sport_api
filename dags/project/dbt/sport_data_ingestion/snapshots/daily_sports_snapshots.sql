{% snapshot daily_sports_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='event_id',
        strategy='check',
        check_cols=[
            'sport_group',
            'sport_title',
            'sport_description',
            'is_active',
            'has_outrights'
        ]
    )
}}

select
    event_id,
    sport_key,
    sport_group,
    sport_title,
    sport_description,
    is_active,
    has_outrights
from {{ ref('daily_sports_inter') }}

{% endsnapshot %}