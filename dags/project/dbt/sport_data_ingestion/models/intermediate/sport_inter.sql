{{config(materialized='table', engine='ReplacingMergeTree()', order_by='sport_key')}}


with odds_source_inter as (

    select *
    from {{ ref('daily_sports_stg') }}

)


select 
distinct sport_key,
sport_title

from odds_source_inter