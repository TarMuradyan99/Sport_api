{{
    config(materialized='table', engine='MergeTree()', order_by='sport_id')
}}


with odds_api_sport_inter as (
    select *,row_number() over(partition by event_id order by commence_time desc,ingested_at desc) as rank from {{ ref('daily_sports_stg') }}
)

select 
 *
from odds_api_sport_inter
where rank = 1