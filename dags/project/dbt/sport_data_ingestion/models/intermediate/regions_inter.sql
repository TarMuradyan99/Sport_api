{{
    config(materialized='table', engine='ReplacingMergeTree()', order_by='sport_id')
}}


with odds_api_sport_inter as (
    select * from {{ ref('daily_sports_stg') }}
)

select 
 distinct region_id,
 region
from odds_api_sport_inter
where region_id is not null