{{config(materialized='table', engine='MergeTree()', order_by='event_id')}}


with sports as (
    select * from {{ ref('daily_sports_inter') }}
)

select * from sports