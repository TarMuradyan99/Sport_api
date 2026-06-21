{{config(materialized='table', engine='ReplacingMergeTree()', order_by='sport_key')}}


with sports as (
    select * from {{ ref('sport_inter') }}
)

select * from sports