{{config(materialized='table', engine='ReplacingMergeTree()', order_by='region_id')}}


with regions_inter as (
    select * from {{ ref('regions_inter') }}
)

select * from regions_inter