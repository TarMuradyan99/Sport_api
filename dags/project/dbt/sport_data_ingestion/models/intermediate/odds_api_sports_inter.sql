{{ 
  config(
    materialized='table',
    engine='MergeTree()',
    order_by='key'
  ) 
}}

with odds_source_inter as (

    select *
    from {{ ref('odds_api_sports_stg') }}

)

select distinct
    *
from odds_source_inter