   {{config(materialized='view')}}


   with source_data as (
    select  
        *
         from {{source('raw','daily_sports')}}
   )

   select 
        cast(region_id as int) as region_id,
        nullif(cast(region as varchar(100)), '') as region,
        nullif(cast(event_id as string), '') as sport_id,
        nullif(cast(sport_key as string), '') as sport_key,
        nullif(cast(sport_title as string), '') as sport_title,
        nullif(cast(commence_time as timestamp), '') as commence_time,
        nullif(cast(home_team as string), '') as home_team,
        nullif(cast(away_team as string), '') as away_team,
        nullif(cast(bookmaker_keys as array<string>), '') as bookmaker_keys,
        nullif(cast(payload as string), '') as payload,
        nullif(cast(ingested_at as timestamp), '') as ingested_at
    from source_data
    where region_id is not null