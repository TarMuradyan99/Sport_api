   {{config(materialised='view')}}


   with odds_source_data as (
    select * from {{source('raw','odds_api_sports')}}
   )

   select 
   cast(key as string) as sport_key,
   nullif(trim(cast(group_name as string)), '') as sport_title,
   nullif(trim(cast(title as string)), '') as sport_title_alt,
   nullif(trim(cast(description as string)), '') as description,
   nullif(trim(cast(active as boolean)), '') as active,
   nullif(trim(cast(has_outrights as timestamp)), '') as has_outrights,
   from odds_source_data
   where key is not null