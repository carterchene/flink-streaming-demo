{{ config(
  materialized = 'table'
  ) }}

SELECT 
    dim_users.userKey AS userKey,
    dim_artists.artistKey AS artistKey,
    dim_songs.songKey AS songKey ,
    dim_location.locationKey AS locationKey,
    listen_events.date_key as date_key,
    listen_events.ts as ts 
 FROM {{ref('fact_listen_events')}} listen_events
  LEFT JOIN {{ref('dim_date')}} 
    ON listen_events.date_key = dim_date.date_key 
  LEFT JOIN {{ ref('dim_users') }} 
    ON listen_events.userId = dim_users.userId -- AND CAST(dim_date.datetime AS DATE) >= dim_users.rowActivationDate AND CAST(dim_date.datetime AS DATE) < dim_users.RowExpirationDate
  LEFT JOIN {{ ref('dim_artists') }} 
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_artists.name
  LEFT JOIN {{ ref('dim_songs') }} 
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_songs.artistName AND listen_events.song = dim_songs.title
  LEFT JOIN {{ ref('dim_location') }} 
    ON listen_events.city = dim_location.city AND listen_events.state = dim_location.stateCode AND listen_events.lat = dim_location.latitude AND listen_events.lon = dim_location.longitude 