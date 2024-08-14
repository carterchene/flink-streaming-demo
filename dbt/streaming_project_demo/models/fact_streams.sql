{{ config(
  materialized = 'table'
  ) }}

with listen_events_w_ts as (
    select {{ dbt_utils.star(from=source('stage','listen_events'), except=['ts']) }}, TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second'  AS ts
    from {{source('stage','listen_events')}}
)

SELECT 
    dim_users.userKey AS userKey,
    dim_artists.artistKey AS artistKey,
    dim_songs.songKey AS songKey ,
    dim_date.date_key AS date_key,
    dim_location.locationKey AS locationKey,
    listen_events.ts as ts
 FROM listen_events_w_ts listen_events
  LEFT JOIN {{ ref('dim_users') }} 
    ON listen_events.userId = dim_users.userId AND CAST(listen_events.ts AS DATE) >= dim_users.rowActivationDate AND CAST(listen_events.ts AS DATE) < dim_users.RowExpirationDate
  LEFT JOIN {{ ref('dim_artists') }} 
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_artists.name
  LEFT JOIN {{ ref('dim_songs') }} 
    ON REPLACE(REPLACE(listen_events.artist, '"', ''), '\\', '') = dim_songs.artistName AND listen_events.song = dim_songs.title
  LEFT JOIN {{ ref('dim_location') }} 
    ON listen_events.city = dim_location.city AND listen_events.state = dim_location.stateCode AND listen_events.lat = dim_location.latitude AND listen_events.lon = dim_location.longitude 
  LEFT JOIN {{ ref('dim_date') }} 
    ON dim_date.date = date_trunc('hour', listen_events.ts)