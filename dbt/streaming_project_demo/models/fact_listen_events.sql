{{ config(materialized='table') }}

select 
    --- this dbt function lets me do select * except, as in Spark and DuckDB sql (redshift sql SUCKS)
    *
    , TO_CHAR(TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second', 'YYYYYMMDDHH')  AS date_key

from {{ source('stage','listen_events')}}




