{{ config(materialized='table') }}

WITH RECURSIVE datetime_series(datetime) AS (
    SELECT 
        '2020-01-01 00:00:00'::timestamp AS datetime
    UNION ALL
    SELECT 
        (datetime + INTERVAL '1 hour')::timestamp
    FROM 
        datetime_series
    WHERE 
        datetime < '2029-12-31 23:00:00'::timestamp
)
SELECT
    TO_CHAR("datetime", 'YYYYMMDDHH') as date_key, 
    -- TO_CHAR(datetime, 'YYYYMMDDHH') as date_key,
    datetime AS datetime,
    DATE(datetime) AS date,
    EXTRACT(YEAR FROM datetime) AS year,
    EXTRACT(QUARTER FROM datetime) AS quarter,
    EXTRACT(MONTH FROM datetime) AS month,
    EXTRACT(WEEK FROM datetime) AS week,
    EXTRACT(DOW FROM datetime) AS day_of_week,
    EXTRACT(DAY FROM datetime) AS day_of_month,
    EXTRACT(DOY FROM datetime) AS day_of_year,
    EXTRACT(HOUR FROM datetime) AS hour,
    CASE 
        WHEN EXTRACT(HOUR FROM datetime) >= 0 AND EXTRACT(HOUR FROM datetime) < 6 THEN 'Night'
        WHEN EXTRACT(HOUR FROM datetime) >= 6 AND EXTRACT(HOUR FROM datetime) < 12 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM datetime) >= 12 AND EXTRACT(HOUR FROM datetime) < 18 THEN 'Afternoon'
        ELSE 'Evening'
    END AS time_of_day,
    CASE WHEN EXTRACT(DOW FROM datetime) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM datetime_series