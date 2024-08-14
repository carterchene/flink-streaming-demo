{{ config(materialized = 'table') }}

WITH base_data AS (
    SELECT DISTINCT
        userId,
        firstName,
        lastName,
        gender,
        level,
        registration,
        TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second'  AS date
    FROM {{ source('stage', 'listen_events') }}
    WHERE userId <> 0
),

lagged_data AS (
    SELECT *,
        LAG(level) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date) AS prev_level,
        CASE 
            WHEN LAG(level) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date ) IS NULL 
                OR LAG(level) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date ) <> level 
            THEN 1 
            ELSE 0 
        END AS lagged
    FROM base_data
),

grouped_data AS (
    SELECT *,
        SUM(lagged) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grouped
    FROM lagged_data
),

min_dates AS (
    SELECT 
        userId, 
        firstName, 
        lastName, 
        gender, 
        registration, 
        level, 
        grouped, 
        DATE(MIN(date)) AS minDate
    FROM grouped_data
    GROUP BY userId, firstName, lastName, gender, registration, level, grouped
),

final_data AS (
    SELECT 
        CAST(userId AS BIGINT) AS userId, 
        firstName, 
        lastName, 
        gender, 
        level, 
        CAST(registration AS BIGINT) AS registration, 
        minDate AS rowActivationDate,
        COALESCE(
            LEAD(minDate) OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped),
            '9999-12-31'::DATE
        ) AS rowExpirationDate,
        CASE 
            WHEN ROW_NUMBER() OVER(PARTITION BY userId, firstName, lastName, gender ORDER BY grouped DESC) = 1 
            THEN 1 
            ELSE 0 
        END AS currentRow
    FROM min_dates
),

zero_one_users AS (
    SELECT 
        CAST(userId AS BIGINT) AS userId, 
        firstName, 
        lastName, 
        gender, 
        level, 
        CAST(registration AS BIGINT) AS registration, 
        DATE(MIN(ts)) AS rowActivationDate, 
        '9999-12-31'::DATE AS rowExpirationDate, 
        1 AS currentRow
    FROM {{ source('stage', 'listen_events') }}
    WHERE userId = 0 OR userId = 1
    GROUP BY userId, firstName, lastName, gender, level, registration
),

combined_data AS (
    SELECT * FROM final_data
    UNION ALL
    SELECT * FROM zero_one_users
)

SELECT 
    {{ dbt_utils.surrogate_key(['userId', 'rowActivationDate', 'level']) }} AS userKey,
    *
FROM combined_data