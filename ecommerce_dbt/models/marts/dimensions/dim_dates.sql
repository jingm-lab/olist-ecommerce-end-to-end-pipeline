{{ config(materialized='table') }}

WITH dates_cte AS (
    SELECT DATEADD(day, seq4(), '2016-01-01'::DATE) as date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 3653))
)

SELECT 
    date_day,
    YEAR(date_day) AS date_year,
    MONTH(date_day) AS date_month,
    DAY(date_day) AS date_day_of_month,
    CASE
        WHEN DAYOFWEEK(date_day) = 0 OR DAYOFWEEK(date_day) = 6 THEN True
        ELSE False
    END AS is_weekend,
    CASE
        WHEN DAYOFWEEK(date_day) = 0 THEN 'Sunday'
        WHEN DAYOFWEEK(date_day) = 1 THEN 'Monday'
        WHEN DAYOFWEEK(date_day) = 2 THEN 'Tuesday'
        WHEN DAYOFWEEK(date_day) = 3 THEN 'Wednesday'
        WHEN DAYOFWEEK(date_day) = 4 THEN 'Thursday'
        WHEN DAYOFWEEK(date_day) = 5 THEN 'Friday'
        ELSE 'Saturday'
    END AS day_of_week,
    DAYOFWEEK(date_day) AS day_of_week_num,
    QUARTER(date_day) AS date_quarter
FROM dates_cte
