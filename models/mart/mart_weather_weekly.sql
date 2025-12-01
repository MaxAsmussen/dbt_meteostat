{{ config(
    materialized='table'
) }}
WITH daily AS (
    SELECT
        date,
        airport_code,
        min_temp_c,
        max_temp_c,
        precipitation_mm,
        max_snow_mm,
        avg_wind_direction,
        avg_wind_speed_kmh,
        wind_peakgust_kmh
    FROM {{ ref('prep_weather_daily') }}
),
weekly AS (
    SELECT
        airport_code,
        DATE_TRUNC('week', date) AS week_start,
        AVG(min_temp_c) AS min_temp_c_avg,
        AVG(max_temp_c) AS max_temp_c_avg,
        SUM(precipitation_mm) AS total_precipitation_mm,
        SUM(max_snow_mm) AS total_snow_mm,
        ROUND(AVG(avg_wind_direction)) AS avg_wind_direction,
        AVG(avg_wind_speed_kmh) AS avg_wind_speed_kmh,
        MAX(wind_peakgust_kmh) AS max_wind_peakgust_kmh
    FROM daily
    GROUP BY airport_code, DATE_TRUNC('week', date)
)
SELECT *
FROM weekly
ORDER BY airport_code, week_start
