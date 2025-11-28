{{ config(
    materialized='table'
) }}

WITH route_stats AS (
    SELECT
        f.origin,
        f.dest,
        COUNT(*) AS total_flights,
        COUNT(DISTINCT f.tail_number) AS unique_airplanes,
        COUNT(DISTINCT f.airline) AS unique_airlines,
        AVG(f.actual_elapsed_time) AS avg_elapsed_time,
        AVG(f.arr_delay) AS avg_arrival_delay,
        MAX(f.arr_delay) AS max_arrival_delay,
        MIN(f.arr_delay) AS min_arrival_delay,
        SUM(f.cancelled) AS total_cancelled,
        SUM(f.diverted) AS total_diverted
    FROM {{ ref('prep_flights') }} f
    GROUP BY f.origin, f.dest
)

SELECT
    rs.origin,
    o.name AS origin_name,
    o.city AS origin_city,
    o.country AS origin_country,
    rs.dest,
    d.name AS dest_name,
    d.city AS dest_city,
    d.country AS dest_country,
    rs.total_flights,
    rs.unique_airplanes,
    rs.unique_airlines,
    rs.avg_elapsed_time,
    rs.avg_arrival_delay,
    rs.max_arrival_delay,
    rs.min_arrival_delay,
    rs.total_cancelled,
    rs.total_diverted
FROM route_stats rs
LEFT JOIN {{ ref('prep_airports') }} o ON rs.origin = o.faa
LEFT JOIN {{ ref('prep_airports') }} d ON rs.dest = d.faa