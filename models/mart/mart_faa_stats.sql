{{ config(
    materialized='table'
) }}

SELECT
    a.faa AS airport,
    a.name,
    COUNT(DISTINCT CASE WHEN f.dest = a.faa THEN f.origin END) AS unique_inbound_connections,
    COUNT(DISTINCT CASE WHEN f.origin = a.faa THEN f.dest END) AS unique_outbound_connections,
    COUNT(*) AS total_planned_flights,
    SUM(f.cancelled) AS total_cancelled_flights,
    SUM(f.diverted) AS total_diverted_flights,
    COUNT(*) - SUM(f.cancelled) - SUM(f.diverted) AS actual_flights
FROM {{ ref('prep_airports') }} a
LEFT JOIN {{ ref('prep_flights') }} f
ON f.origin = a.faa OR f.dest = a.faa
GROUP BY a.faa, a.name
