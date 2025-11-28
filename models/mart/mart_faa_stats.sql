{{ config(
    materialized='table'
) }}

WITH inbound AS (
    SELECT dest AS airport, origin
    FROM {{ ref('prep_flights') }}
    GROUP BY dest, origin
),
outbound AS (
    SELECT origin AS airport, dest
    FROM {{ ref('prep_flights') }}
    GROUP BY origin, dest
),
departure_stats AS (
    SELECT 
        origin AS airport,
        COUNT(*) AS total_departures,
        SUM(cancelled) AS total_cancelled,
        SUM(diverted) AS total_diverted
    FROM {{ ref('prep_flights') }}
    GROUP BY origin
),
arrivals_stats AS (
    SELECT 
        dest AS airport,
        COUNT(*) AS total_arrivals,
        SUM(cancelled) AS total_cancelled_arr,
        SUM(diverted) AS total_diverted_arr
    FROM {{ ref('prep_flights') }}
    GROUP BY dest
)
SELECT
    a.faa AS airport,
    a.name,
    COUNT(DISTINCT i.origin) AS unique_inbound_connections,
    COUNT(DISTINCT o.dest) AS unique_outbound_connections,
    ds.total_departures + ars.total_arrivals AS total_planned_flights,
    ds.total_cancelled + ars.total_cancelled_arr AS total_cancelled_flights,
    ds.total_diverted + ars.total_diverted_arr AS total_diverted_flights,
    (ds.total_departures + ars.total_arrivals)
        - (ds.total_cancelled + ars.total_cancelled_arr + ds.total_diverted + ars.total_diverted_arr)
        AS actual_flights
FROM {{ ref('prep_airports') }} a
LEFT JOIN inbound i ON i.airport = a.faa
LEFT JOIN outbound o ON o.airport = a.faa
LEFT JOIN departure_stats ds ON ds.airport = a.faa
LEFT JOIN arrivals_stats ars ON ars.airport = a.faa
GROUP BY 
    a.faa, 
    a.name,
    ds.total_departures, 
    ds.total_cancelled,
    ds.total_diverted,
    ars.total_arrivals,
    ars.total_cancelled_arr,
    ars.total_diverted_arr
