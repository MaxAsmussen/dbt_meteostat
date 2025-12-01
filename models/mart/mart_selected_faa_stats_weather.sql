{{ config(materialized='table') }}

WITH daily_flights AS (
    SELECT
        flight_date::date AS date,
        airport,
        COUNT(*) AS total_flights,
        SUM(cancelled) AS cancelled_flights,
        SUM(diverted) AS diverted_flights,
        COUNT(*) - SUM(cancelled) - SUM(diverted) AS actual_flights
    FROM (
        SELECT
            flight_date,
            origin AS airport,
            cancelled,
            diverted
        FROM {{ ref('prep_flights') }}

        UNION ALL

        SELECT
            flight_date,
            dest AS airport,
            cancelled,
            diverted
        FROM {{ ref('prep_flights') }}
    ) f
    GROUP BY flight_date, airport
),

daily_inbound AS (
    SELECT
        flight_date::date AS date,
        dest AS airport,
        COUNT(DISTINCT origin) AS inbound_connections
    FROM {{ ref('prep_flights') }}
    GROUP BY flight_date, dest
),

daily_outbound AS (
    SELECT
        flight_date::date AS date,
        origin AS airport,
        COUNT(DISTINCT dest) AS outbound_connections
    FROM {{ ref('prep_flights') }}
    GROUP_BY flight_date, origin
)

SELECT
    w.date,
    w.airport_code AS airport,
    a.name,
    a.city,
    a.country,

    db.outbound_connections,
    da.inbound_connections,

    df.total_flights AS total_planned_flights,
    df.cancelled_flights AS total_cancelled_flights,
    df.diverted_flights AS total_diverted_flights,
    df.actual_flights,

    w.min_temp_c,
    w.max_temp_c,
    w.precipitation_mm,
    w.max_snow_mm,
    w.avg_wind_direction,
    w.avg_wind_speed_kmh,
    w.wind_peakgust_kmh

FROM {{ ref('prep_weather_daily') }} w
LEFT JOIN daily_flights df
    ON df.date = w.date AND df.airport = w.airport_code
LEFT JOIN daily_inbound da
    ON da.date = w.date AND da.airport = w.airport_code
LEFT JOIN daily_outbound db
    ON db.date = w.date AND db.airport = w.airport_code
LEFT JOIN {{ ref('prep_airports') }} a
    ON a.faa = w.airport_code

WHERE w.airport_code IN ('LAX','MIA','JFK')

