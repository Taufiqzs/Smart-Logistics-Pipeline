-- Berkas dbt/models/marts/fact_area_risk_daily.sql
-- Menghasilkan mart risiko area harian dengan menggabungkan cuaca, kualitas udara, dan keterlambatan logistik.

WITH delivery AS (

    SELECT
        event_date,
        city,
        orders,
        avg_actual_delivery_min,
        avg_expected_delivery_min,
        avg_delay_min,
        delay_rate,
        avg_route_rating
    FROM {{ ref('fact_delivery_performance') }}

),

weather AS (

    SELECT
        event_date,
        city,
        MAX(weather_risk) AS weather_risk,
        MAX(active_weather_alerts) AS active_weather_alerts,
        COUNTIF(weather_risk IS NOT NULL) > 0 AS has_weather_data

    FROM `jcdeah-009.smart_logistics_silver.environment_risk_daily`

    GROUP BY
        event_date,
        city

),

air_quality AS (

    SELECT
        event_date,
        city,
        avg_pm25,
        air_quality_risk,
        station_count,
        observation_count

    FROM `jcdeah-009.smart_logistics_silver.openaq_air_quality_daily`

),

date_city_spine AS (

    SELECT event_date, city
    FROM delivery

    UNION DISTINCT

    SELECT event_date, city
    FROM weather

    UNION DISTINCT

    SELECT event_date, city
    FROM air_quality

),

combined AS (

    SELECT

        s.event_date,
        s.city,

        -- Komponen logistik
        d.orders,
        d.avg_actual_delivery_min,
        d.avg_expected_delivery_min,
        d.avg_delay_min,
        d.delay_rate,
        d.avg_route_rating,

        -- Komponen cuaca
        w.weather_risk,
        w.active_weather_alerts,
        COALESCE(w.has_weather_data, FALSE)
            AS has_weather_data,

        -- Komponen kualitas udara
        a.avg_pm25,
        a.air_quality_risk,
        a.station_count,
        a.observation_count,

        a.air_quality_risk IS NOT NULL
            AS has_air_quality_data

    FROM date_city_spine s

    LEFT JOIN delivery d
        ON s.event_date = d.event_date
        AND s.city = d.city

    LEFT JOIN weather w
        ON s.event_date = w.event_date
        AND s.city = w.city

    LEFT JOIN air_quality a
        ON s.event_date = a.event_date
        AND s.city = a.city

),

scored AS (

    SELECT

        *,

        ROUND(
            0.45 * COALESCE(weather_risk, 0)
            + 0.35 * COALESCE(air_quality_risk, 0)
            + 0.20 * COALESCE(
                LEAST(100, delay_rate * 100),
                0
            ),
            2
        ) AS risk_score,

        CASE

            WHEN weather_risk IS NULL
                 AND air_quality_risk IS NULL
                 AND delay_rate IS NULL
            THEN 'INSUFFICIENT_DATA'

            WHEN
                0.45 * COALESCE(weather_risk, 0)
                + 0.35 * COALESCE(air_quality_risk, 0)
                + 0.20 * COALESCE(
                    LEAST(100, delay_rate * 100),
                    0
                ) < 25
            THEN 'LOW'

            WHEN
                0.45 * COALESCE(weather_risk, 0)
                + 0.35 * COALESCE(air_quality_risk, 0)
                + 0.20 * COALESCE(
                    LEAST(100, delay_rate * 100),
                    0
                ) < 50
            THEN 'MODERATE'

            WHEN
                0.45 * COALESCE(weather_risk, 0)
                + 0.35 * COALESCE(air_quality_risk, 0)
                + 0.20 * COALESCE(
                    LEAST(100, delay_rate * 100),
                    0
                ) < 75
            THEN 'HIGH'

            ELSE 'CRITICAL'

        END AS risk_band

    FROM combined

)

SELECT

    event_date,
    city,

    -- Komponen logistik
    orders,
    avg_actual_delivery_min,
    avg_expected_delivery_min,
    avg_delay_min,
    delay_rate,
    avg_route_rating,

    -- Komponen cuaca
    weather_risk,
    active_weather_alerts,
    has_weather_data,

    -- Komponen kualitas udara
    avg_pm25,
    air_quality_risk,
    station_count,
    observation_count,
    has_air_quality_data,

    -- Skor risiko akhir
    risk_score,
    risk_band

FROM scored
