-- Berkas dbt/tests/risk_band_consistency.sql
-- Memastikan kategori risiko sesuai dengan rentang risk_score yang ditetapkan.

SELECT
    event_date,
    city,
    risk_score,
    risk_band

FROM {{ ref('fact_area_risk_daily') }}

WHERE

    (
        risk_band = 'LOW'
        AND NOT (risk_score < 25)
    )

    OR

    (
        risk_band = 'MODERATE'
        AND NOT (risk_score >= 25 AND risk_score < 50)
    )

    OR

    (
        risk_band = 'HIGH'
        AND NOT (risk_score >= 50 AND risk_score < 75)
    )

    OR

    (
        risk_band = 'CRITICAL'
        AND NOT (risk_score >= 75)
    )

    OR

    (
        risk_band = 'INSUFFICIENT_DATA'
        AND NOT (
            weather_risk IS NULL
            AND air_quality_risk IS NULL
            AND delay_rate IS NULL
        )
    )
