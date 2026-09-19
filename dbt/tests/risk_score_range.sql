-- Berkas dbt/tests/risk_score_range.sql
-- Memastikan risk_score selalu berada pada rentang 0 sampai 100.

SELECT
    event_date,
    city,
    risk_score

FROM {{ ref('fact_area_risk_daily') }}

WHERE
    risk_score < 0
    OR risk_score > 100
