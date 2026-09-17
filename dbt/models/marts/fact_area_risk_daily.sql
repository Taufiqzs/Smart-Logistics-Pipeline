with delivery as (
  select * from {{ ref('fact_delivery_performance') }}
),
environment as (
  select
    event_date,
    city,
    max(weather_risk) as weather_risk,
    max(air_quality_risk) as air_quality_risk
  from `smart_logistics_silver.environment_risk_daily`
  group by 1, 2
)
select
  d.event_date,
  d.city,
  round(
    0.45 * coalesce(e.weather_risk, 0)
    + 0.35 * coalesce(e.air_quality_risk, 0)
    + 0.20 * least(100, d.delay_rate * 100)
  , 2) as risk_score,
  d.orders,
  d.avg_actual_delivery_min,
  d.avg_delay_min,
  d.delay_rate
from delivery d
left join environment e
  using(event_date, city)
