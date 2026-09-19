-- Berkas sql/bigquery/build_openaq_air_quality_daily.sql
-- Mengagregasi observasi PM2.5 terkini menjadi metrik kualitas udara harian per kota.

MERGE `jcdeah-009.smart_logistics_silver.openaq_air_quality_daily` AS target

USING (

  WITH ranked_observations AS (

    SELECT
      DATE(TIMESTAMP(e.measurement_datetime.utc)) AS event_date,
      m.city,
      e.location_id,
      e.value AS pm25,
      TIMESTAMP(e.measurement_datetime.utc) AS measurement_utc,
      e.ingestion_timestamp,

      ROW_NUMBER() OVER (
        PARTITION BY
          DATE(TIMESTAMP(e.measurement_datetime.utc)),
          e.location_id

        ORDER BY
          TIMESTAMP(e.measurement_datetime.utc) DESC,
          e.ingestion_timestamp DESC
      ) AS rn

    FROM
      `jcdeah-009.smart_logistics_silver.openaq_events_external` e

    INNER JOIN
      `jcdeah-009.smart_logistics_silver.openaq_station_mapping` m
      ON e.location_id = m.location_id

    WHERE
      e.validation_status = 'VALID'
      AND e.freshness_status = 'CURRENT'
      AND e.parameter = 'pm25'
      AND e.parameter_id = 2
      AND e.value >= 0
  ),

  daily AS (

    SELECT
      event_date,
      city,

      AVG(pm25) AS avg_pm25,

      COUNT(DISTINCT location_id) AS station_count,

      COUNT(*) AS observation_count

    FROM ranked_observations

    WHERE rn = 1

    GROUP BY
      event_date,
      city
  )

  SELECT
    event_date,
    city,

    ROUND(avg_pm25, 2) AS avg_pm25,

    ROUND(
      LEAST(
        100.0,
        (avg_pm25 / 75.0) * 100.0
      ),
      2
    ) AS air_quality_risk,

    station_count,
    observation_count,

    CURRENT_TIMESTAMP() AS processed_at

  FROM daily

) AS source

ON
  target.event_date = source.event_date
  AND target.city = source.city

WHEN MATCHED THEN UPDATE SET
  avg_pm25 = source.avg_pm25,
  air_quality_risk = source.air_quality_risk,
  station_count = source.station_count,
  observation_count = source.observation_count,
  processed_at = source.processed_at

WHEN NOT MATCHED THEN INSERT (
  event_date,
  city,
  avg_pm25,
  air_quality_risk,
  station_count,
  observation_count,
  processed_at
)

VALUES (
  source.event_date,
  source.city,
  source.avg_pm25,
  source.air_quality_risk,
  source.station_count,
  source.observation_count,
  source.processed_at
);
