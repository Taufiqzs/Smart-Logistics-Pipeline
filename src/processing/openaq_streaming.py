import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows


# Batas umur data dalam jam untuk menentukan apakah observasi masih CURRENT atau sudah STALE.
FRESHNESS_THRESHOLD_HOURS = 24


# Kelas `ParseMessageDoFn` mengubah pesan Pub/Sub dari bytes menjadi dictionary Python.
class ParseMessageDoFn(beam.DoFn):
# Menyimpan invalid tag yang digunakan pada proses ini.
    INVALID_TAG = "invalid"

# Fungsi `process()` menjalankan pemrosesan setiap pesan yang diterima.
    def process(self, message):
        try:
# Menyimpan record yang digunakan pada proses ini.
            record = json.loads(message.decode("utf-8"))

# Menyimpan daftar field yang wajib tersedia pada setiap event.
            required_fields = [
                "source",
                "ingestion_timestamp",
                "location_id",
                "value",
                "measurement_datetime",
            ]

# Menyimpan missing yang digunakan pada proses ini.
            missing = [
                field for field in required_fields
                if field not in record
            ]

            if missing:
                record["validation_status"] = "INVALID"
                record["validation_error"] = (
                    f"Missing required fields: {missing}"
                )
                yield beam.pvalue.TaggedOutput(
                    self.INVALID_TAG,
                    record,
                )
                return

            yield record

        except Exception as exc:
            yield beam.pvalue.TaggedOutput(
                self.INVALID_TAG,
                {
                    "validation_status": "INVALID",
                    "validation_error": f"JSON parsing error: {exc}",
                    "raw_message": message.decode(
                        "utf-8",
                        errors="replace",
                    ),
                },
            )


# Kelas `ValidateAndEnrichDoFn` memvalidasi dan memperkaya event OpenAQ sebelum diteruskan.
class ValidateAndEnrichDoFn(beam.DoFn):
# Menyimpan invalid tag yang digunakan pada proses ini.
    INVALID_TAG = "invalid"

# Fungsi `process()` menjalankan pemrosesan setiap pesan yang diterima.
    def process(self, record):
        try:
# Menyimpan value yang digunakan pada proses ini.
            value = float(record["value"])

            if value < 0:
                record["validation_status"] = "INVALID"
                record["validation_error"] = (
                    f"Negative PM2.5 value: {value}"
                )
                yield beam.pvalue.TaggedOutput(
                    self.INVALID_TAG,
                    record,
                )
                return

# Menyimpan measurement utc yang digunakan pada proses ini.
            measurement_utc = record["measurement_datetime"]["utc"]
# Menyimpan ingestion timestamp yang digunakan pada proses ini.
            ingestion_timestamp = record["ingestion_timestamp"]

# Menyimpan measurement time yang digunakan pada proses ini.
            measurement_time = datetime.fromisoformat(
                measurement_utc.replace("Z", "+00:00")
            )

# Menyimpan ingestion time yang digunakan pada proses ini.
            ingestion_time = datetime.fromisoformat(
                ingestion_timestamp.replace("Z", "+00:00")
            )

# Menyimpan age hours yang digunakan pada proses ini.
            age_hours = (
                ingestion_time - measurement_time
            ).total_seconds() / 3600

            record["value"] = value
            record["validation_status"] = "VALID"
            record["measurement_age_hours"] = round(age_hours, 2)

            if age_hours <= FRESHNESS_THRESHOLD_HOURS:
                record["freshness_status"] = "CURRENT"
            else:
                record["freshness_status"] = "STALE"

            yield record

        except Exception as exc:
            record["validation_status"] = "INVALID"
            record["validation_error"] = str(exc)

            yield beam.pvalue.TaggedOutput(
                self.INVALID_TAG,
                record,
            )


# Membangun dan menjalankan pipeline Apache Beam/Dataflow.
def run():
# Menyimpan options yang digunakan pada proses ini.
    options = PipelineOptions(
        save_main_session=True
    )

# Menyimpan standard options yang digunakan pada proses ini.
    standard_options = options.view_as(StandardOptions)
# Menyimpan streaming yang digunakan pada proses ini.
    standard_options.streaming = True

    with beam.Pipeline(options=options) as pipeline:

# Menyimpan parsed yang digunakan pada proses ini.
        parsed = (
            pipeline
            | "ReadOpenAQ"
            >> beam.io.ReadFromPubSub(
                topic="projects/jcdeah-009/topics/openaq-events"
            )
            | "ParseJSON"
            >> beam.ParDo(ParseMessageDoFn()).with_outputs(
                "invalid",
                main="valid",
            )
        )

# Menyimpan validated yang digunakan pada proses ini.
        validated = (
            parsed.valid
            | "ValidateAndEnrich"
            >> beam.ParDo(
                ValidateAndEnrichDoFn()
            ).with_outputs(
                "invalid",
                main="valid",
            )
        )

# Menyimpan silver yang digunakan pada proses ini.
        silver = validated.valid

# Menyimpan audit from parse yang digunakan pada proses ini.
        audit_from_parse = parsed.invalid
# Menyimpan audit from validation yang digunakan pada proses ini.
        audit_from_validation = validated.invalid

# Menyimpan audit yang digunakan pada proses ini.
        audit = (
            (
                audit_from_parse,
                audit_from_validation,
            )
            | "MergeAudit"
            >> beam.Flatten()
        )

        (
            silver
            | "SilverWindow"
            >> beam.WindowInto(
                FixedWindows(60)
            )
            | "SilverJSON"
            >> beam.Map(json.dumps)
            | "WriteSilver"
            >> beam.io.WriteToText(
                "gs://jcdeah-009-smart-logistics-silver/openaq/events",
                file_name_suffix=".jsonl",
                num_shards=1,
            )
        )

        (
            audit
            | "AuditWindow"
            >> beam.WindowInto(
                FixedWindows(60)
            )
            | "AuditJSON"
            >> beam.Map(json.dumps)
            | "WriteAudit"
            >> beam.io.WriteToText(
                "gs://jcdeah-009-smart-logistics-raw/audit/openaq/events",
                file_name_suffix=".jsonl",
                num_shards=1,
            )
        )


if __name__ == "__main__":
    run()