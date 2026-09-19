import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import DoFn
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
)
from apache_beam.transforms import trigger
from apache_beam.transforms.window import FixedWindows

from src.common.config import (
    PROJECT_ID,
    REGION,
    SILVER_BUCKET,
    RAW_BUCKET,
    TEMP_BUCKET,
    OPENAQ_TOPIC,
)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Menyimpan logger yang digunakan pada proses ini.
LOGGER = logging.getLogger(__name__)


# ============================================================
# KONFIGURASI
# ============================================================

# Menyimpan openaq subscription yang digunakan pada proses ini.
OPENAQ_SUBSCRIPTION = (
    f"projects/{PROJECT_ID}/subscriptions/openaq-dataflow-sub"
)

# Menyimpan silver output yang digunakan pada proses ini.
SILVER_OUTPUT = (
    f"gs://{SILVER_BUCKET}/openaq/events"
)

# Menyimpan audit output yang digunakan pada proses ini.
AUDIT_OUTPUT = (
    f"gs://{RAW_BUCKET}/audit/openaq/events"
)


# Usia maksimum pengukuran OpenAQ yang masih diperbolehkan.
# Pengukuran yang lebih lama dari batas ini dianggap kedaluwarsa.
MAX_FRESHNESS_HOURS = 24


# ============================================================
# PARSING PESAN PUB/SUB
# ============================================================

# Kelas `ParseMessageDoFn` digunakan sebagai komponen pemrosesan Apache Beam untuk pipeline streaming.
class ParseMessageDoFn(DoFn):
    """
    Mem-parsing pesan OpenAQ dari Pub/Sub dari bytes menjadi dictionary Python.

    Pesan atau JSON yang tidak valid dikirim ke output samping 'invalid'.
    """

# Fungsi `process()` menjalankan pemrosesan setiap pesan yang diterima oleh Apache Beam.
    def process(self, element):
        try:
            # ReadFromPubSub dari Pub/Sub biasanya mengembalikan data dalam bentuk bytes.
            if isinstance(element, bytes):
# Menyimpan element yang digunakan pada proses ini.
                element = element.decode("utf-8")

# Menyimpan record yang digunakan pada proses ini.
            record = json.loads(element)

            if not isinstance(record, dict):
                raise ValueError("Message is not a JSON object")

            yield record

        except Exception as exc:
            LOGGER.warning(
                "OPENAQ_PARSE_ERROR error=%s",
                exc,
            )

            yield beam.pvalue.TaggedOutput(
                "invalid",
                {
                    "validation_status": "INVALID",
                    "validation_error": f"JSON_PARSE_ERROR: {exc}",
                    "raw_message": (
                        element.decode("utf-8", errors="replace")
                        if isinstance(element, bytes)
                        else str(element)
                    ),
                    "processed_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                },
            )


# ============================================================
# VALIDASI + PENGAYAAN
# ============================================================

# Kelas `ValidateAndEnrichDoFn` digunakan sebagai komponen pemrosesan Apache Beam untuk pipeline streaming.
class ValidateAndEnrichDoFn(DoFn):
    """
    Memvalidasi event PM2.5 OpenAQ dan memperkayanya dengan:

    - validation_status
    - validation_error
    - measurement_age_hours
    - freshness_status
    - processed_at

    Record yang valid dikirim ke output utama.
    Record yang tidak valid/kedaluwarsa dikirim ke output samping 'invalid'.
    """

# Fungsi `process()` menjalankan pemrosesan setiap pesan yang diterima oleh Apache Beam.
    def process(self, record):

# Menyimpan waktu ketika record selesai diproses.
        processed_at = datetime.now(timezone.utc)

# Menyimpan daftar kesalahan yang ditemukan selama validasi.
        validation_errors = []

        # ----------------------------------------------------
        # Validasi field yang wajib tersedia.
        # ----------------------------------------------------

# Menyimpan daftar field yang wajib tersedia pada setiap event.
        required_fields = [
            "source",
            "ingestion_timestamp",
            "location_id",
            "latitude",
            "longitude",
            "parameter",
            "value",
            "measurement_datetime",
        ]

        for field in required_fields:
            if field not in record:
                validation_errors.append(
                    f"MISSING_FIELD:{field}"
                )

        if validation_errors:
            record["validation_status"] = "INVALID"
            record["validation_error"] = ";".join(
                validation_errors
            )
            record["processed_at"] = processed_at.isoformat()

            yield beam.pvalue.TaggedOutput(
                "invalid",
                record,
            )

            return

        # ----------------------------------------------------
        # Validasi parameter
        # ----------------------------------------------------

# Menyimpan nama parameter pengukuran yang diterima dari OpenAQ.
        parameter = str(
            record.get("parameter") or ""
        ).lower()

# Menyimpan ID parameter OpenAQ untuk memastikan parameter yang diproses adalah PM2.5.
        parameter_id = record.get("parameter_id")

        if parameter != "pm25" and parameter_id != 2:
            validation_errors.append(
                "NOT_PM25"
            )

        # ----------------------------------------------------
        # Validasi nilai
        # ----------------------------------------------------

        try:
# Menyimpan nilai hasil pengukuran yang akan divalidasi.
            value = float(record["value"])

            if value < 0:
                validation_errors.append(
                    "NEGATIVE_VALUE"
                )

            if value != value:  # NaN
                validation_errors.append(
                    "NAN_VALUE"
                )

        except (TypeError, ValueError):
            validation_errors.append(
                "INVALID_VALUE"
            )

        # ----------------------------------------------------
        # Validasi geografis
        # ----------------------------------------------------

        try:
# Menyimpan koordinat lintang lokasi pengukuran.
            latitude = float(record["latitude"])
# Menyimpan koordinat bujur lokasi pengukuran.
            longitude = float(record["longitude"])

            # Batas geografis perkiraan Indonesia.
            if not (
                -11.0 <= latitude <= 6.0
                and 95.0 <= longitude <= 141.0
            ):
                validation_errors.append(
                    "OUTSIDE_INDONESIA_BOUNDS"
                )

        except (TypeError, ValueError):
            validation_errors.append(
                "INVALID_COORDINATES"
            )

        # ----------------------------------------------------
        # Timestamp pengukuran
        # ----------------------------------------------------

# Menyimpan informasi waktu pengukuran dari event OpenAQ.
        measurement_datetime = (
            record.get("measurement_datetime") or {}
        )

# Menyimpan waktu pengukuran dalam zona waktu UTC.
        measurement_utc = (
            measurement_datetime.get("utc")
            if isinstance(
                measurement_datetime,
                dict,
            )
            else None
        )

        if not measurement_utc:
            validation_errors.append(
                "MISSING_MEASUREMENT_UTC"
            )

# Menyimpan waktu pengukuran yang telah diproses.
            measurement_time = None

        else:
            try:
# Menyimpan waktu pengukuran yang telah diproses.
                measurement_time = (
                    datetime.fromisoformat(
                        measurement_utc.replace(
                            "Z",
                            "+00:00",
                        )
                    )
                )

                if measurement_time.tzinfo is None:
# Menyimpan waktu pengukuran yang telah diproses.
                    measurement_time = (
                        measurement_time.replace(
                            tzinfo=timezone.utc
                        )
                    )

# Menyimpan waktu pengukuran yang telah diproses.
                measurement_time = (
                    measurement_time.astimezone(
                        timezone.utc
                    )
                )

            except (TypeError, ValueError):
                validation_errors.append(
                    "INVALID_MEASUREMENT_TIMESTAMP"
                )

# Menyimpan waktu pengukuran yang telah diproses.
                measurement_time = None

        # ----------------------------------------------------
        # Kesegaran data
        # ----------------------------------------------------

# Menyimpan usia data pengukuran dalam satuan jam.
        measurement_age_hours = None
# Menyimpan status kesegaran data pengukuran.
        freshness_status = "UNKNOWN"

        if measurement_time is not None:
# Menyimpan usia data pengukuran dalam satuan jam.
            measurement_age_hours = (
                (
                    processed_at
                    - measurement_time
                ).total_seconds()
                / 3600.0
            )

            if measurement_age_hours < 0:
# Menyimpan status kesegaran data pengukuran.
                freshness_status = "FUTURE"

                validation_errors.append(
                    "FUTURE_MEASUREMENT"
                )

            elif (
                measurement_age_hours
                <= MAX_FRESHNESS_HOURS
            ):
# Menyimpan status kesegaran data pengukuran.
                freshness_status = "CURRENT"

            else:
# Menyimpan status kesegaran data pengukuran.
                freshness_status = "STALE"

                validation_errors.append(
                    "STALE_MEASUREMENT"
                )

        # ----------------------------------------------------
        # Pengayaan record
        # ----------------------------------------------------

        record["measurement_age_hours"] = (
            round(
                measurement_age_hours,
                4,
            )
            if measurement_age_hours is not None
            else None
        )

        record["freshness_status"] = (
            freshness_status
        )

        record["processed_at"] = (
            processed_at.isoformat()
        )

        # ----------------------------------------------------
        # Hasil validasi
        # ----------------------------------------------------

        if validation_errors:

            record["validation_status"] = "INVALID"

            record["validation_error"] = ";".join(
                validation_errors
            )

            LOGGER.warning(
                "OPENAQ_INVALID "
                "location_id=%s "
                "value=%s "
                "status=%s "
                "freshness=%s "
                "errors=%s",
                record.get("location_id"),
                record.get("value"),
                record.get("validation_status"),
                record.get("freshness_status"),
                record.get("validation_error"),
            )

            yield beam.pvalue.TaggedOutput(
                "invalid",
                record,
            )

        else:

            record["validation_status"] = "VALID"

            record["validation_error"] = None

            LOGGER.info(
                "OPENAQ_VALIDATE "
                "location_id=%s "
                "value=%s "
                "status=%s "
                "freshness=%s "
                "age_hours=%.2f",
                record.get("location_id"),
                record.get("value"),
                record.get("validation_status"),
                record.get("freshness_status"),
                record.get(
                    "measurement_age_hours",
                    0,
                ),
            )

            yield record


# ============================================================
# KONVERSI KE JSON
# ============================================================

# Kelas `ToJSON` mengubah record Python menjadi string JSON untuk keluaran pipeline.
class ToJSON(DoFn):

# Fungsi `process()` menjalankan pemrosesan setiap pesan yang diterima oleh Apache Beam.
    def process(self, record):
        yield json.dumps(
            record,
            ensure_ascii=False,
        )


# ============================================================
# JENDELA STREAMING
# ============================================================

# Membagi data streaming ke dalam window waktu tetap untuk diproses secara berkala.
def apply_streaming_window(
    pcollection,
    label,
):

    return (
        pcollection
        | label
        >> beam.WindowInto(
            FixedWindows(60),

            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(
                    30
                )
            ),

            accumulation_mode=(
                trigger.AccumulationMode.DISCARDING
            ),
        )
    )


# ============================================================
# PIPELINE
# ============================================================

# Membangun dan menjalankan pipeline Apache Beam/Dataflow.
def run():
    """
    OpenAQ Pub/Sub
        ↓
    Dataflow
        ↓
    GCS Silver / Audit
    """

    # --------------------------------------------------------
    # DATAFLOW OPTIONS
    # --------------------------------------------------------

# Menyimpan konfigurasi pipeline Dataflow yang digunakan saat eksekusi.
    options = PipelineOptions(
        runner="DataflowRunner",

        project=PROJECT_ID,

        region=REGION,

        temp_location=(
            f"gs://{TEMP_BUCKET}/dataflow/temp"
        ),

        staging_location=(
            f"gs://{TEMP_BUCKET}/dataflow/staging"
        ),

        job_name="openaq-streaming-risk",

        save_main_session=True,
    )

    # --------------------------------------------------------
    # STREAMING MODE
    # --------------------------------------------------------

# Menyimpan opsi standar Apache Beam untuk pipeline.
    standard_options = options.view_as(
        StandardOptions
    )

# Mengaktifkan mode streaming untuk pipeline.
    standard_options.streaming = True

    # --------------------------------------------------------
    # KONFIGURASI AWAL SAAT STARTUP
    # --------------------------------------------------------

    LOGGER.warning(
        "OPENAQ_PIPELINE_START "
        "project=%s "
        "region=%s "
        "runner=DataflowRunner "
        "subscription=%s "
        "silver_output=%s "
        "audit_output=%s",
        PROJECT_ID,
        REGION,
        OPENAQ_SUBSCRIPTION,
        SILVER_OUTPUT,
        AUDIT_OUTPUT,
    )

    # --------------------------------------------------------
    # PIPELINE
    # --------------------------------------------------------

    with beam.Pipeline(
        options=options
    ) as pipeline:

        # ====================================================
        # 1. READ PUB/SUB
        # ====================================================

# Menyimpan pesan yang dibaca dari subscription Pub/Sub.
        messages = (
            pipeline
            | "ReadOpenAQ"
            >> ReadFromPubSub(
                subscription=(
                    OPENAQ_SUBSCRIPTION
                ),
            )
        )

        # ====================================================
        # 2. PARSING JSON
        # ====================================================

# Menyimpan pesan yang sudah diubah dari JSON menjadi dictionary Python.
        parsed = (
            messages
            | "ParseJSON"
            >> beam.ParDo(
                ParseMessageDoFn()
            ).with_outputs(
                "invalid",
                main="valid",
            )
        )

        # ====================================================
        # 3. VALIDATE + ENRICH
        # ====================================================

# Menyimpan record yang sudah melewati proses validasi dan pengayaan.
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

        # ====================================================
        # RECORD SILVER YANG VALID
        # ====================================================

# Menyimpan record valid yang akan ditulis ke layer Silver.
        silver_records = (
            validated.valid
        )

        # ====================================================
        # AUDIT RECORDS
        # ====================================================

# Menyimpan record audit yang dihasilkan setelah proses parsing.
        audit_from_parse = (
            parsed.invalid
        )

# Menyimpan record audit yang dihasilkan setelah proses validasi.
        audit_from_validation = (
            validated.invalid
        )

        # ====================================================
        # 4. MERGE AUDIT STREAMS
        # ====================================================

# Menggabungkan seluruh record audit dari tahap parsing dan validasi.
        audit_records = (
            (
                audit_from_parse,
                audit_from_validation,
            )
            | "MergeAudit"
            >> beam.Flatten()
        )

        # ====================================================
        # 5. SILVER WINDOW
        # ====================================================

# Menyimpan record Silver yang sudah dikelompokkan berdasarkan window waktu.
        silver_windowed = (
            apply_streaming_window(
                silver_records,
                "Silver60SecondWindows",
            )
        )

        # ====================================================
        # 6. AUDIT WINDOW
        # ====================================================

# Menyimpan record audit yang sudah dikelompokkan berdasarkan window waktu.
        audit_windowed = (
            apply_streaming_window(
                audit_records,
                "Audit60SecondWindows",
            )
        )

        # ====================================================
        # 7. WRITE SILVER
        # ====================================================

        (
            silver_windowed
            | "SilverToJSON"
            >> beam.ParDo(
                ToJSON()
            )
            | "WriteSilver"
            >> beam.io.WriteToText(
                SILVER_OUTPUT,
                file_name_suffix=".jsonl",
                num_shards=1,
                append_trailing_newlines=True,
            )
        )

        # ====================================================
        # 8. WRITE AUDIT
        # ====================================================

        (
            audit_windowed
            | "AuditToJSON"
            >> beam.ParDo(
                ToJSON()
            )
            | "WriteAudit"
            >> beam.io.WriteToText(
                AUDIT_OUTPUT,
                file_name_suffix=".jsonl",
                num_shards=1,
                append_trailing_newlines=True,
            )
        )

    # --------------------------------------------------------
    # PIPELINE FINISHED
    # --------------------------------------------------------

    LOGGER.warning(
        "OPENAQ_PIPELINE_FINISHED"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run()