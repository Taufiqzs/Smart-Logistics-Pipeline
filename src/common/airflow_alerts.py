"""
Modul notifikasi kegagalan Airflow.

Modul ini digunakan untuk mengirim email alert ketika
task pada pipeline Smart Logistics mengalami kegagalan.

Metode pengiriman:
    Gmail SMTP melalui STARTTLS pada port 587.

Credential SMTP diambil dari environment variable:
    AIRFLOW__SMTP__SMTP_HOST
    AIRFLOW__SMTP__SMTP_PORT
    AIRFLOW__SMTP__SMTP_USER
    AIRFLOW__SMTP__SMTP_PASSWORD
    AIRFLOW_ALERT_EMAIL
"""

import os
import smtplib

from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape


# ============================================================
# KONFIGURASI PIPELINE
# ============================================================

# Nama pipeline yang ditampilkan pada email.
PIPELINE_NAME = (
    "Smart Logistics Weather-Air Quality Risk Pipeline"
)

# Nama sistem yang digunakan pada subject email.
SYSTEM_NAME = "Smart Logistics"


# ============================================================
# FUNGSI UTAMA EMAIL ALERT
# ============================================================

def notify_pipeline_failure(context):
    """
    Mengirim email notifikasi ketika task Airflow gagal.

    Fungsi ini digunakan sebagai:
        on_failure_callback

    Parameter:
        context:
            Dictionary context yang diberikan oleh Airflow
            ketika task mengalami kegagalan.

    Informasi yang diambil:
        - DAG ID
        - Task ID
        - Run ID
        - Logical date
        - Start date
        - Exception
        - URL log Airflow

    Email dikirim melalui:
        Gmail SMTP + STARTTLS.
    """

    # ========================================================
    # MENGAMBIL INFORMASI DAG
    # ========================================================

    # Mengambil objek DAG dari context Airflow.
    dag = context.get("dag")

    # Mengambil ID DAG.
    dag_id = (
        dag.dag_id
        if dag
        else "unknown"
    )


    # ========================================================
    # MENGAMBIL INFORMASI TASK
    # ========================================================

    # Mengambil TaskInstance dari context.
    task_instance = context.get(
        "task_instance"
    )

    # Mengambil ID task yang mengalami kegagalan.
    task_id = (
        task_instance.task_id
        if task_instance
        else "unknown"
    )


    # ========================================================
    # MENGAMBIL INFORMASI RUN
    # ========================================================

    # Mengambil ID eksekusi DAG.
    run_id = context.get(
        "run_id",
        "unknown",
    )

    # Mengambil logical date.
    logical_date = context.get(
        "logical_date"
    )

    # Mengambil objek DagRun.
    dag_run = context.get(
        "dag_run"
    )

    # Mengambil waktu mulai DAG run.
    run_start_date = (
        dag_run.start_date
        if dag_run
        else None
    )

    # Airflow pada beberapa manual run dapat memberikan
    # logical_date=None.
    #
    # Karena itu kita menggunakan beberapa fallback.
    execution_time = (
        logical_date
        or run_start_date
        or datetime.now(timezone.utc)
    )

    # Mengubah waktu menjadi format ISO 8601.
    execution_time_text = (
        execution_time.isoformat()
    )


    # ========================================================
    # MENGAMBIL INFORMASI ERROR
    # ========================================================

    # Mengambil exception yang menyebabkan task gagal.
    exception = context.get(
        "exception"
    )

    # Mengubah exception menjadi teks.
    error_message = (
        str(exception)
        if exception
        else "Tidak ada detail error yang tersedia."
    )


    # ========================================================
    # MENGAMBIL URL LOG AIRFLOW
    # ========================================================

    # Mengambil URL log task jika tersedia.
    log_url = (
        task_instance.log_url
        if task_instance
        else None
    )

    # Jika URL tidak tersedia, gunakan teks alternatif.
    log_url = (
        log_url
        if log_url
        else "#"
    )


    # ========================================================
    # MENGAMBIL KONFIGURASI SMTP
    # ========================================================

    # Host SMTP Gmail.
    smtp_host = os.getenv(
        "AIRFLOW__SMTP__SMTP_HOST",
        "smtp.gmail.com",
    )

    # Port SMTP Gmail.
    smtp_port = int(
        os.getenv(
            "AIRFLOW__SMTP__SMTP_PORT",
            "587",
        )
    )

    # Username Gmail.
    smtp_user = os.getenv(
        "AIRFLOW__SMTP__SMTP_USER"
    )

    # Gmail App Password.
    smtp_password = os.getenv(
        "AIRFLOW__SMTP__SMTP_PASSWORD"
    )

    # Alamat email penerima alert.
    recipient = os.getenv(
        "AIRFLOW_ALERT_EMAIL"
    )


    # ========================================================
    # VALIDASI KONFIGURASI
    # ========================================================

    # Pastikan SMTP username tersedia.
    if not smtp_user:
        print(
            "Email alert gagal: "
            "AIRFLOW__SMTP__SMTP_USER belum dikonfigurasi."
        )
        return

    # Pastikan SMTP password tersedia.
    if not smtp_password:
        print(
            "Email alert gagal: "
            "AIRFLOW__SMTP__SMTP_PASSWORD belum dikonfigurasi."
        )
        return

    # Pastikan alamat penerima tersedia.
    if not recipient:
        print(
            "Email alert gagal: "
            "AIRFLOW_ALERT_EMAIL belum dikonfigurasi."
        )
        return


    # ========================================================
    # ESCAPE HTML
    # ========================================================

    # Escape data dinamis agar karakter khusus pada
    # error message tidak merusak struktur HTML email.
    safe_dag_id = escape(
        str(dag_id)
    )

    safe_task_id = escape(
        str(task_id)
    )

    safe_run_id = escape(
        str(run_id)
    )

    safe_execution_time = escape(
        execution_time_text
    )

    safe_error_message = escape(
        error_message
    )

    safe_log_url = escape(
        str(log_url),
        quote=True,
    )


    # ========================================================
    # SUBJECT EMAIL
    # ========================================================

    # Subject dibuat singkat tetapi informatif.
    subject = (
        f"[CRITICAL] {SYSTEM_NAME} - "
        f"Task Failure: {task_id}"
    )


    # ========================================================
    # HTML EMAIL
    # ========================================================

    html_content = f"""
<!DOCTYPE html>

<html lang="en">

<head>

    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width,
        initial-scale=1.0"
    >

    <title>
        Smart Logistics Pipeline Alert
    </title>

</head>


<body
    style="
        margin:0;
        padding:0;
        background-color:#f4f6f8;
        font-family:
            Arial,
            Helvetica,
            sans-serif;
        color:#1f2937;
    "
>

    <table
        width="100%"
        cellpadding="0"
        cellspacing="0"
        border="0"
        style="
            background-color:#f4f6f8;
            padding:30px 15px;
        "
    >

        <tr>

            <td align="center">

                <table
                    width="680"
                    cellpadding="0"
                    cellspacing="0"
                    border="0"
                    style="
                        max-width:680px;
                        width:100%;
                        background-color:#ffffff;
                        border-radius:10px;
                        overflow:hidden;
                        box-shadow:
                            0 2px 8px
                            rgba(0,0,0,0.08);
                    "
                >

                    <!-- HEADER -->

                    <tr>

                        <td
                            style="
                                background-color:#111827;
                                padding:24px 30px;
                            "
                        >

                            <div
                                style="
                                    font-size:13px;
                                    color:#9ca3af;
                                    font-weight:bold;
                                    letter-spacing:1px;
                                "
                            >
                                SMART LOGISTICS
                            </div>

                            <div
                                style="
                                    margin-top:8px;
                                    font-size:24px;
                                    line-height:32px;
                                    color:#ffffff;
                                    font-weight:bold;
                                "
                            >
                                Pipeline Failure Alert
                            </div>

                        </td>

                    </tr>


                    <!-- STATUS -->

                    <tr>

                        <td
                            style="
                                padding:25px 30px 10px 30px;
                            "
                        >

                            <table
                                width="100%"
                                cellpadding="0"
                                cellspacing="0"
                                border="0"
                            >

                                <tr>

                                    <td
                                        style="
                                            vertical-align:top;
                                        "
                                    >

                                        <div
                                            style="
                                                font-size:13px;
                                                color:#6b7280;
                                                margin-bottom:6px;
                                            "
                                        >
                                            STATUS
                                        </div>

                                        <div
                                            style="
                                                display:inline-block;
                                                background-color:#fee2e2;
                                                color:#b91c1c;
                                                padding:7px 13px;
                                                border-radius:6px;
                                                font-size:13px;
                                                font-weight:bold;
                                            "
                                        >
                                            CRITICAL
                                        </div>

                                    </td>

                                </tr>

                            </table>

                        </td>

                    </tr>


                    <!-- MESSAGE -->

                    <tr>

                        <td
                            style="
                                padding:10px 30px 20px 30px;
                            "
                        >

                            <p
                                style="
                                    margin:0;
                                    font-size:15px;
                                    line-height:24px;
                                    color:#374151;
                                "
                            >
                                A task in the
                                <strong>
                                    {escape(PIPELINE_NAME)}
                                </strong>
                                has failed and requires
                                investigation.
                            </p>

                        </td>

                    </tr>


                    <!-- DETAILS -->

                    <tr>

                        <td
                            style="
                                padding:0 30px 25px 30px;
                            "
                        >

                            <table
                                width="100%"
                                cellpadding="0"
                                cellspacing="0"
                                border="0"
                                style="
                                    border:1px solid #e5e7eb;
                                    border-radius:7px;
                                    overflow:hidden;
                                "
                            >

                                <tr>

                                    <td
                                        width="180"
                                        style="
                                            padding:12px 15px;
                                            background-color:#f9fafb;
                                            border-bottom:
                                                1px solid #e5e7eb;
                                            font-size:13px;
                                            font-weight:bold;
                                            color:#4b5563;
                                        "
                                    >
                                        DAG
                                    </td>

                                    <td
                                        style="
                                            padding:12px 15px;
                                            border-bottom:
                                                1px solid #e5e7eb;
                                            font-size:13px;
                                            color:#111827;
                                        "
                                    >
                                        {safe_dag_id}
                                    </td>

                                </tr>


                                <tr>

                                    <td
                                        style="
                                            padding:12px 15px;
                                            background-color:#f9fafb;
                                            border-bottom:
                                                1px solid #e5e7eb;
                                            font-size:13px;
                                            font-weight:bold;
                                            color:#4b5563;
                                        "
                                    >
                                        Failed Task
                                    </td>

                                    <td
                                        style="
                                            padding:12px 15px;
                                            border-bottom:
                                                1px solid #e5e7eb;
                                            font-size:13px;
                                            color:#111827;
                                            font-weight:bold;
                                        "
                                    >
                                        {safe_task_id}
                                    </td>

                                </tr>


                                <tr>

                                    <td
                                        style="
                                            padding:12px 15px;
                                            background-color:#f9fafb;
                                            border-bottom:
                                                1px solid #e5e7eb;
                                            font-size:13px;
                                            font-weight:bold;
                                            color:#4b5563;
                                        "
                                    >
                                        Run ID
                                    </td>

                                    <td
                                        style="
                                            padding:12px 15px;
                                            border-bottom:
                                                1px solid #e5e7eb;
                                            font-size:13px;
                                            color:#111827;
                                            word-break:break-all;
                                        "
                                    >
                                        {safe_run_id}
                                    </td>

                                </tr>


                                <tr>

                                    <td
                                        style="
                                            padding:12px 15px;
                                            background-color:#f9fafb;
                                            border-bottom:
                                                1px solid #e5e7eb;
                                            font-size:13px;
                                            font-weight:bold;
                                            color:#4b5563;
                                        "
                                    >
                                        Execution Time
                                    </td>

                                    <td
                                        style="
                                            padding:12px 15px;
                                            border-bottom:
                                                1px solid #e5e7eb;
                                            font-size:13px;
                                            color:#111827;
                                        "
                                    >
                                        {safe_execution_time}
                                    </td>

                                </tr>


                                <tr>

                                    <td
                                        style="
                                            padding:12px 15px;
                                            background-color:#f9fafb;
                                            font-size:13px;
                                            font-weight:bold;
                                            color:#4b5563;
                                        "
                                    >
                                        Alert Generated
                                    </td>

                                    <td
                                        style="
                                            padding:12px 15px;
                                            font-size:13px;
                                            color:#111827;
                                        "
                                    >
                                        {escape(
                                            datetime.now(
                                                timezone.utc
                                            ).isoformat()
                                        )}
                                    </td>

                                </tr>

                            </table>

                        </td>

                    </tr>


                    <!-- ERROR -->

                    <tr>

                        <td
                            style="
                                padding:0 30px 25px 30px;
                            "
                        >

                            <div
                                style="
                                    font-size:13px;
                                    color:#6b7280;
                                    font-weight:bold;
                                    margin-bottom:8px;
                                "
                            >
                                ERROR DETAILS
                            </div>

                            <div
                                style="
                                    background-color:#fff7ed;
                                    border-left:
                                        4px solid #f97316;
                                    padding:15px;
                                    border-radius:5px;
                                "
                            >

                                <pre
                                    style="
                                        margin:0;
                                        white-space:pre-wrap;
                                        word-break:break-word;
                                        font-family:
                                            Consolas,
                                            Monaco,
                                            monospace;
                                        font-size:13px;
                                        line-height:20px;
                                        color:#7c2d12;
                                    "
                                >{safe_error_message}</pre>

                            </div>

                        </td>

                    </tr>


                    <!-- ACTION -->

                    <tr>

                        <td
                            align="center"
                            style="
                                padding:5px 30px 30px 30px;
                            "
                        >

                            <a
                                href="{safe_log_url}"
                                style="
                                    display:inline-block;
                                    background-color:#2563eb;
                                    color:#ffffff;
                                    text-decoration:none;
                                    font-size:14px;
                                    font-weight:bold;
                                    padding:12px 22px;
                                    border-radius:6px;
                                "
                            >
                                Open Airflow Task Log
                            </a>

                        </td>

                    </tr>


                    <!-- FOOTER -->

                    <tr>

                        <td
                            style="
                                background-color:#f9fafb;
                                border-top:
                                    1px solid #e5e7eb;
                                padding:18px 30px;
                            "
                        >

                            <div
                                style="
                                    font-size:12px;
                                    line-height:19px;
                                    color:#6b7280;
                                "
                            >

                                This is an automated notification
                                generated by Apache Airflow.

                                <br>

                                Pipeline:
                                <strong>
                                    {escape(PIPELINE_NAME)}
                                </strong>

                                <br>

                                Please investigate the failed task
                                and review the Airflow task log.

                            </div>

                        </td>

                    </tr>

                </table>

            </td>

        </tr>

    </table>

</body>

</html>
"""


    # ========================================================
    # PLAIN TEXT FALLBACK
    # ========================================================

    # Menyediakan versi plain text untuk email client
    # yang tidak mendukung HTML.
    plain_text = f"""
SMART LOGISTICS - PIPELINE FAILURE ALERT

STATUS: CRITICAL

Pipeline:
{PIPELINE_NAME}

DAG:
{dag_id}

Failed Task:
{task_id}

Run ID:
{run_id}

Execution Time:
{execution_time_text}

Error:
{error_message}

Airflow Task Log:
{log_url}

This is an automated notification generated
by Apache Airflow.
"""


    # ========================================================
    # MEMBUAT EMAIL MESSAGE
    # ========================================================

    # Membuat multipart email agar mendukung:
    # - plain text
    # - HTML
    message = MIMEMultipart(
        "alternative"
    )

    # Subject email.
    message["Subject"] = subject

    # Pengirim.
    message["From"] = smtp_user

    # Penerima.
    message["To"] = recipient

    # Menambahkan plain text.
    message.attach(
        MIMEText(
            plain_text,
            "plain",
            "utf-8",
        )
    )

    # Menambahkan HTML.
    message.attach(
        MIMEText(
            html_content,
            "html",
            "utf-8",
        )
    )


    # ========================================================
    # MENGIRIM EMAIL
    # ========================================================

    try:

        # Membuka koneksi SMTP Gmail.
        with smtplib.SMTP(
            smtp_host,
            smtp_port,
            timeout=30,
        ) as server:

            # Mengaktifkan STARTTLS.
            server.starttls()

            # Login menggunakan Gmail App Password.
            server.login(
                smtp_user,
                smtp_password,
            )

            # Mengirim email.
            server.send_message(
                message
            )

        # Log keberhasilan pengiriman.
        print(
            "Email alert berhasil dikirim ke "
            f"{recipient}"
        )

    except Exception as email_error:

        # Error email tidak boleh menutupi
        # error task utama.
        print(
            "Gagal mengirim email alert: "
            f"{email_error}"
        )