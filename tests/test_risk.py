# ============================================================
# TEST RISIKO
# ============================================================
# File ini berisi pengujian otomatis untuk memastikan fungsi
# perhitungan risiko menghasilkan skor dan kategori yang sesuai.
# Setiap fungsi test_ akan dijalankan oleh pytest.

from src.beam.risk_pipeline import calculate_risk


# Menguji bahwa skor risiko selalu berada dalam rentang 0 sampai 100
# dan kondisi cuaca ekstrem menghasilkan kategori CRITICAL.
def test_risk_score_bounds():
    row = {
        "severity": "extreme",
        "pm25": 75,
        "delay_rate": 1,
    }

    out = calculate_risk(row)

    assert 0 <= out["risk_score"] <= 100
    assert out["risk_band"] == "CRITICAL"


# Menguji kondisi tanpa risiko cuaca, kualitas udara, dan keterlambatan.
# Hasil yang diharapkan adalah skor 0 dengan kategori LOW.
def test_low_risk():
    row = {
        "severity": "unknown",
        "pm25": 0,
        "delay_rate": 0,
    }

    out = calculate_risk(row)

    assert out["risk_score"] == 0
    assert out["risk_band"] == "LOW"


# Menguji kontribusi risiko cuaca tingkat moderate ketika
# kualitas udara dan keterlambatan tidak memberikan risiko.
def test_moderate_risk():
    row = {
        "severity": "moderate",
        "pm25": 0,
        "delay_rate": 0,
    }

    out = calculate_risk(row)

    assert out["risk_score"] == 22.5
    assert out["risk_band"] == "LOW"


# Menguji kombinasi cuaca severe, PM2.5, dan keterlambatan
# yang seharusnya menghasilkan skor dalam kategori HIGH.
def test_high_risk():
    row = {
        "severity": "severe",
        "pm25": 50,
        "delay_rate": 0.5,
    }

    out = calculate_risk(row)

    assert 50 <= out["risk_score"] < 75
    assert out["risk_band"] == "HIGH"


# Menguji kondisi risiko maksimum dari cuaca, PM2.5,
# dan keterlambatan sehingga menghasilkan skor 100.
def test_critical_risk():
    row = {
        "severity": "extreme",
        "pm25": 75,
        "delay_rate": 1,
    }

    out = calculate_risk(row)

    assert out["risk_score"] == 100
    assert out["risk_band"] == "CRITICAL"


# Menguji fallback ketika data menggunakan nama field
# weather_severity, bukan severity.
def test_weather_severity_fallback():
    row = {
        "weather_severity": "severe",
        "pm25": 0,
        "delay_rate": 0,
    }

    out = calculate_risk(row)

    assert out["risk_score"] == 36.0
    assert out["risk_band"] == "MODERATE"