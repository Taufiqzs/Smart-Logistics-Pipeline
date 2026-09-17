from src.beam.risk_pipeline import calculate_risk

def test_risk_score_bounds():
    row = {"severity": "extreme", "pm25": 75, "delay_rate": 1}
    out = calculate_risk(row)
    assert 0 <= out["risk_score"] <= 100
    assert out["risk_band"] == "CRITICAL"

def test_low_risk():
    row = {"severity": "unknown", "pm25": 0, "delay_rate": 0}
    out = calculate_risk(row)
    assert out["risk_score"] == 0
    assert out["risk_band"] == "LOW"
