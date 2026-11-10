"""Tests for the philvolcs DAG's pure helpers: bulletin parser, Gemini model
selection, and structured-output validation. The DAG module is loaded directly
from the dags folder (it is not importable by module name from tests/)."""

import importlib.util
import json
import os

import pytest
import requests as real_requests

SAMPLE_HTML = """
<html><body>
<table><tbody>
<tr><th>Date</th><th>Lat</th><th>Lon</th><th>Depth</th><th>Mag</th><th>Location</th></tr>
</tbody></table>
<table><tbody>
<tr>
  <td class="auto-style91">
    <a href="2026_Earthquake_Information\\November\\2026_1110_190700_B1.html">
      <span>11 November 2026 - 03:07 AM</span></a></td>
  <td align="center"> 12.85 </td>
  <td align="center">123.04</td>
  <td align="center">010</td>
  <td align="center">2.2</td>
  <td>023<span> km S 74&deg; W of Claveria (Masbate)</span></td>
</tr>
<tr>
  <td><a href="2026_Earthquake_Information\\November\\2026_1110_185440_B1.html">11 November 2026 - 02:54 AM</a></td>
  <td>06.02</td><td>125.39</td><td>019</td><td>1.6</td>
  <td>012 km N 65&deg; E of Malapatan (Sarangani)</td>
</tr>
<tr><td colspan="6">not an event row</td></tr>
<tr>
  <td>11 November 2026 - 03:07 AM</td>
  <td>not-a-number</td><td>123.04</td><td>010</td><td>2.2</td>
  <td>bad row</td>
</tr>
</tbody></table>
</body></html>
"""


def _load_dag_module():
    path = os.path.join(os.environ["AIRFLOW_HOME"], "dags", "philvolcs.py")
    spec = importlib.util.spec_from_file_location("philvolcs_dag_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_dag_module()


def test_parse_bulletin_html_extracts_valid_rows_newest_first():
    rows = mod.parse_bulletin_html(SAMPLE_HTML)
    # header row, colspan row and bad-numeric row are skipped
    assert len(rows) == 2
    first = rows[0]
    assert first["event_time_text"] == "11 November 2026 - 03:07 AM"
    assert first["latitude_text"] == "12.85"
    assert first["longitude_text"] == "123.04"
    assert first["depth_text"] == "010"
    assert first["magnitude_text"] == "2.2"
    assert first["location"] == "023 km S 74° W of Claveria (Masbate)"
    assert first["bulletin_url"] == (
        "https://earthquake.phivolcs.dost.gov.ph/"
        "2026_Earthquake_Information/November/2026_1110_190700_B1.html"
    )
    assert rows[1]["event_time_text"] == "11 November 2026 - 02:54 AM"


def test_parse_bulletin_html_returns_empty_on_garbage():
    assert mod.parse_bulletin_html("<html><body>no table here</body></html>") == []


def test_pick_flash_models_filters_and_ranks():
    models = [
        "models/gemini-2.5-flash",
        "models/gemini-3-flash-lite",
        "models/gemini-2.5-flash-image",
        "models/gemini-embedding-001",
        "models/gemma-3-27b",
        "models/gemini-2.0-pro",
    ]
    picked = mod.pick_flash_models(models)
    assert "models/gemini-2.5-flash" in picked
    assert "models/gemini-2.5-flash-image" not in picked
    assert "models/gemini-embedding-001" not in picked
    assert "models/gemma-3-27b" not in picked
    assert "models/gemini-2.0-pro" not in picked
    # non-lite models rank ahead of lite variants
    assert picked.index("models/gemini-2.5-flash") < picked.index(
        "models/gemini-3-flash-lite"
    )


def _good_payload():
    return {
        "headline": "M4.5 quake strikes Balut Island",
        "flash_report_message": (
            "A magnitude 4.5 earthquake struck 062 km S 82° W of Balut Island "
            "at 12:27 AM PST on 11 November 2026. Coordinates5.33°N, 124.87°E, "
            "depth 11 km."
        ),
        "magnitude": 4.5,
        "latitude": 5.33,
        "longitude": 124.87,
        "depth_km": 11,
        "location": "062 km S 82° W of Balut Island (Davao Occidental)",
        "event_time": "2026-11-10T16:27:00+00:00",
        "urgency": "moderate",
    }


def test_validate_flash_report_accepts_good_payload():
    payload = mod.validate_flash_report(_good_payload())
    assert payload["urgency"] == "moderate"


def test_validate_flash_report_rejects_missing_field():
    payload = _good_payload()
    del payload["flash_report_message"]
    with pytest.raises(ValueError, match="missing fields"):
        mod.validate_flash_report(payload)


def test_validate_flash_report_rejects_bad_urgency():
    payload = _good_payload()
    payload["urgency"] = "catastrophic"
    with pytest.raises(ValueError, match="urgency"):
        mod.validate_flash_report(payload)


def test_validate_flash_report_rejects_wrong_type():
    payload = _good_payload()
    payload["magnitude"] = "4.5"
    with pytest.raises(ValueError, match="magnitude"):
        mod.validate_flash_report(payload)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class _FakeClient:
    def __init__(self, rows):
        self._rows = rows
        self.sql = None

    def query(self, sql):
        self.sql = sql
        return _FakeResult(self._rows)


class _BqRow:
    def __init__(self, event_time, latitude, longitude):
        self.event_time = event_time
        self.latitude = latitude
        self.longitude = longitude


def test_already_reported_handles_iso_string_event_time():
    # regression: event_time is ISO-encoded before this helper runs; it used to
    # call .strftime() on the string and crash with AttributeError
    from datetime import datetime, timezone

    event = {
        "event_time": "2026-11-10T20:09:00+00:00",
        "latitude": 12.34,
        "longitude": 123.45,
    }
    match = _BqRow(datetime(2026, 11, 10, 20, 9, tzinfo=timezone.utc), 12.34, 123.45)
    assert mod._already_reported(_FakeClient([match]), event) is True

    other = _BqRow(datetime(2026, 11, 10, 21, 9, tzinfo=timezone.utc), 12.34, 123.45)
    assert mod._already_reported(_FakeClient([other]), event) is False
    assert mod._already_reported(_FakeClient([]), event) is False


class _FakeResp:
    def __init__(self, status, payload=None, text=""):
        self.status_code = status
        self.ok = 200 <= status < 300
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json body")
        return self._payload


def _llm_ok(payload=None):
    payload = payload or _good_payload()
    return _FakeResp(
        200, {"candidates": [{"content": {"parts": [{"text": json.dumps(payload)}]}}]}
    )


class _FakeRequests:
    """requests stand-in: replays scripted outcomes, in order."""

    exceptions = real_requests.exceptions

    def __init__(self, outcomes):
        self._outcomes = list(outcomes)
        self.calls = 0

    def post(self, url, **kwargs):
        out = self._outcomes[self.calls]
        self.calls += 1
        if isinstance(out, Exception):
            raise out
        return out


_HANG = real_requests.exceptions.ReadTimeout("read timed out")


def test_attempt_report_capacity_errors_do_not_burn_tries(monkeypatch):
    # 3 burned tries would exhaust MAX_LLM_ATTEMPTS before reaching the good model
    fake = _FakeRequests(
        [
            _FakeResp(503, text="The model is overloaded"),
            _FakeResp(503, text="The model is overloaded"),
            _FakeResp(503, text="The model is overloaded"),
            _llm_ok(),
        ]
    )
    monkeypatch.setattr(mod, "requests", fake)
    payload = mod._attempt_report("p", ["k1", "k2"], ["models/a", "models/b", "models/c", "models/d"])
    assert payload["_model"] == "models/d"
    assert fake.calls == 4


def test_attempt_report_refunds_first_hang(monkeypatch):
    # without the refund, 3 hangs burn all tries before the healthy model
    fake = _FakeRequests([_HANG, _HANG, _HANG, _llm_ok()])
    monkeypatch.setattr(mod, "requests", fake)
    payload = mod._attempt_report("p", ["k1"], ["models/a", "models/b", "models/c", "models/d"])
    assert payload["_model"] == "models/d"
    assert fake.calls == 4


def test_attempt_report_raises_when_every_model_hangs(monkeypatch):
    fake = _FakeRequests([_HANG, _HANG])
    monkeypatch.setattr(mod, "requests", fake)
    with pytest.raises(RuntimeError, match="timed out"):
        mod._attempt_report("p", ["k1"], ["models/a", "models/b"])


def test_attempt_report_invalid_payload_burns_try_and_rotates(monkeypatch):
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)  # skip micro-backoff
    bad = _FakeResp(200, {"candidates": [{"content": {"parts": [{"text": "not json"}]}}]})
    fake = _FakeRequests([bad, _llm_ok()])
    monkeypatch.setattr(mod, "requests", fake)
    payload = mod._attempt_report("p", ["k1"], ["models/a", "models/b"])
    assert payload["_model"] == "models/b"
    assert fake.calls == 2


def test_scrub_hides_api_key():
    out = mod._scrub(
        "Max retries exceeded with url: /v1beta/models/m:generateContent?key=SECRET123XYZ (Caused by)"
    )
    assert "SECRET123XYZ" not in out
    assert "key=<redacted>" in out
