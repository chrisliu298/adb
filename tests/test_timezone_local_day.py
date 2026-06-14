"""Day/hour bucketing must use LOCAL time, not the raw UTC timestamp.

Claude/Codex/Grok all persist UTC timestamps (ISO 'Z' or epoch-ms). The
dashboard groups activity by the viewer's local calendar day (matching Codex
and Grok, and Claude's own heatmap). These tests pin the process timezone to
US/Pacific so a UTC instant in the early-morning UTC hours falls on the
PREVIOUS local day — the case a naive UTC-date prefix gets wrong.
"""

import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import orjson
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import adb
from parser.parsers import claude as cp


@pytest.fixture
def pacific_tz():
    prev = os.environ.get("TZ")
    os.environ["TZ"] = "America/Los_Angeles"
    time.tzset()
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = prev
        time.tzset()


# 2026-06-10 04:30 UTC == 2026-06-09 21:30 PDT (UTC-7): local day is the 9th.
UTC_TS = "2026-06-10T04:30:00.000Z"
LOCAL_DAY = date(2026, 6, 9)
UTC_DAY = date(2026, 6, 10)
EPOCH_MS = int(datetime(2026, 6, 10, 4, 30, tzinfo=timezone.utc).timestamp() * 1000)


def _clear():
    for c in (Path(__file__).resolve().parent.parent / ".cache").glob("claude-daily*-*.json"):
        c.unlink()


def test_local_dt_converts_iso_z_to_local_day(pacific_tz):
    assert cp._local_dt(UTC_TS).date() == LOCAL_DAY
    assert cp._local_dt(UTC_TS).date() != UTC_DAY


def test_local_dt_converts_epoch_ms_to_local_day(pacific_tz):
    assert cp._local_dt(EPOCH_MS).date() == LOCAL_DAY


def test_parse_date_converts_timestamp_to_local(pacific_tz):
    # firstSessionDate is a full UTC timestamp -> local day.
    assert cp._parse_date(UTC_TS) == LOCAL_DAY
    # a bare local date string passes through untouched (no spurious shift).
    assert cp._parse_date("2026-06-10") == UTC_DAY


def test_session_daily_bucketed_in_local_time(pacific_tz, tmp_path):
    base = tmp_path / "projects"
    proj = base / "p"
    proj.mkdir(parents=True)
    rec = {
        "timestamp": UTC_TS,
        "message": {
            "id": "m1", "role": "assistant", "model": "claude-opus-4-8",
            "stop_reason": "end_turn", "usage": {"output_tokens": 5},
            "content": [{"type": "tool_use", "name": "Bash"}],
        },
    }
    with (proj / "s.jsonl").open("wb") as f:
        f.write(orjson.dumps(rec) + b"\n")

    _clear()
    daily, aux = cp._build_daily_from_sessions([base])
    days = {d.day for d in daily}
    assert LOCAL_DAY in days
    assert UTC_DAY not in days
    assert aux["model_first_seen"] == {"claude-opus-4-8": LOCAL_DAY.isoformat()}


def test_history_active_days_uses_local_day(pacific_tz, tmp_path):
    hist = tmp_path / "history.jsonl"
    with hist.open("wb") as f:
        f.write(orjson.dumps({"timestamp": EPOCH_MS}) + b"\n")
    assert adb._history_active_days(hist) == {LOCAL_DAY}
