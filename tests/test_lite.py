"""Tests for the `adb lite` view and the helpers it shares with the full dashboard.

Run: uv run pytest tests/test_lite.py

Covers:
  - _compute_recent: the today/yesterday/week/month window derivation that both
    print_stats and print_lite rely on (drift here desyncs the glance from full).
  - fmt_cost_compact: the short-money formatter that keeps lite from wrapping.
  - print_lite: renders without error and surfaces the key fields.
  - claude.parse(fetch_rate_limits=False): skips the OAuth network call (the
    lite view's offline guarantee) while keeping the local tier label.
"""

import sys
import tempfile
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import adb  # noqa: E402
from adb import _compute_recent, fmt_cost_compact, print_lite  # noqa: E402
from parser.parsers import claude as claude_parser  # noqa: E402
from parser.types import DayActivity, TokenBreakdown, ToolStats  # noqa: E402


def _daily(pairs):
    return {d: DayActivity(day=d, output_tokens=tok, messages=tok, sessions=1, tool_calls=tok)
            for d, tok in pairs}


def test_compute_recent_windows():
    # Wed 2026-06-17 → week starts Mon 2026-06-15; prior week 06-08..06-10;
    # prior month (same elapsed days) 05-01..05-17.
    today = date(2026, 6, 17)
    daily = _daily([
        (date(2026, 6, 17), 100),  # today
        (date(2026, 6, 16), 50),   # yesterday (also this week)
        (date(2026, 6, 15), 30),   # Monday (this week)
        (date(2026, 6, 5), 200),   # earlier this month
        (date(2026, 6, 10), 70),   # prior week
        (date(2026, 5, 10), 300),  # prior month
    ])
    total_output = 100 + 50 + 30 + 200 + 70 + 300
    r = _compute_recent(daily, total_cost=float(total_output), total_output_tokens=total_output, today=today)

    assert r.cost_per_token == 1.0
    assert r.today[3] == 100
    assert r.yesterday[3] == 50
    assert r.week[3] == 180, r.week           # 06-15..17: 30 + 50 + 100
    assert r.month[3] == 450, r.month         # all June: 200 + 70 + 30 + 50 + 100
    assert r.pw_otoks == 70
    assert r.pm_otoks == 300
    assert r.n_week_days == 3
    assert r.week_start == date(2026, 6, 15)
    assert r.month_start == date(2026, 6, 1)


def test_compute_recent_empty_is_safe():
    r = _compute_recent({}, total_cost=0.0, total_output_tokens=0, today=date(2026, 6, 17))
    assert r.cost_per_token == 0.0
    assert r.today == (0, 0, 0, 0)
    assert r.pw_otoks == 0 and r.pm_otoks == 0


def test_sparkline_levels():
    from adb import _sparkline
    out = _sparkline([0, 50, 100]).plain
    assert len(out) == 3
    assert out[0] == "▁"   # zero → lowest block
    assert out[2] == "█"   # series max → highest block


def test_fmt_cost_compact():
    assert fmt_cost_compact(74542.92) == "$74.5K"
    assert fmt_cost_compact(10000) == "$10.0K"
    assert fmt_cost_compact(9999) == "$9,999"
    assert fmt_cost_compact(148.32) == "$148"
    assert fmt_cost_compact(1_500_000) == "$1.5M"
    assert fmt_cost_compact(0) == "$0"


def test_print_lite_smoke(monkeypatch):
    # Keep the active-day / streak counts hermetic (no real history files / remotes).
    monkeypatch.setattr(adb, "_history_active_days", lambda p: set())
    monkeypatch.setattr(adb, "_load_remote_hosts", lambda: [])

    today = date.today()
    claude = ToolStats(
        source="claude",
        total_tokens=TokenBreakdown(output_tokens=1000),
        total_sessions=5,
        total_cost=1000.0,
        daily=[DayActivity(day=today, output_tokens=1000, messages=10, sessions=5, tool_calls=20, cost=1000.0)],
        extra={"tier": "max"},
    )
    with adb.console.capture() as cap:
        print_lite(claude, None, None)
    out = cap.get()

    for token in ("adb", "Claude", "TOKENS", "SPEND", "STREAK", "Today", "Week", "Month",
                  "Max", "tok/d", "cache", "out", "active", "/mo", "/sess", "top1%"):
        assert token in out, f"{token!r} missing from lite output:\n{out}"


def test_parse_skips_rate_limit_fetch_when_disabled(monkeypatch):
    # Force creds to look present and stub the network fetch with a sentinel so we
    # can prove the flag — not the absence of creds — gates the call.
    monkeypatch.setattr(claude_parser, "_get_creds", lambda: {"accessToken": "x"})
    monkeypatch.setattr(claude_parser, "_get_tier", lambda cr: "max")
    monkeypatch.setattr(claude_parser, "_fetch_rate_limits", lambda cr: ["SENTINEL"])

    with tempfile.TemporaryDirectory() as d:
        base = Path(d)
        common = dict(
            stats_path=base / "nope.json",
            history_path=base / "nope.jsonl",
            projects_base=[base],
        )
        off = claude_parser.parse(**common, fetch_rate_limits=False)
        on = claude_parser.parse(**common, fetch_rate_limits=True)

    assert off is not None and on is not None
    assert off.rate_limits == [], "lite path must not hit the network"
    assert on.rate_limits == ["SENTINEL"], "default path must still fetch"


if __name__ == "__main__":
    import traceback

    import pytest  # noqa: F401  (allow `python tests/test_lite.py` too)

    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        # Skip monkeypatch-requiring tests in the bare-python runner; use pytest.
        if t.__code__.co_argcount:
            continue
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception:
            failed += 1
            print(f"FAIL {t.__name__}")
            traceback.print_exc()
    print(f"\n{len([t for t in tests if not t.__code__.co_argcount]) - failed} passed (run pytest for the full set)")
    sys.exit(1 if failed else 0)
