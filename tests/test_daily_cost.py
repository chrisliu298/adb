"""Tests for per-day cost attribution (DayActivity.cost).

Covers the dataclass merge, the multi-machine ToolStats merge, the to_dict/
from_dict round-trip, and Codex parser reconciliation (sum of per-day cost
equals the priced total_cost).
"""

import sys
from datetime import date
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adb import _merge_two
from parser.parsers import codex as cx
from parser.types import DayActivity, ToolStats


def test_day_activity_add_sums_cost():
    a = DayActivity(day=date(2026, 1, 1), output_tokens=100, cost=1.5)
    b = DayActivity(day=date(2026, 1, 1), output_tokens=50, cost=0.25)
    a.add(b)
    assert a.output_tokens == 150
    assert a.cost == 1.75


def test_merge_two_sums_same_day_cost():
    a = ToolStats(source="claude", daily=[DayActivity(day=date(2026, 1, 1), cost=2.0)])
    b = ToolStats(source="codex", daily=[DayActivity(day=date(2026, 1, 1), cost=3.0)])
    merged = _merge_two(a, b)
    assert len(merged.daily) == 1
    assert merged.daily[0].cost == 5.0


def test_day_activity_cost_round_trips():
    ts = ToolStats(
        source="codex",
        daily=[DayActivity(day=date(2026, 1, 2), output_tokens=10, cost=4.25)],
    )
    rt = ToolStats.from_dict(ts.to_dict())
    assert rt.daily[0].cost == 4.25


def _codex_session(path: Path, model: str, snapshots: list[int]) -> None:
    """A minimal Codex rollout: a turn_context (model) + cumulative token_count
    snapshots whose output_tokens climb by `snapshots`."""
    rows = [
        {"type": "session_meta", "timestamp": "2026-06-03T00:00:00Z",
         "payload": {"id": path.stem, "cwd": "/x"}},
        {"type": "turn_context", "timestamp": "2026-06-03T00:00:00Z",
         "payload": {"model": model}},
    ]
    cum = 0
    for i, out in enumerate(snapshots):
        cum += out
        rows.append({
            "type": "event_msg", "timestamp": f"2026-06-03T00:0{i}:30Z",
            "payload": {"type": "token_count", "info": {"total_token_usage": {
                "input_tokens": cum * 2, "output_tokens": cum,
                "total_tokens": cum * 3}}},
        })
    with path.open("wb") as f:
        for r in rows:
            f.write(orjson.dumps(r) + b"\n")


def test_codex_daily_cost_reconciles_to_total(tmp_path):
    sessions = tmp_path / "sessions" / "2026" / "06" / "03"
    sessions.mkdir(parents=True)
    _codex_session(sessions / "s1.jsonl", "gpt-5.4", [1000, 2000, 1500])
    ts = cx.parse(sessions_dirs=[tmp_path / "sessions"])
    assert ts is not None
    assert ts.total_cost > 0
    daily_cost = sum(d.cost for d in ts.daily)
    # Per-delta sum vs the aggregate model pricing differ only by float ordering.
    assert abs(daily_cost - ts.total_cost) < 1e-9
