"""Tests for the per-session cost distribution (ToolStats.session_costs)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adb import _merge_two
from parser.parsers import codex as cx
from parser.types import ToolStats


def test_merge_concatenates_session_costs():
    a = ToolStats(source="claude", session_costs=[1.0, 4.0])
    b = ToolStats(source="codex", session_costs=[2.5])
    assert sorted(_merge_two(a, b).session_costs) == [1.0, 2.5, 4.0]


def test_session_costs_round_trip():
    ts = ToolStats(source="codex", session_costs=[0.125, 9.0])
    # rounds to cents on serialize
    assert ToolStats.from_dict(ts.to_dict()).session_costs == [0.12, 9.0]


def test_codex_session_cost_prices_per_model():
    s = cx._SessionSummary(
        session_id="s", started_at=None, ended_at=None, cwd=None, repo_url=None,
        turns=1, tool_calls=0, user_messages=0, assistant_messages=0,
        tokens=cx._TokenUsage(),
        tokens_by_model={"gpt-5.2": cx._TokenUsage(input_tokens=1_000_000, output_tokens=1_000_000)},
    )
    # gpt-5.2: input $1.75/M, output $14/M (no cached tokens) -> $15.75
    assert round(cx._session_cost(s), 2) == 15.75
