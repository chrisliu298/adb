"""Tests for the per-tool-name call-count breakdown (ToolStats.tool_calls_by_name).

Covers the Claude extraction + msg.id dedup, the merge across machines, and the
ToolStats serialization round-trip.
"""

import sys
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adb import _merge_two
from parser.parsers import claude as cp
from parser.types import ToolStats


def _asst(mid: str, *tools: str) -> dict:
    content = [{"type": "tool_use", "name": t} for t in tools]
    return {
        "timestamp": "2026-06-01T10:00:00Z",
        "message": {
            "id": mid,
            "role": "assistant",
            "model": "claude-opus-4-8",
            "stop_reason": "tool_use",
            "usage": {"output_tokens": 10},
            "content": content,
        },
    }


def _write_session(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        for r in records:
            f.write(orjson.dumps(r) + b"\n")


def _clear_cache() -> None:
    for c in (Path(__file__).resolve().parent.parent / ".cache").glob("claude-daily7-*.json"):
        c.unlink()


def test_claude_tool_names_counted(tmp_path):
    base = tmp_path / "projects"
    _write_session(
        base / "proj-a" / "s1.jsonl",
        [_asst("m1", "Bash", "Read"), _asst("m2", "Edit"), _asst("m3", "Bash")],
    )
    _clear_cache()
    _daily, aux = cp._build_daily_from_sessions([base])
    assert aux["tool_calls_by_name"] == {"Bash": 2, "Read": 1, "Edit": 1}


def test_tool_names_deduped_by_msg_id(tmp_path):
    # The same assistant message replayed in a subagent transcript (same msg.id)
    # must count its tools once, mirroring the tool_calls dedup.
    base = tmp_path / "projects"
    _write_session(base / "proj" / "s1.jsonl", [_asst("dup", "Bash", "Grep")])
    _write_session(base / "proj" / "agent-x.jsonl", [_asst("dup", "Bash", "Grep")])
    _clear_cache()
    _daily, aux = cp._build_daily_from_sessions([base])
    assert aux["tool_calls_by_name"] == {"Bash": 1, "Grep": 1}


def test_merge_sums_tool_counts():
    a = ToolStats(source="claude", tool_calls_by_name={"Bash": 3, "Read": 5})
    b = ToolStats(source="codex", tool_calls_by_name={"Bash": 1, "exec_command": 7})
    merged = _merge_two(a, b)
    assert merged.tool_calls_by_name == {"Bash": 4, "Read": 5, "exec_command": 7}


def test_tool_calls_by_name_round_trips():
    ts = ToolStats(source="codex", tool_calls_by_name={"apply_patch": 12, "shell": 4})
    assert ToolStats.from_dict(ts.to_dict()).tool_calls_by_name == {"apply_patch": 12, "shell": 4}
