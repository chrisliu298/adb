"""Tests for the heatmap, stop-reason, and model-first-seen aux signals."""

import sys
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adb import _merge_two
from parser.parsers import claude as cp
from parser.types import ToolStats, _pad_heatmap


def _asst(mid, ts, model, stop_reason, *tools):
    content = [{"type": "tool_use", "name": t} for t in tools]
    return {
        "timestamp": ts,
        "message": {
            "id": mid, "role": "assistant", "model": model,
            "stop_reason": stop_reason, "usage": {"output_tokens": 5}, "content": content,
        },
    }


def _write(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        for r in records:
            f.write(orjson.dumps(r) + b"\n")


def _clear():
    for c in (Path(__file__).resolve().parent.parent / ".cache").glob("claude-daily6-*.json"):
        c.unlink()


def test_claude_aux_heatmap_stops_models(tmp_path):
    base = tmp_path / "projects"
    _write(base / "p" / "s.jsonl", [
        {"timestamp": "2026-06-01T10:00:00Z", "message": {"role": "user", "content": "hi"}},
        _asst("m1", "2026-06-01T10:00:05Z", "claude-opus-4-8", "end_turn", "Bash"),
        _asst("m2", "2026-05-20T09:00:00Z", "claude-opus-4-8", "max_tokens"),
    ])
    _clear()
    _daily, aux = cp._build_daily_from_sessions([base])
    # 3 messages (1 user + 2 assistant) each land in exactly one heatmap cell.
    assert sum(aux["heatmap"]) == 3
    assert len(aux["heatmap"]) == 168
    assert aux["stop_reasons"] == {"end_turn": 1, "max_tokens": 1}
    # earliest day wins for the model first-seen.
    assert aux["model_first_seen"] == {"claude-opus-4-8": "2026-05-20"}


def test_merge_heatmap_elementwise():
    a = ToolStats(source="claude", heatmap=[1] * 168)
    b = ToolStats(source="codex", heatmap=[2] * 168)
    assert _merge_two(a, b).heatmap == [3] * 168


def test_merge_stop_reasons_summed_and_models_min():
    a = ToolStats(source="claude", stop_reasons={"end_turn": 3},
                  model_first_seen={"opus": "2026-03-01", "gpt": "2026-04-01"})
    b = ToolStats(source="codex", stop_reasons={"end_turn": 2, "max_tokens": 1},
                  model_first_seen={"opus": "2026-02-15"})
    m = _merge_two(a, b)
    assert m.stop_reasons == {"end_turn": 5, "max_tokens": 1}
    assert m.model_first_seen == {"opus": "2026-02-15", "gpt": "2026-04-01"}


def test_round_trip_and_pad():
    ts = ToolStats(source="claude", heatmap=[7] * 168,
                   stop_reasons={"refusal": 2}, model_first_seen={"opus": "2026-01-02"})
    back = ToolStats.from_dict(ts.to_dict())
    assert back.heatmap == [7] * 168
    assert back.stop_reasons == {"refusal": 2}
    assert back.model_first_seen == {"opus": "2026-01-02"}
    # a short/garbled cached heatmap is normalized to 168 ints
    assert _pad_heatmap([1, 2, 3]) == [1, 2, 3] + [0] * 165
    assert len(_pad_heatmap("bad")) == 168
