"""Per-project rollup (ToolStats.projects) for Codex.

Project cost must be CUMULATIVE across every session sharing a repo/cwd, matching
the Claude parser — not the cost of the single most-recent session.
"""

import sys
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from parser.parsers import codex as cx


def _write_session(path: Path, sid: str, cwd: str, day: str, *, inp: int, out: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"timestamp": f"{day}T00:00:00Z", "type": "session_meta",
         "payload": {"id": sid, "cwd": cwd}},
        {"timestamp": f"{day}T00:00:01Z", "type": "turn_context",
         "payload": {"model": "gpt-5.2"}},
        {"timestamp": f"{day}T00:00:02Z", "type": "event_msg",
         "payload": {"type": "token_count", "info": {"total_token_usage": {
             "input_tokens": inp, "cached_input_tokens": 0,
             "output_tokens": out, "total_tokens": inp + out}}}},
    ]
    path.write_bytes(b"\n".join(orjson.dumps(r) for r in rows))


def test_codex_project_cost_is_cumulative_across_sessions(tmp_path):
    # Two sessions in the SAME cwd. gpt-5.2: input $1.75/M, output $14/M.
    sess = tmp_path / "sessions" / "2026" / "06"
    # earlier session: 1M in + 1M out -> $1.75 + $14.00 = $15.75
    _write_session(sess / "07" / "rollout-a.jsonl", "sid-a", "/tmp/proj", "2026-06-07", inp=1_000_000, out=1_000_000)
    # later (most-recent) session: 2M in + 0 out -> $3.50
    _write_session(sess / "08" / "rollout-b.jsonl", "sid-b", "/tmp/proj", "2026-06-08", inp=2_000_000, out=0)

    ts = cx.parse(sessions_dir=tmp_path / "sessions", cache_dir=tmp_path / ".cache")
    assert ts is not None

    projs = [p for p in ts.projects if p.path.endswith("/tmp/proj") or p.path == "/tmp/proj"]
    assert len(projs) == 1, f"expected one row for the shared cwd, got {ts.projects}"
    # CUMULATIVE: $15.75 + $3.50 = $19.25, not the recent-only $3.50.
    assert round(projs[0].cost, 2) == 19.25
    # Tokens summed across both sessions too: input 3M, output 1M.
    assert projs[0].input_tokens == 3_000_000
    assert projs[0].output_tokens == 1_000_000


def test_codex_returns_all_projects_no_pre_merge_truncation(tmp_path):
    # The parser must NOT truncate to top-N: the dashboard does the cross-machine
    # merge + top-N, so a per-bucket cap would drop the long tail before the merge.
    sess = tmp_path / "sessions" / "2026" / "06" / "08"
    for i in range(15):
        _write_session(sess / f"rollout-{i}.jsonl", f"sid-{i}", f"/tmp/proj-{i}",
                       "2026-06-08", inp=1_000_000, out=10_000 * (i + 1))

    ts = cx.parse(sessions_dir=tmp_path / "sessions", cache_dir=tmp_path / ".cache")
    assert ts is not None
    # All 15 cost-bearing projects returned, not capped at 10.
    assert len(ts.projects) == 15
