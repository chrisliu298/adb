"""Regression test for the cumulative-total-decreasing bug.

A Codex session rotated off a remote's live ~/.codex/sessions (and thus pruned
from the rsync mirror) must still be counted from its preserved .remote-<host>
recall-sync staging copy, so the lifetime token total never decreases. Sessions
present in both the mirror and the staging dir must be counted once (deduped by
session_meta.id), not double-counted.

Runnable standalone (`python tests/test_codex_recovery.py`) or under pytest.
"""

import sys
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from parser.parsers import codex as codex_parser  # noqa: E402

TS = "2026-06-01T12:00:00.000Z"


def _write_session(path: Path, session_id: str, *, inp: int, out: int) -> None:
    """Write a minimal valid Codex rollout with one cumulative token snapshot."""
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {"timestamp": TS, "type": "session_meta",
         "payload": {"id": session_id, "cwd": "/tmp/x", "model": "gpt-5.3-codex"}},
        {"timestamp": TS, "type": "turn_context",
         "payload": {"model": "gpt-5.3-codex"}},
        {"timestamp": TS, "type": "event_msg",
         "payload": {"type": "token_count", "info": {"total_token_usage": {
             "input_tokens": inp, "cached_input_tokens": 0, "output_tokens": out,
             "reasoning_output_tokens": 0, "total_tokens": inp + out}}}},
    ]
    path.write_bytes(b"\n".join(orjson.dumps(r) for r in records))


def _total(*dirs: Path) -> int:
    cache_dir = dirs[0].parent / ".codex-parser-cache"
    ts = codex_parser.parse(sessions_dirs=list(dirs), cache_dir=cache_dir)
    return ts.total_tokens.total if ts else 0


def run(tmp: Path) -> None:
    mirror = tmp / "mirror"
    staging = tmp / ".remote-host"  # dot-prefixed, like a real staging dir

    _write_session(mirror / "a.jsonl", "sess-A", inp=1000, out=500)   # in both
    _write_session(staging / "a.jsonl", "sess-A", inp=1000, out=500)
    _write_session(mirror / "b.jsonl", "sess-B", inp=2000, out=800)   # mirror only

    before = _total(mirror, staging)
    # A (1500) deduped across mirror+staging + B (2800) = 4300, not 5800.
    assert before == 4300, f"dedup failed: expected 4300, got {before}"

    # Rotate A off the remote: it's pruned from the mirror but preserved in staging.
    (mirror / "a.jsonl").unlink()

    after = _total(mirror, staging)
    assert after == before, f"total decreased after rotation: {before} -> {after}"
    assert after == 4300, f"rotated session not recovered from staging: {after}"
    print("PASS: cumulative total is monotonic across rotation; dedup holds")


def test_codex_rotation_recovery(tmp_path) -> None:
    run(tmp_path)


def test_codex_archived_sessions_count_and_dedup(tmp_path) -> None:
    live = tmp_path / "sessions"
    archived = tmp_path / "archived_sessions"

    _write_session(live / "active.jsonl", "sess-active", inp=1000, out=500)
    _write_session(live / "dupe.jsonl", "sess-dupe", inp=200, out=100)
    _write_session(archived / "dupe.jsonl", "sess-dupe", inp=200, out=100)
    _write_session(archived / "archived.jsonl", "sess-archived", inp=3000, out=700)

    total = _total(live, archived)
    # active (1500) + duplicate once (300) + archived-only (3700)
    assert total == 5500, f"expected archived sessions to count once, got {total}"


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        run(tmp)
        test_codex_archived_sessions_count_and_dedup(tmp / "archived-case")
