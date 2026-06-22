"""Regression test for Codex session-id dedup (parser/parsers/codex.py).

Run: python tests/test_codex_dedup.py

A rollout copied into more than one place (e.g. task-synth saves a copy into its
task dir while the original lives in the shadow CODEX_HOME) shares one
session_meta.id. The parser has no cross-file token dedup, so it must collapse
duplicates to one file (the largest) before parsing, or the session counts twice.
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import orjson  # noqa: E402

from parser.parsers.codex import (  # noqa: E402
    _dedup_files_by_session,
    _session_id_of,
)


def _rollout(path, sid, n_pad, tokens=None):
    rows = [{"type": "session_meta", "timestamp": "2026-06-03T00:00:00Z",
             "payload": {"id": sid, "cwd": "/x"}}]
    if tokens is not None:
        rows.append({"type": "event_msg", "timestamp": "2026-06-03T00:00:00Z",
                     "payload": {"type": "token_count",
                                 "info": {"total_token_usage": {"total_tokens": tokens}}}})
    # padding lines so files of the "same" session differ in size
    for _ in range(n_pad):
        rows.append({"type": "response_item", "timestamp": "2026-06-03T00:00:01Z",
                     "payload": {"type": "message", "role": "assistant", "pad": "x" * 50}})
    with open(path, "wb") as f:
        for r in rows:
            f.write(orjson.dumps(r) + b"\n")


def _write_rows(path, rows):
    with open(path, "wb") as f:
        for r in rows:
            f.write(orjson.dumps(r) + b"\n")


def test_session_id_read():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "codex_session.jsonl"
        _rollout(p, "sid-abc", 1)
        assert _session_id_of(p) == "sid-abc"


def test_dedup_keeps_largest_per_session():
    with tempfile.TemporaryDirectory() as d:
        original = Path(d) / "shadow.jsonl"
        copy_small = Path(d) / "task.jsonl"
        _rollout(original, "dup-1", 20)   # larger (more complete)
        _rollout(copy_small, "dup-1", 2)  # smaller copy, same session id
        kept = _dedup_files_by_session([original, copy_small])
    assert kept == [original], kept  # one file, the larger one


def test_dedup_keeps_token_richest():
    # The byte-LARGER copy has FEWER tokens; dedup must keep the token-richer one,
    # not the byte-largest (byte size is not a token-count guarantee).
    with tempfile.TemporaryDirectory() as d:
        padded = Path(d) / "padded.jsonl"   # byte-larger, token-poorer
        rich = Path(d) / "rich.jsonl"       # byte-smaller, token-richer
        _rollout(padded, "dup-2", n_pad=200, tokens=100)
        _rollout(rich, "dup-2", n_pad=1, tokens=9000)
        assert padded.stat().st_size > rich.stat().st_size  # sanity
        kept = _dedup_files_by_session([padded, rich])
    assert kept == [rich], kept


def test_dedup_tie_keeps_diagnostic_richest_before_byte_size():
    with tempfile.TemporaryDirectory() as d:
        padded = Path(d) / "padded.jsonl"
        diagnostic = Path(d) / "diagnostic.jsonl"
        base = [
            {"type": "session_meta", "timestamp": "2026-06-03T00:00:00Z",
             "payload": {"id": "dup-diag", "cwd": "/x"}},
            {"type": "event_msg", "timestamp": "2026-06-03T00:00:00Z",
             "payload": {"type": "token_count",
                         "info": {"total_token_usage": {"total_tokens": 100}}}},
        ]
        _write_rows(
            padded,
            base
            + [{"type": "event_msg", "timestamp": "2026-06-03T00:00:01Z",
                "payload": {"type": "notice", "pad": "x" * 2000}}],
        )
        _write_rows(
            diagnostic,
            base
            + [{"type": "response_item", "timestamp": "2026-06-03T00:00:01Z",
                "payload": {"type": "message", "role": "assistant"}}],
        )
        assert padded.stat().st_size > diagnostic.stat().st_size
        kept = _dedup_files_by_session([padded, diagnostic])
    assert kept == [diagnostic], kept


def test_distinct_sessions_all_kept():
    with tempfile.TemporaryDirectory() as d:
        a = Path(d) / "a.jsonl"
        b = Path(d) / "b.jsonl"
        _rollout(a, "s-a", 1)
        _rollout(b, "s-b", 1)
        kept = _dedup_files_by_session([a, b])
    assert set(kept) == {a, b}, kept


def test_unkeyed_files_all_kept():
    with tempfile.TemporaryDirectory() as d:
        a = Path(d) / "a.jsonl"
        b = Path(d) / "b.jsonl"
        a.write_text('{"type":"response_item"}\n')  # no session_meta
        b.write_text('{"type":"response_item"}\n')
        kept = _dedup_files_by_session([a, b])
    assert set(kept) == {a, b}, kept


if __name__ == "__main__":
    import traceback

    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception:
            failed += 1
            print(f"FAIL {t.__name__}")
            traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
