"""Regression tests for the Claude loose-token dedup path (parser/parsers/claude.py).

Run: python tests/test_loose_dedup.py

Guards the v3 dedup contract against the v2 bugs:
  - streaming partials were dropped (composite key kept the *first*, smallest
    snapshot instead of the largest);
  - requestId-less subagent replays were never deduped and double-counted.
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import orjson  # noqa: E402

from parser.parsers.claude import (  # noqa: E402
    _aggregate_loose,
    _parse_file_tokens_loose,
)


def _line(mid, rid, model, inp, out, cr=0, cw=0):
    return {
        "timestamp": "2026-06-03T00:00:00Z",
        "requestId": rid,
        "message": {
            "id": mid,
            "role": "assistant",
            "model": model,
            "usage": {
                "input_tokens": inp,
                "output_tokens": out,
                "cache_read_input_tokens": cr,
                "cache_creation_input_tokens": cw,
            },
        },
    }


def _write(path, rows):
    with open(path, "wb") as f:
        for r in rows:
            f.write(orjson.dumps(r) + b"\n")


def test_within_file_keep_max():
    # Streaming snapshots of one message: same mid, growing output. Keep the
    # largest once — not the first (10), not the sum (260).
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "s.jsonl"
        _write(
            p,
            [
                _line("m1", "r1", "claude-opus-4-8", 100, 10),
                _line("m1", "r1", "claude-opus-4-8", 100, 250),
            ],
        )
        entries = _parse_file_tokens_loose(p)
    assert len(entries) == 1, entries
    assert entries[0][3] == 250, entries  # output_tokens
    assert entries[0][2] == 100, entries  # input not summed across partials


def test_cross_file_replay_dedup():
    # Subagent replay: same mid, *different* requestId, across two files. The
    # v2 composite key counted this twice; v3 keys on mid alone -> counted once.
    parent = [["m2", "claude-opus-4-8", 100, 300, 0, 0, 0]]
    replay = [["m2", "claude-opus-4-8", 100, 300, 0, 0, 0]]
    total, models = _aggregate_loose([parent, replay])
    assert total.output_tokens == 300, total.output_tokens  # not 600
    assert total.input_tokens == 100, total.input_tokens
    assert models["claude-opus-4-8"].output_tokens == 300


def test_cross_file_keep_max():
    # Same mid in two files with different output -> keep the larger.
    small = [["m3", "claude-opus-4-8", 100, 40, 0, 0, 0]]
    large = [["m3", "claude-opus-4-8", 100, 900, 0, 0, 0]]
    total, _ = _aggregate_loose([small, large])
    assert total.output_tokens == 900, total.output_tokens


def test_distinct_mids_counted_separately():
    a = [["m4", "claude-opus-4-8", 100, 50, 0, 0, 0]]
    b = [["m5", "claude-opus-4-8", 100, 70, 0, 0, 0]]
    total, _ = _aggregate_loose([a, b])
    assert total.output_tokens == 120, total.output_tokens


def test_no_mid_all_counted():
    # Entries without a msg.id can't be deduped -> all counted distinctly.
    a = [["", "unknown", 5, 5, 0, 0, 0], ["", "unknown", 5, 5, 0, 0, 0]]
    total, _ = _aggregate_loose([a])
    assert total.input_tokens == 10, total.input_tokens
    assert total.output_tokens == 10, total.output_tokens


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
