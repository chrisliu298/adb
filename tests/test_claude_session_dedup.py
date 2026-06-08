"""Regression test for the Claude daily/session-count dedup.

A remote host is read as its rsync mirror PLUS its .remote-<host> staging dir,
which hold copies of the same session at the same relative path. The session
count must collapse those copies (keyed on the path relative to the base) while
keeping DISTINCT transcript files separate — subagent stubs share an
`agent-<hash>.jsonl` basename across different parents, so a basename-keyed
dedup would wrongly merge them.

Runnable standalone (`python tests/test_claude_session_dedup.py`) or under pytest.
"""

import sys
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from parser.parsers import claude as cp  # noqa: E402

DAY = "2026-06-01"


def _write(path: Path, sid: str) -> None:
    """One minimal Claude session line carrying a timestamp + sessionId."""
    path.parent.mkdir(parents=True, exist_ok=True)
    rec = {
        "timestamp": f"{DAY}T12:00:00.000Z",
        "sessionId": sid,
        "message": {"role": "user", "content": "hi"},
    }
    path.write_bytes(orjson.dumps(rec) + b"\n")


def _sessions(*dirs: Path) -> int:
    # Force a cold parse each call so we measure the parse, not a warm cache.
    for c in (Path(__file__).resolve().parent.parent / ".cache").glob("claude-daily5-*.json"):
        c.unlink()
    daily, _aux = cp._build_daily_from_sessions(list(dirs))
    return sum(d.sessions for d in daily)


def run(tmp: Path) -> None:
    mirror = tmp / "mirror"
    staging = tmp / ".remote-host"  # dot-prefixed, like a real staging dir

    # One session at the same relative path in BOTH mirror and staging -> once.
    _write(mirror / "proj" / "A.jsonl", "sid-A")
    _write(staging / "proj" / "A.jsonl", "sid-A")
    # Two DISTINCT subagent transcripts sharing a basename but at different
    # relative paths (different parents) -> count twice.
    _write(mirror / "proj" / "p1" / "subagents" / "agent-dup.jsonl", "sid-S1")
    _write(mirror / "proj" / "p2" / "subagents" / "agent-dup.jsonl", "sid-S2")

    n = _sessions(mirror, staging)
    # A (deduped across mirror+staging by relative path) + the two distinct
    # subagent transcripts = 3. A basename-keyed dedup would merge the subagents
    # and report 2.
    assert n == 3, f"expected 3 sessions, got {n}"
    print("PASS: session count dedups by relative path; subagent stubs kept distinct")


def test_claude_session_dedup(tmp_path) -> None:
    run(tmp_path)


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        run(Path(d))
