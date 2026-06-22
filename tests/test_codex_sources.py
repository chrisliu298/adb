import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import adb  # noqa: E402
from adb import _local_codex_session_dirs, _remote_cache_age_hours  # noqa: E402
from parser.parsers import codex as codex_parser  # noqa: E402


TS = "2026-06-01T12:00:00.000Z"


def _write_session(path: Path, session_id: str, *, inp: int, out: int) -> None:
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


def test_local_codex_sources_include_archived_sessions() -> None:
    with tempfile.TemporaryDirectory() as d:
        home = Path(d)
        with patch.object(Path, "home", return_value=home):
            assert _local_codex_session_dirs() == [
                home / ".codex" / "sessions",
                home / ".codex" / "archived_sessions",
            ]


def test_remote_cache_age_ignores_empty_archived_dir() -> None:
    with tempfile.TemporaryDirectory() as d:
        cache = Path(d)
        (cache / "host" / "codex" / "archived_sessions").mkdir(parents=True)
        with patch.object(adb, "REMOTE_CACHE", cache):
            assert _remote_cache_age_hours(["host"]) is None

            archived_file = cache / "host" / "codex" / "archived_sessions" / "x.jsonl"
            archived_file.write_text("{}\n")
            age = _remote_cache_age_hours(["host"])
            assert age is not None and age < 1


def test_codex_parser_default_includes_archived_sibling() -> None:
    with tempfile.TemporaryDirectory() as d:
        home = Path(d)
        sessions = home / ".codex" / "sessions"
        archived = home / ".codex" / "archived_sessions"
        _write_session(sessions / "active.jsonl", "sess-active", inp=100, out=50)
        _write_session(archived / "archived.jsonl", "sess-archived", inp=200, out=70)

        ts = codex_parser.parse(
            sessions_dir=sessions,
            cache_dir=home / ".parser-cache",
        )
        assert ts is not None
        assert ts.total_tokens.total == 420


def test_load_all_local_dedups_data_live_and_archived_sources() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        home = root / "home"
        data = root / "data"
        _write_session(data / "codex" / "local" / "stored.jsonl", "sess-stored", inp=1000, out=1)
        _write_session(data / "codex" / "local" / "dupe.jsonl", "sess-dupe", inp=50, out=50)
        _write_session(home / ".codex" / "sessions" / "dupe.jsonl", "sess-dupe", inp=50, out=50)
        _write_session(home / ".codex" / "archived_sessions" / "archived.jsonl", "sess-archived", inp=200, out=30)

        real_parse = codex_parser.parse

        def parse_with_tmp_cache(**kwargs):
            return real_parse(**kwargs, cache_dir=root / ".parser-cache")

        with (
            patch.object(Path, "home", return_value=home),
            patch.object(adb, "DATA_DIR", data),
            patch.object(adb.claude_parser, "parse", return_value=None),
            patch.object(adb.grok_parser, "parse", return_value=None),
            patch.object(adb.codex_parser, "parse", side_effect=parse_with_tmp_cache),
        ):
            claude, codex, grok, machines = adb.load_all(["local"])

        assert claude is None
        assert grok is None
        assert codex is not None
        assert codex.total_tokens.total == 1331
        assert machines["local"][1] is not None
        assert machines["local"][1].total_sessions == 3


if __name__ == "__main__":
    for _name, _test in sorted(globals().items()):
        if _name.startswith("test_"):
            _test()
    print("PASS: Codex source tests")
