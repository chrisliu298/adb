import sys
import tempfile
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import orjson
from rich.console import Console

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import adb  # noqa: E402
from adb import _local_codex_session_dirs, _remote_cache_age_hours  # noqa: E402
from parser.parsers import codex as codex_parser  # noqa: E402
from parser.types import ToolStats  # noqa: E402


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


def _write_records(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\n".join(orjson.dumps(r) for r in records))


def _base_session(session_id: str) -> list[dict]:
    return [
        {"timestamp": "2026-06-01T12:00:00.000Z", "type": "session_meta",
         "payload": {"id": session_id, "cwd": "/tmp/x", "model": "gpt-5.4"}},
        {"timestamp": "2026-06-01T12:00:00.000Z", "type": "turn_context",
         "payload": {"model": "gpt-5.4"}},
    ]


def _token_count(ts: str, *, inp: int, out: int) -> dict:
    return {"timestamp": ts, "type": "event_msg",
            "payload": {"type": "token_count", "info": {"total_token_usage": {
                "input_tokens": inp, "cached_input_tokens": 0,
                "output_tokens": out, "reasoning_output_tokens": 0,
                "total_tokens": inp + out}}}}


def _message(ts: str, role: str) -> dict:
    return {"timestamp": ts, "type": "response_item",
            "payload": {"type": "message", "role": role,
                        "content": [{"type": "output_text", "text": "ok"}]}}


def _tool_call(ts: str, *, item_type: str = "function_call") -> dict:
    return {"timestamp": ts, "type": "response_item",
            "payload": {"type": item_type, "name": "exec_command"}}


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


def test_codex_parser_flags_assistant_output_without_token_snapshot() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        sessions = root / "sessions"
        _write_records(
            sessions / "unmetered.jsonl",
            _base_session("sess-unmetered")
            + [_message("2026-06-01T12:00:01.000Z", "assistant")],
        )

        ts = codex_parser.parse(sessions_dir=sessions, cache_dir=root / ".parser-cache")

        assert ts is not None
        assert ts.total_tokens.total == 0
        assert ts.extra["missing_token_sessions"] == 1
        assert ts.extra["missing_token_no_snapshot_sessions"] == 1
        assert ts.extra["missing_token_after_last_snapshot_sessions"] == 0
        assert ts.extra["missing_token_assistant_messages"] == 1
        assert ts.extra["missing_token_user_messages"] == 0
        assert ts.extra["missing_token_tool_calls"] == 0
        assert ts.extra["missing_token_no_token_count_sessions"] == 1
        assert ts.extra["missing_token_token_count_without_usage_sessions"] == 0
        assert ts.extra["missing_token_by_model"] == {"gpt-5.4": 1}
        assert ts.extra["missing_token_by_month"] == {"2026-06": 1}


def test_codex_parser_does_not_flag_assistant_output_after_final_token_snapshot() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        sessions = root / "sessions"
        _write_records(
            sessions / "late-output.jsonl",
            _base_session("sess-late-output")
            + [
                _token_count("2026-06-01T12:00:01.000Z", inp=100, out=20),
                _message("2026-06-01T12:00:02.000Z", "assistant"),
            ],
        )

        ts = codex_parser.parse(sessions_dir=sessions, cache_dir=root / ".parser-cache")

        assert ts is not None
        assert ts.total_tokens.total == 120
        assert ts.extra["missing_token_sessions"] == 0
        assert ts.extra["missing_token_by_model"] == {}


def test_codex_parser_flags_new_user_message_after_final_token_snapshot() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        sessions = root / "sessions"
        _write_records(
            sessions / "late-user.jsonl",
            _base_session("sess-late-user")
            + [
                _token_count("2026-06-01T12:00:01.000Z", inp=100, out=20),
                _message("2026-06-01T12:00:01.000Z", "user"),
                _message("2026-06-01T12:00:02.000Z", "assistant"),
            ],
        )

        ts = codex_parser.parse(sessions_dir=sessions, cache_dir=root / ".parser-cache")

        assert ts is not None
        assert ts.total_tokens.total == 120
        assert ts.extra["missing_token_sessions"] == 1
        assert ts.extra["missing_token_no_snapshot_sessions"] == 0
        assert ts.extra["missing_token_after_last_snapshot_sessions"] == 1
        assert ts.extra["missing_token_assistant_messages"] == 1
        assert ts.extra["missing_token_user_messages"] == 1
        assert ts.extra["missing_token_tool_calls"] == 0
        assert ts.extra["missing_token_by_model"] == {"gpt-5.4": 1}
        assert ts.extra["missing_token_by_month"] == {"2026-06": 1}


def test_codex_parser_does_not_flag_response_items_before_final_token_snapshot() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        sessions = root / "sessions"
        _write_records(
            sessions / "covered-output.jsonl",
            _base_session("sess-covered-output")
            + [
                _message("2026-06-01T12:00:01.000Z", "assistant"),
                _token_count("2026-06-01T12:00:02.000Z", inp=100, out=20),
            ],
        )

        ts = codex_parser.parse(sessions_dir=sessions, cache_dir=root / ".parser-cache")

        assert ts is not None
        assert ts.total_tokens.total == 120
        assert ts.extra["missing_token_sessions"] == 0
        assert ts.extra["missing_token_by_model"] == {}


def test_codex_parser_flags_user_only_no_token_session() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        sessions = root / "sessions"
        _write_records(
            sessions / "user-only.jsonl",
            _base_session("sess-user-only")
            + [_message("2026-06-01T12:00:01.000Z", "user")],
        )

        ts = codex_parser.parse(sessions_dir=sessions, cache_dir=root / ".parser-cache")

        assert ts is not None
        assert ts.extra["missing_token_sessions"] == 1
        assert ts.extra["missing_token_no_snapshot_sessions"] == 1
        assert ts.extra["missing_token_user_messages"] == 1
        assert ts.extra["missing_token_assistant_messages"] == 0
        assert ts.extra["missing_token_no_token_count_sessions"] == 1
        assert ts.extra["missing_token_by_model"] == {"gpt-5.4": 1}


def test_codex_parser_does_not_flag_turn_context_only_no_token_session() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        sessions = root / "sessions"
        _write_records(
            sessions / "turn-context-only.jsonl",
            _base_session("sess-turn-context-only"),
        )

        ts = codex_parser.parse(sessions_dir=sessions, cache_dir=root / ".parser-cache")

        assert ts is not None
        assert ts.extra["missing_token_sessions"] == 0
        assert ts.extra["missing_token_by_model"] == {}


def test_codex_parser_splits_no_snapshot_token_count_without_usage() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        sessions = root / "sessions"
        _write_records(
            sessions / "rate-limit-only.jsonl",
            _base_session("sess-rate-limit-only")
            + [{"timestamp": "2026-06-01T12:00:01.000Z", "type": "event_msg",
                "payload": {"type": "token_count", "info": None,
                            "rate_limits": {"primary": {"used_percent": 1.0}}}}],
        )

        ts = codex_parser.parse(sessions_dir=sessions, cache_dir=root / ".parser-cache")

        assert ts is not None
        assert ts.extra["missing_token_sessions"] == 1
        assert ts.extra["missing_token_no_snapshot_sessions"] == 1
        assert ts.extra["missing_token_token_count_without_usage_sessions"] == 1
        assert ts.extra["missing_token_no_token_count_sessions"] == 0


def test_codex_parser_flags_tool_call_without_token_snapshot() -> None:
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        sessions = root / "sessions"
        _write_records(
            sessions / "tool-only.jsonl",
            _base_session("sess-tool-only")
            + [_tool_call("2026-06-01T12:00:01.000Z", item_type="custom_tool_call")],
        )

        ts = codex_parser.parse(sessions_dir=sessions, cache_dir=root / ".parser-cache")

        assert ts is not None
        assert ts.extra["missing_token_sessions"] == 1
        assert ts.extra["missing_token_no_snapshot_sessions"] == 1
        assert ts.extra["missing_token_tool_calls"] == 1
        assert ts.extra["missing_token_by_model"] == {"gpt-5.4": 1}


def test_merge_two_sums_codex_missing_token_diagnostics() -> None:
    left = ToolStats(
        source="codex",
        extra={
            "tier": "",
            "missing_token_sessions": 1,
            "missing_token_no_snapshot_sessions": 1,
            "missing_token_after_last_snapshot_sessions": 0,
            "missing_token_token_count_without_usage_sessions": 1,
            "missing_token_no_token_count_sessions": 0,
            "missing_token_by_model": {"gpt-5.4": 1},
            "missing_token_by_month": {"2026-05": 1},
        },
    )
    right = ToolStats(
        source="codex",
        extra={
            "tier": "pro",
            "missing_token_sessions": 2,
            "missing_token_no_snapshot_sessions": 0,
            "missing_token_after_last_snapshot_sessions": 2,
            "missing_token_token_count_without_usage_sessions": 0,
            "missing_token_no_token_count_sessions": 2,
            "missing_token_by_model": {"gpt-5.4": 1, "gpt-5.5": 1},
            "missing_token_by_month": {"2026-05": 1, "2026-06": 1},
        },
    )

    merged = adb._merge_two(left, right)

    assert merged.extra["tier"] == "pro"
    assert merged.extra["missing_token_sessions"] == 3
    assert merged.extra["missing_token_no_snapshot_sessions"] == 1
    assert merged.extra["missing_token_after_last_snapshot_sessions"] == 2
    assert merged.extra["missing_token_token_count_without_usage_sessions"] == 1
    assert merged.extra["missing_token_no_token_count_sessions"] == 2
    assert merged.extra["missing_token_by_model"] == {"gpt-5.4": 2, "gpt-5.5": 1}
    assert merged.extra["missing_token_by_month"] == {"2026-05": 2, "2026-06": 1}


def test_print_stats_shows_codex_missing_warning_without_models() -> None:
    codex = ToolStats(
        source="codex",
        total_sessions=1,
        extra={
            "missing_token_sessions": 1,
            "missing_token_no_snapshot_sessions": 1,
            "missing_token_after_last_snapshot_sessions": 0,
            "missing_token_token_count_without_usage_sessions": 1,
            "missing_token_no_token_count_sessions": 0,
            "missing_token_user_messages": 1,
            "missing_token_developer_messages": 1,
            "missing_token_assistant_messages": 0,
            "missing_token_tool_calls": 0,
            "missing_token_by_model": {"gpt-5.4": 1},
            "missing_token_by_month": {"2026-06": 1},
        },
    )
    out = StringIO()
    test_console = Console(file=out, force_terminal=False, width=160, color_system=None)

    with patch.object(adb, "console", test_console):
        adb.print_stats(None, codex, None, {"local": (None, codex, None)})

    text = out.getvalue()
    assert "Unmetered Codex: 1 sessions" in text
    assert "2 input msgs" in text


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
