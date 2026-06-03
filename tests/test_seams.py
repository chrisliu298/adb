"""Regression tests for discovery/pricing seams.

Run: python tests/test_seams.py

Covers:
  - _iter_session_files skips audit.jsonl mirrors (Claude local-agent-mode).
  - Codex _pricing_for prefix fallback picks the LONGEST matching key, so a
    future gpt-5.3-codex-spark-* variant resolves to spark, not plain gpt-5.3.
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from parser.parsers.claude import _iter_session_files  # noqa: E402
from parser.parsers.codex import MODEL_PRICING, _pricing_for  # noqa: E402


def test_iter_session_files_skips_audit():
    with tempfile.TemporaryDirectory() as d:
        proj = Path(d) / "proj"
        proj.mkdir()
        (proj / "abc.jsonl").write_text("{}\n")
        (proj / "audit.jsonl").write_text("{}\n")
        names = {f.name for f in _iter_session_files(proj)}
    assert names == {"abc.jsonl"}, names


def test_pricing_exact_spark():
    assert _pricing_for("gpt-5.3-codex-spark") is MODEL_PRICING["gpt-5.3-codex-spark"]


def test_pricing_longest_prefix():
    # Non-exact suffix exercises the prefix fallback. "gpt-5.3" would match in
    # dict order; the longest-key rule must pick the spark entry instead.
    p = _pricing_for("gpt-5.3-codex-spark-turbo")
    assert p is MODEL_PRICING["gpt-5.3-codex-spark"], "matched gpt-5.3, not spark"


def test_pricing_plain_prefix():
    # gpt-5.4-mini has no dedicated rate yet -> inherits gpt-5.4 via prefix.
    assert _pricing_for("gpt-5.4-mini") is MODEL_PRICING["gpt-5.4"]


def test_pricing_unknown_is_none():
    assert _pricing_for("o4-mini") is None


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
