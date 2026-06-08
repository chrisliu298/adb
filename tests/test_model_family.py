"""Tests for the display-layer model -> family mapping in adb.py."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from adb import _model_family


def test_known_families():
    cases = {
        "claude-opus-4-7": "Claude Opus",
        "claude-opus-4-5-20251101": "Claude Opus",
        "claude-sonnet-4-6": "Claude Sonnet",
        "claude-sonnet-4-5-20250929": "Claude Sonnet",
        "claude-haiku-4-5-20251001": "Claude Haiku",
        "claude-future-tier": "Claude",
        "gpt-5.4": "GPT-5",
        "gpt-5.2-codex": "GPT-5",
        "gpt-5.1-codex-mini": "GPT-5",
        "gpt-5.3-codex-spark": "GPT-5",
        "grok-build": "Grok",
        "grok-composer-2.5-fast": "Grok",
        "GROK-BUILD": "Grok",  # case-insensitive
    }
    for name, family in cases.items():
        assert _model_family(name) == family, name


def test_non_claude_gpt_grok_fold_into_others():
    for name in ("deepseek-v4-pro", "mimo-v2.5-pro", "MiniMax-M3", "kimi-k2.6", "some-future-model-9"):
        assert _model_family(name) == "Others", name


def test_matching_is_anchored_to_vendor_prefix():
    # A stray tier/vendor substring in another vendor's id must NOT mis-family.
    assert _model_family("not-a-sonnet-model") == "Others"
    assert _model_family("deepseek-opus-compatible") == "Others"
    assert _model_family("mygptproxy-model") == "Others"


def test_gpt_label_carries_major_version():
    # The label is derived from the name, so a future major isn't called GPT-5.
    assert _model_family("gpt-5.5") == "GPT-5"
    assert _model_family("gpt-6.1") == "GPT-6"
    assert _model_family("gpt-4o") == "GPT-4"
