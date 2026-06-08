"""Tests for the pure family-rollup arithmetic behind the Models table."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import adb
from adb import _family_summaries
from parser.types import TokenBreakdown


def _tb(inp=0, out=0, cr=0, cw=0):
    return TokenBreakdown(
        input_tokens=inp, output_tokens=out,
        cache_read_tokens=cr, cache_write_tokens=cw,
    )


def _fixed_prices(monkeypatch):
    """Pin _model_prices so the rollup math is independent of real pricing dicts."""
    prices = {
        "claude-opus-4-8": (5.0, 25.0),
        "claude-opus-4-6": (5.0, 25.0),
        "gpt-5.5": (5.0, 30.0),
        "gpt-5.4": (2.5, 15.0),
        "kimi-k2.6": (0.95, 4.0),
        "mystery-model": (None, None),  # unpriced
    }
    monkeypatch.setattr(adb, "_model_prices", lambda n: prices.get(n, (None, None)))


def _sample():
    return {
        "claude-opus-4-8": (_tb(inp=10, out=100, cr=1000, cw=50), 400.0),
        "claude-opus-4-6": (_tb(inp=5, out=40, cr=500, cw=20), 150.0),
        "gpt-5.5": (_tb(inp=20, out=200, cr=300, cw=10), 300.0),
        "gpt-5.4": (_tb(inp=8, out=80, cr=100, cw=5), 30.0),
        "kimi-k2.6": (_tb(inp=4, out=10, cr=30, cw=2), 12.0),
    }


def test_grouping_and_order(monkeypatch):
    _fixed_prices(monkeypatch)
    fams = _family_summaries(_sample())
    # Families sorted by cost desc: Opus (550) > GPT-5 (330) > Others/Kimi (12).
    assert [f.name for f in fams] == ["Claude Opus", "GPT-5", "Others"]
    # Members sorted by cost desc within each family.
    assert [m.name for m in fams[0].members] == ["claude-opus-4-8", "claude-opus-4-6"]
    assert [m.name for m in fams[1].members] == ["gpt-5.5", "gpt-5.4"]


def test_rollups_sum_members(monkeypatch):
    _fixed_prices(monkeypatch)
    fams = _family_summaries(_sample())
    opus = next(f for f in fams if f.name == "Claude Opus")
    assert opus.cost == 550.0
    assert opus.tb.total == _tb(inp=10, out=100, cr=1000, cw=50).total + _tb(inp=5, out=40, cr=500, cw=20).total
    assert opus.tb.input_tokens == 15
    assert opus.tb.output_tokens == 140


def test_grand_totals_preserved(monkeypatch):
    _fixed_prices(monkeypatch)
    merged = _sample()
    fams = _family_summaries(merged)
    # Σ family cost == Σ all model costs.
    assert sum(f.cost for f in fams) == sum(c for _, c in merged.values())
    # Σ family tokens == Σ all model tokens.
    assert sum(f.tb.total for f in fams) == sum(tb.total for tb, _ in merged.values())
    # Grand weighted In/Out == recombination of per-family numerators/denominators,
    # i.e. the family rollups are consistent with the grand Total row.
    flat_in_num = flat_in_den = flat_out_num = flat_out_den = 0.0
    for name, (tb, _) in merged.items():
        in_p, out_p = adb._model_prices(name)
        if in_p is not None:
            t = tb.input_tokens + tb.cache_read_tokens + tb.cache_write_tokens
            flat_in_num += in_p * t
            flat_in_den += t
        if out_p is not None:
            flat_out_num += out_p * tb.output_tokens
            flat_out_den += tb.output_tokens
    assert sum(f.in_num for f in fams) == flat_in_num
    assert sum(f.in_den for f in fams) == flat_in_den
    assert sum(f.out_num for f in fams) == flat_out_num
    assert sum(f.out_den for f in fams) == flat_out_den


def test_unpriced_member_excluded_from_weighting_but_kept_in_totals(monkeypatch):
    _fixed_prices(monkeypatch)
    merged = {
        "kimi-k2.6": (_tb(inp=4, out=10, cr=30, cw=2), 12.0),
        "mystery-model": (_tb(inp=100, out=200, cr=0, cw=0), 0.0),  # unpriced
    }
    fams = _family_summaries(merged)
    others = next(f for f in fams if f.name == "Others")
    # Both models counted in token/cost totals...
    assert others.tb.input_tokens == 104
    assert others.cost == 12.0
    # ...but the unpriced one contributes nothing to the weighted denominators.
    assert others.in_den == 4 + 30 + 2  # only kimi's input-side tokens
    assert others.out_den == 10


def test_deterministic_order_on_cost_tie(monkeypatch):
    _fixed_prices(monkeypatch)
    # Two families with identical (zero) cost and tokens must order reproducibly
    # by name (the tie-break) — "grok" sorts before "others".
    merged = {
        "kimi-k2.6": (_tb(inp=1), 0.0),
        "grok-build": (_tb(inp=1), 0.0),
    }
    assert [f.name for f in _family_summaries(merged)] == ["Grok", "Others"]
    # ...and the reverse insertion order yields the same result.
    merged_rev = {
        "grok-build": (_tb(inp=1), 0.0),
        "kimi-k2.6": (_tb(inp=1), 0.0),
    }
    assert [f.name for f in _family_summaries(merged_rev)] == ["Grok", "Others"]
