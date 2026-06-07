"""Regression test for the floor guard (parser/floor.py).

The lifetime per-tool token total must never be displayed below its recorded
high-water: a clean run ratchets the floor up, a drop holds at the high-water and
reports the regression (never lowers the floor), and --rebaseline accepts a lower
value on demand.

Run: python tests/test_floor.py
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from parser import floor  # noqa: E402


def test_floor_ratchets_holds_and_rebaselines():
    with tempfile.TemporaryDirectory() as d:
        floor.FLOOR_PATH = Path(d) / "floor.json"

        # First run arms the floor.
        eff, reg = floor.apply({"claude": 100, "codex": 50, "grok": 1})
        assert reg == [] and eff == {"claude": 100, "codex": 50, "grok": 1}, (eff, reg)

        # A higher run ratchets the floor up.
        eff, reg = floor.apply({"claude": 120, "codex": 50, "grok": 1})
        assert reg == [] and eff["claude"] == 120, (eff, reg)

        # A DROP holds the displayed value at the high-water and reports it.
        eff, reg = floor.apply({"claude": 90, "codex": 50, "grok": 1})
        assert eff["claude"] == 120, eff                # held, not 90
        assert reg == [("claude", 120, 90)], reg

        # The floor was NOT ratcheted down — the next real value still sees 120.
        eff, reg = floor.apply({"claude": 121, "codex": 50, "grok": 1})
        assert eff["claude"] == 121 and reg == [], (eff, reg)

        # --rebaseline accepts a lower value (intentional correction).
        eff, reg = floor.apply({"claude": 80, "codex": 50, "grok": 1}, rebaseline=True)
        assert eff["claude"] == 80 and reg == [], (eff, reg)
    print("PASS: floor ratchets up, holds on a drop, and rebaselines on demand")


if __name__ == "__main__":
    test_floor_ratchets_holds_and_rebaselines()
