"""Monotonic 'floor' guard — the lifetime per-tool token total must never decrease.

adb records each clean full run's per-tool token totals to a ledger in the
DURABLE store (`data/.meta/`, not the disposable `.cache/`, so a routine cache
clear cannot silently disarm the guard). If a later full run computes a LOWER
total for a tool (data loss, a parser/cache bug, a corrupted store), the displayed
lifetime holds at the recorded high-water and a loud banner flags the drop — the
number is never silently shown lower. Only an explicit `--rebaseline` accepts a
lower value. Fail-closed: an existing-but-unreadable ledger is never overwritten
and degrades the guard loudly rather than re-arming at a possibly-degraded total.
"""

from __future__ import annotations

from pathlib import Path

import orjson

FLOOR_PATH = Path(__file__).resolve().parent.parent / "data" / ".meta" / "adb-floor.json"

# Sentinel tool name signalling an unreadable ledger (handled specially by the UI).
UNREADABLE = "__floor_unreadable__"


def _load() -> dict[str, int] | None:
    """Return the stored per-tool floor, `{}` if none exists yet, or `None` if a
    ledger file EXISTS but cannot be read (fail-closed: callers must not lower it)."""
    if not FLOOR_PATH.exists():
        return {}
    try:
        return {k: int(v) for k, v in orjson.loads(FLOOR_PATH.read_bytes()).get("tokens", {}).items()}
    except Exception:
        return None


def _save(per_tool: dict[str, int]) -> None:
    try:
        FLOOR_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = FLOOR_PATH.with_name(FLOOR_PATH.name + ".tmp")
        tmp.write_bytes(orjson.dumps({"tokens": per_tool}))
        tmp.replace(FLOOR_PATH)  # atomic
    except OSError:
        pass


def apply(
    computed: dict[str, int], *, rebaseline: bool = False
) -> tuple[dict[str, int], list[tuple[str, int, int]]]:
    """Compare computed per-tool totals to the stored floor and ratchet it.

    Returns (effective per-tool totals, regressions) where each regression is
    (tool, floor_value, computed_value). On a drop the effective value holds at
    the floor and the floor is not lowered; otherwise the floor ratchets up.
    `rebaseline=True` accepts the computed values as-is (the only way to lower).
    """
    floor = _load()
    if floor is None:  # ledger exists but is unreadable -> don't touch it, alert
        return dict(computed), [(UNREADABLE, 0, 0)]

    effective: dict[str, int] = {}
    new_floor: dict[str, int] = dict(floor)  # preserve tools not in this run
    regressions: list[tuple[str, int, int]] = []
    for tool, val in computed.items():
        fv = floor.get(tool, 0)
        if not rebaseline and val < fv:
            regressions.append((tool, fv, val))
            effective[tool] = fv
            new_floor[tool] = fv
        else:
            effective[tool] = val
            new_floor[tool] = val
    for tool, fv in floor.items():
        effective.setdefault(tool, fv)

    if rebaseline:
        _save(new_floor)  # may lower
    else:
        # Re-read before saving so a concurrent run's higher high-water is never
        # clobbered (read-modify-write race); keep the max per tool.
        latest = _load() or {}
        merged = {k: max(int(latest.get(k, 0)), new_floor.get(k, 0)) for k in set(latest) | set(new_floor)}
        _save(merged)
    return effective, regressions
