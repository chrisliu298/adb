"""Parse Grok Build CLI stats from ~/.grok/sessions/**.

The Grok Build CLI stores one directory per session under
``~/.grok/sessions/<url-encoded-cwd>/<session-uuid>/``. Unlike Claude and
Codex, it does NOT persist a per-message token breakdown: there is no
input/output split and no cache-read accounting anywhere on disk. The only
token quantity recorded is a per-session *context-window size* snapshot:

- ``signals.json -> contextTokensUsed`` (the canonical number), or
- ``max(updates.jsonl _meta.totalTokens)`` as a fallback when signals.json is
  absent (a few active sessions never flush signals.json).

Sessions are single-turn with no compaction, so that snapshot is effectively
the whole token footprint of the session. We treat it as context/input tokens
(there is nothing finer to split it into) and price it per model.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import orjson

from parser.types import (
    CostBreakdown,
    DayActivity,
    ProjectInfo,
    TokenBreakdown,
    ToolStats,
)

SESSIONS_DIR = Path(os.environ.get("GROK_HOME", Path.home() / ".grok")) / "sessions"


@dataclass(frozen=True, slots=True)
class ModelPricing:
    input_usd_per_mtok: float
    cached_input_usd_per_mtok: float | None
    output_usd_per_mtok: float
    # Long-context tier: xAI charges a higher rate once a request's context
    # exceeds the threshold. None means flat pricing.
    long_ctx_threshold: int | None = None
    input_usd_per_mtok_long: float | None = None


# grok-build-0.1: xAI official pricing (docs.x.ai/developers/pricing). Tiered:
# <=200K context at $1/$0.20/$2, >200K at $2/$0.40/$4.
# grok-composer-2.5-fast: xAI publishes no per-token rate for the Grok CLI
# route; this is the Cursor Composer 2.5 "fast" tier as a NOTIONAL proxy, not a
# billed rate. These constants are the single place to adjust cost.
MODEL_PRICING: dict[str, ModelPricing] = {
    "grok-build": ModelPricing(
        input_usd_per_mtok=1.0,
        cached_input_usd_per_mtok=0.2,
        output_usd_per_mtok=2.0,
        long_ctx_threshold=200_000,
        input_usd_per_mtok_long=2.0,
    ),
    "grok-composer-2.5-fast": ModelPricing(
        input_usd_per_mtok=3.0,
        cached_input_usd_per_mtok=None,
        output_usd_per_mtok=15.0,
    ),
}


def _pricing_for(model: str) -> ModelPricing | None:
    return MODEL_PRICING.get(str(model or "").strip())


def _session_cost(model: str, context_tokens: int) -> float:
    """Notional cost for one session: context tokens priced at the model's
    full input rate (the most defensible single-number estimate when the data
    has no input/output/cache split). Applies the long-context tier per
    session, since billing tiers are per request, not per total."""
    pricing = _pricing_for(model)
    if pricing is None:
        return 0.0
    rate = pricing.input_usd_per_mtok
    if (
        pricing.long_ctx_threshold is not None
        and pricing.input_usd_per_mtok_long is not None
        and context_tokens > pricing.long_ctx_threshold
    ):
        rate = pricing.input_usd_per_mtok_long
    return context_tokens * rate / 1e6


@dataclass(slots=True)
class _Session:
    model: str
    context_tokens: int
    messages: int
    tool_calls: int
    turns: int
    started_at: datetime | None
    ended_at: datetime | None
    project_key: str | None


@dataclass(slots=True)
class _Aggregates:
    earliest: datetime | None = None
    sessions: list[_Session] = field(default_factory=list)
    tokens_by_model: dict[str, int] = field(default_factory=dict)
    messages_by_hour: dict[int, int] = field(
        default_factory=lambda: {h: 0 for h in range(24)}
    )
    daily: dict[date, DayActivity] = field(default_factory=dict)
    heatmap: list = field(default_factory=lambda: [0] * 168)  # weekday*24+hour, local
    model_first_seen: dict = field(default_factory=dict)      # model -> earliest ISO day


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None


def _local_day(dt: datetime) -> date:
    return dt.astimezone().date()


def _load_json(path: Path) -> dict | None:
    try:
        return orjson.loads(path.read_bytes())
    except (OSError, orjson.JSONDecodeError):
        return None


def _max_updates_tokens(path: Path) -> int:
    """Fallback token source: the peak ``_meta.totalTokens`` seen in
    updates.jsonl. Used only when signals.json is missing."""
    mx = 0
    try:
        with path.open("rb") as f:
            for line in f:
                if b"totalTokens" not in line:
                    continue
                try:
                    obj = orjson.loads(line)
                except orjson.JSONDecodeError:
                    continue
                meta = (obj.get("params") or {}).get("_meta") or {}
                tt = meta.get("totalTokens")
                if isinstance(tt, int) and tt > mx:
                    mx = tt
    except OSError:
        return mx
    return mx


def _parse_session_dir(d: Path, agg: _Aggregates) -> None:
    summary = _load_json(d / "summary.json")
    if summary is None:
        return
    info = summary.get("info") or {}
    signals = _load_json(d / "signals.json")

    # Tokens: prefer signals.contextTokensUsed; fall back to the peak
    # totalTokens in updates.jsonl for sessions that never flushed signals.
    if signals is not None:
        context_tokens = int(signals.get("contextTokensUsed", 0) or 0)
    else:
        context_tokens = _max_updates_tokens(d / "updates.jsonl")

    if signals is not None:
        messages = int(signals.get("userMessageCount", 0) or 0) + int(
            signals.get("assistantMessageCount", 0) or 0
        )
        tool_calls = int(signals.get("toolCallCount", 0) or 0)
        turns = int(signals.get("turnCount", 0) or 0)
        model = signals.get("primaryModelId") or summary.get("current_model_id")
    else:
        messages = int(summary.get("num_chat_messages", 0) or 0)
        tool_calls = 0
        turns = 1 if context_tokens else 0
        model = summary.get("current_model_id")

    # Skip sessions with no activity at all (started but never used).
    if context_tokens <= 0 and messages <= 0:
        return

    model = str(model or "unknown")
    started = _parse_ts(summary.get("created_at"))
    ended = _parse_ts(summary.get("updated_at")) or started

    remotes = summary.get("git_remotes") or []
    project_key = (
        (remotes[0] if remotes else None)
        or summary.get("git_root_dir")
        or info.get("cwd")
    )

    agg.sessions.append(
        _Session(
            model=model,
            context_tokens=context_tokens,
            messages=messages,
            tool_calls=tool_calls,
            turns=turns,
            started_at=started,
            ended_at=ended,
            project_key=project_key,
        )
    )

    agg.tokens_by_model[model] = agg.tokens_by_model.get(model, 0) + context_tokens

    if started is not None:
        if agg.earliest is None or started < agg.earliest:
            agg.earliest = started
        day = _local_day(started)
        da = agg.daily.setdefault(day, DayActivity(day=day))
        da.messages += messages
        da.sessions += 1
        da.tool_calls += tool_calls
        da.cost += _session_cost(model, context_tokens)
        lt = started.astimezone()
        agg.messages_by_hour[lt.hour] += messages
        agg.heatmap[lt.weekday() * 24 + lt.hour] += messages
        mday = day.isoformat()
        prev = agg.model_first_seen.get(model)
        if prev is None or mday < prev:
            agg.model_first_seen[model] = mday


def _iter_session_dirs(sessions_dir: Path) -> list[Path]:
    """Session dirs live two levels deep: sessions/<enc-cwd>/<uuid>/. Skip
    dot-prefixed parts so sync mirrors (.remote-<host>) aren't double-counted."""
    dirs: list[Path] = []
    for summary in sessions_dir.glob("*/*/summary.json"):
        rel = summary.relative_to(sessions_dir)
        if any(part.startswith(".") for part in rel.parts):
            continue
        dirs.append(summary.parent)
    return sorted(dirs)


def _dir_fingerprint(dirs: list[Path]) -> str:
    """Fast fingerprint over the files the parser reads (summary/signals/
    updates), so warm runs hit the cache."""
    total_size = 0
    max_mtime = 0
    count = 0
    for d in dirs:
        for name in ("summary.json", "signals.json", "updates.jsonl"):
            try:
                st = (d / name).stat()
            except OSError:
                continue
            count += 1
            total_size += st.st_size
            if st.st_mtime_ns > max_mtime:
                max_mtime = st.st_mtime_ns
    return f"{len(dirs)}:{count}:{total_size}:{max_mtime}"


def _load_cache(cache_path: Path) -> dict | None:
    if cache_path.exists():
        try:
            return orjson.loads(cache_path.read_bytes())
        except Exception:
            return None
    return None


def _save_cache(cache_path: Path, fingerprint: str, ts: ToolStats) -> None:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(orjson.dumps({"fp": fingerprint, "data": ts.to_dict()}))
    except OSError:
        pass


def _build_projects(agg: _Aggregates) -> list[ProjectInfo]:
    by_key: dict[str, ProjectInfo] = {}
    for s in agg.sessions:
        key = s.project_key or "unknown"
        cost = _session_cost(s.model, s.context_tokens)
        dur_ms = 0
        if s.started_at and s.ended_at:
            dur_ms = int((s.ended_at - s.started_at).total_seconds() * 1000)
        if key in by_key:
            by_key[key].cost += cost
            by_key[key].input_tokens += s.context_tokens
            by_key[key].duration_ms += dur_ms
        else:
            shown = key.replace(str(Path.home()), "~")
            by_key[key] = ProjectInfo(
                path=shown,
                cost=cost,
                input_tokens=s.context_tokens,
                output_tokens=0,
                duration_ms=dur_ms,
            )

    rows = [p for p in by_key.values() if p.cost > 0]
    rows.sort(key=lambda p: p.cost, reverse=True)
    return rows[:10]


def parse(
    *,
    sessions_dir: Path = SESSIONS_DIR,
    sessions_dirs: list[Path] | None = None,
) -> ToolStats | None:
    """Parse Grok Build CLI session logs. Returns None if no data available.

    sessions_dirs: optional list of bases read together (e.g. the durable store
    bucket plus the live home for freshness). A session is a self-contained
    `<enc-cwd>/<uuid>` dir, so copies across bases are collapsed by that relative
    path (first base wins) — Grok has no cross-file token replay to dedup further.
    """
    bases = sessions_dirs if sessions_dirs is not None else [sessions_dir]
    bases = [b for b in bases if b.exists()]
    if not bases:
        return None
    seen: set[str] = set()
    dirs: list[Path] = []
    for b in bases:
        for d in _iter_session_dirs(b):
            rel = str(d.relative_to(b))
            if rel in seen:
                continue
            seen.add(rel)
            dirs.append(d)
    if not dirs:
        return None

    import hashlib

    # RESOLVED, NUL-joined bases; v2 tag bumped on accounting changes (see codex).
    base_hash = hashlib.md5("\x00".join(str(b.resolve()) for b in bases).encode()).hexdigest()[:12]
    cache_dir = Path(__file__).resolve().parent.parent.parent / ".cache"
    cache_path = cache_dir / f"grok-sessions-v5-{base_hash}.json"
    fp = _dir_fingerprint(dirs)
    cached = _load_cache(cache_path)
    if cached and cached.get("fp") == fp:
        try:
            return ToolStats.from_dict(cached["data"])
        except Exception:
            pass

    agg = _Aggregates()
    for d in dirs:
        _parse_session_dir(d, agg)
    if not agg.sessions:
        return None

    # Per-model token totals map to the input bucket; the data has no
    # output/cache split to populate the other buckets.
    models: dict[str, TokenBreakdown] = {
        model: TokenBreakdown(input_tokens=tokens)
        for model, tokens in agg.tokens_by_model.items()
    }

    # Cost is summed per session so the long-context tier applies per request,
    # not against the per-model total. cost_breakdown mirrors it (input bucket).
    model_costs: dict[str, float] = {m: 0.0 for m in models}
    cb = CostBreakdown()
    unpriced_models: set[str] = set()
    unpriced_tokens = 0
    for s in agg.sessions:
        if _pricing_for(s.model) is None:
            unpriced_models.add(s.model)
            unpriced_tokens += s.context_tokens
            continue
        cost = _session_cost(s.model, s.context_tokens)
        model_costs[s.model] += cost
        cb.input_tokens += s.context_tokens
        cb.input_cost += cost

    total_tokens = TokenBreakdown(
        input_tokens=sum(s.context_tokens for s in agg.sessions)
    )
    total_cost = sum(model_costs.values())

    daily = sorted(agg.daily.values(), key=lambda d: d.day)
    first_date = _local_day(agg.earliest) if agg.earliest else None

    longest_dur_ms = 0
    longest_msgs = 0
    for s in agg.sessions:
        if s.started_at and s.ended_at:
            dur = int((s.ended_at - s.started_at).total_seconds() * 1000)
            if dur > longest_dur_ms:
                longest_dur_ms = dur
                longest_msgs = s.messages

    result = ToolStats(
        source="grok",
        total_tokens=total_tokens,
        total_sessions=len(agg.sessions),
        total_messages=sum(s.messages for s in agg.sessions),
        total_tool_calls=sum(s.tool_calls for s in agg.sessions),
        total_turns=sum(s.turns for s in agg.sessions),
        total_cost=total_cost,
        first_date=first_date,
        models=models,
        model_costs=model_costs,
        cost_breakdown=cb,
        daily=daily,
        hour_counts=dict(agg.messages_by_hour),
        rate_limits=[],
        projects=_build_projects(agg),
        session_tokens=[s.context_tokens for s in agg.sessions if s.context_tokens > 0],
        heatmap=list(agg.heatmap),
        model_first_seen=dict(agg.model_first_seen),
        longest_session_duration_ms=longest_dur_ms,
        longest_session_messages=longest_msgs,
        unpriced_models=unpriced_models,
        unpriced_tokens=unpriced_tokens,
        extra={},
    )
    _save_cache(cache_path, fp, result)
    return result
