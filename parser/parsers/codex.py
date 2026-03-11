"""Parse Codex CLI stats from ~/.codex/sessions/**/*.jsonl."""

from __future__ import annotations

import os
import re
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import orjson

from parser.types import (
    CostBreakdown,
    DayActivity,
    ProjectInfo,
    RateLimitInfo,
    TokenBreakdown,
    ToolStats,
)

DATE_SUFFIX_RE = re.compile(r"^(?P<base>.+)-\d{4}-\d{2}-\d{2}$")

SESSIONS_DIR = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")) / "sessions"


@dataclass(frozen=True, slots=True)
class ModelPricing:
    input_usd_per_mtok: float
    cached_input_usd_per_mtok: float | None
    output_usd_per_mtok: float


MODEL_PRICING: dict[str, ModelPricing] = {
    # gpt-5.1-codex-mini normalizes to gpt-5.1-mini
    "gpt-5.1-mini": ModelPricing(
        input_usd_per_mtok=0.25,
        cached_input_usd_per_mtok=0.025,
        output_usd_per_mtok=2.0,
    ),
    # gpt-5.2-codex normalizes to gpt-5.2, gpt-5.2 stays as-is
    "gpt-5.2": ModelPricing(
        input_usd_per_mtok=1.75,
        cached_input_usd_per_mtok=0.175,
        output_usd_per_mtok=14.0,
    ),
    # gpt-5.3-codex normalizes to gpt-5.3
    "gpt-5.3": ModelPricing(
        input_usd_per_mtok=1.75,
        cached_input_usd_per_mtok=0.175,
        output_usd_per_mtok=14.0,
    ),
    # gpt-5.4 stays as-is (no codex suffix to strip)
    "gpt-5.4": ModelPricing(
        input_usd_per_mtok=2.50,
        cached_input_usd_per_mtok=0.25,
        output_usd_per_mtok=15.0,
    ),
    # gpt-5.3-codex-spark doesn't match any strip rule, stays as-is (no official API pricing yet)
    "gpt-5.3-codex-spark": ModelPricing(
        input_usd_per_mtok=0,
        cached_input_usd_per_mtok=0,
        output_usd_per_mtok=0,
    ),
}


# --- Internal token usage (raw, not normalized) ---


@dataclass(slots=True)
class _TokenUsage:
    input_tokens: int = 0
    cached_input_tokens: int = 0
    output_tokens: int = 0
    reasoning_output_tokens: int = 0
    total_tokens: int = 0

    @classmethod
    def from_dict(cls, d: dict | None) -> _TokenUsage:
        if not d:
            return cls()
        return cls(
            input_tokens=int(d.get("input_tokens", 0) or 0),
            cached_input_tokens=int(d.get("cached_input_tokens", 0) or 0),
            output_tokens=int(d.get("output_tokens", 0) or 0),
            reasoning_output_tokens=int(d.get("reasoning_output_tokens", 0) or 0),
            total_tokens=int(d.get("total_tokens", 0) or 0),
        )

    def add(self, other: _TokenUsage) -> None:
        self.input_tokens += other.input_tokens
        self.cached_input_tokens += other.cached_input_tokens
        self.output_tokens += other.output_tokens
        self.reasoning_output_tokens += other.reasoning_output_tokens
        self.total_tokens += other.total_tokens


@dataclass(slots=True)
class _SessionSummary:
    session_id: str
    started_at: datetime | None
    ended_at: datetime | None
    cwd: str | None
    repo_url: str | None
    turns: int
    tool_calls: int
    user_messages: int
    assistant_messages: int
    tokens: _TokenUsage
    tokens_by_model: dict[str, _TokenUsage]
    rate_limits_at: datetime | None = None
    rate_limits: dict | None = None


@dataclass(slots=True)
class _Aggregates:
    earliest: datetime | None = None
    latest: datetime | None = None
    sessions: list[_SessionSummary] = field(default_factory=list)
    totals: _TokenUsage = field(default_factory=_TokenUsage)
    tokens_by_day: dict[date, _TokenUsage] = field(default_factory=dict)
    tokens_by_model: dict[str, _TokenUsage] = field(default_factory=dict)
    messages_by_hour: dict[int, int] = field(
        default_factory=lambda: {h: 0 for h in range(24)}
    )
    daily: dict[date, DayActivity] = field(default_factory=dict)
    tool_calls_by_name: dict[str, int] = field(default_factory=dict)


def _parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _local_day(dt: datetime) -> date:
    return dt.astimezone().date()


def _to_local(dt: datetime) -> datetime:
    return dt.astimezone()


def _parse_session_file(
    path: Path, agg: _Aggregates, *, since: datetime | None
) -> None:
    meta: dict = {}
    started_at: datetime | None = None
    ended_at: datetime | None = None
    contexts: list[tuple[datetime, str]] = []
    token_snapshots: list[tuple[datetime, _TokenUsage]] = []
    turns = tool_calls = user_messages = assistant_messages = 0
    active_days: set[date] = set()
    sess_rl_at: datetime | None = None
    sess_rl: dict | None = None

    def track_session(dt: datetime) -> None:
        nonlocal started_at, ended_at
        if started_at is None or dt < started_at:
            started_at = dt
        if ended_at is None or dt > ended_at:
            ended_at = dt

    def track_agg(dt: datetime) -> None:
        if agg.earliest is None or dt < agg.earliest:
            agg.earliest = dt
        if agg.latest is None or dt > agg.latest:
            agg.latest = dt

    try:
        with path.open("rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = orjson.loads(line)
                except orjson.JSONDecodeError:
                    continue
                dt = _parse_ts(obj.get("timestamp"))
                if dt is None:
                    continue
                in_range = since is None or dt >= since
                track_session(dt)
                if in_range:
                    track_agg(dt)

                typ = obj.get("type")
                payload = obj.get("payload") or {}

                if typ == "session_meta":
                    meta = payload
                    continue

                if typ == "turn_context":
                    model = payload.get("model") or "unknown"
                    contexts.append((dt, str(model)))
                    if in_range:
                        turns += 1
                        d = _local_day(dt)
                        active_days.add(d)
                        agg.daily.setdefault(d, DayActivity(day=d))
                    continue

                if typ == "event_msg" and payload.get("type") == "token_count":
                    rl = payload.get("rate_limits")
                    if rl and (sess_rl_at is None or dt > sess_rl_at):
                        sess_rl_at = dt
                        sess_rl = rl
                    if in_range:
                        active_days.add(_local_day(dt))
                    info = payload.get("info")
                    if not info:
                        continue
                    total = info.get("total_token_usage")
                    if not isinstance(total, dict):
                        continue
                    token_snapshots.append((dt, _TokenUsage.from_dict(total)))
                    continue

                if typ == "response_item":
                    item_type = payload.get("type")
                    if item_type == "function_call":
                        if in_range:
                            tool_calls += 1
                            d = _local_day(dt)
                            active_days.add(d)
                            agg.daily.setdefault(d, DayActivity(day=d)).tool_calls += 1
                            name = payload.get("name")
                            if isinstance(name, str) and name:
                                agg.tool_calls_by_name[name] = (
                                    agg.tool_calls_by_name.get(name, 0) + 1
                                )
                        continue
                    if item_type == "message":
                        if in_range:
                            role = payload.get("role")
                            d = _local_day(dt)
                            active_days.add(d)
                            if role == "user":
                                user_messages += 1
                                agg.daily.setdefault(d, DayActivity(day=d)).messages += 1
                                agg.messages_by_hour[_to_local(dt).hour] += 1
                            elif role == "assistant":
                                assistant_messages += 1
                                agg.daily.setdefault(d, DayActivity(day=d)).messages += 1
                                agg.messages_by_hour[_to_local(dt).hour] += 1
                        continue
    except OSError:
        return

    if since is not None and not active_days:
        return

    for d in active_days:
        agg.daily.setdefault(d, DayActivity(day=d)).sessions += 1

    # Delta computation for token snapshots
    contexts.sort(key=lambda x: x[0])
    ctx_times = [c[0] for c in contexts]
    ctx_models = [c[1] for c in contexts]

    def model_for(dt: datetime) -> str:
        if not ctx_times:
            return "unknown"
        i = bisect_right(ctx_times, dt) - 1
        return ctx_models[i] if i >= 0 else "unknown"

    token_snapshots.sort(key=lambda x: x[0])
    prev = _TokenUsage()
    session_tokens = _TokenUsage()
    session_tokens_by_model: dict[str, _TokenUsage] = {}

    for dt, totals in token_snapshots:
        delta = _TokenUsage(
            input_tokens=max(0, totals.input_tokens - prev.input_tokens),
            cached_input_tokens=max(
                0, totals.cached_input_tokens - prev.cached_input_tokens
            ),
            output_tokens=max(0, totals.output_tokens - prev.output_tokens),
            reasoning_output_tokens=max(
                0, totals.reasoning_output_tokens - prev.reasoning_output_tokens
            ),
            total_tokens=max(0, totals.total_tokens - prev.total_tokens),
        )
        # Handle counter resets
        if totals.input_tokens < prev.input_tokens:
            delta.input_tokens = totals.input_tokens
        if totals.cached_input_tokens < prev.cached_input_tokens:
            delta.cached_input_tokens = totals.cached_input_tokens
        if totals.output_tokens < prev.output_tokens:
            delta.output_tokens = totals.output_tokens
        if totals.reasoning_output_tokens < prev.reasoning_output_tokens:
            delta.reasoning_output_tokens = totals.reasoning_output_tokens
        if totals.total_tokens < prev.total_tokens:
            delta.total_tokens = totals.total_tokens

        prev = totals

        if since is not None and dt < since:
            continue

        m = model_for(dt)
        session_tokens_by_model.setdefault(m, _TokenUsage()).add(delta)
        session_tokens.add(delta)
        agg.tokens_by_model.setdefault(m, _TokenUsage()).add(delta)
        d = _local_day(dt)
        agg.tokens_by_day.setdefault(d, _TokenUsage()).add(delta)
        agg.daily.setdefault(d, DayActivity(day=d)).output_tokens += delta.output_tokens
        agg.totals.add(delta)

    cwd = meta.get("cwd")
    repo_url = (meta.get("git") or {}).get("repository_url")
    agg.sessions.append(
        _SessionSummary(
            session_id=str(meta.get("id") or path.stem),
            started_at=started_at,
            ended_at=ended_at,
            cwd=cwd,
            repo_url=repo_url,
            turns=turns,
            tool_calls=tool_calls,
            user_messages=user_messages,
            assistant_messages=assistant_messages,
            tokens=session_tokens,
            tokens_by_model=session_tokens_by_model,
            rate_limits_at=sess_rl_at,
            rate_limits=sess_rl,
        )
    )


def _normalize_model(model: str) -> str:
    m = str(model or "").strip().lower()
    if ":" in m:
        m = m.split(":", 1)[0]
    for suffix in ("-latest", "-preview"):
        if m.endswith(suffix):
            m = m[: -len(suffix)]
    if match := DATE_SUFFIX_RE.match(m):
        m = match.group("base")
    for suffix in ("-codex-mini", "-codex-nano"):
        if m.endswith(suffix):
            m = m[: -len(suffix)] + suffix.replace("-codex", "")
            break
    for suffix in ("-codex-max", "-codex"):
        if m.endswith(suffix):
            m = m[: -len(suffix)]
            break
    return m


def _pricing_for(model: str) -> ModelPricing | None:
    base = _normalize_model(model)
    p = MODEL_PRICING.get(base)
    if p is not None:
        return p
    for key, pricing in MODEL_PRICING.items():
        if base.startswith(key + "-"):
            return pricing
    return None


def _fmt_reset(resets_at: int | float | None) -> str:
    if resets_at is None:
        return "?"
    try:
        delta = datetime.fromtimestamp(float(resets_at), tz=UTC) - datetime.now(tz=UTC)
    except (ValueError, OSError):
        return "?"
    secs = int(delta.total_seconds())
    if secs <= 0:
        return "now"
    mins, _ = divmod(secs, 60)
    hrs, mins = divmod(mins, 60)
    days, hrs = divmod(hrs, 24)
    if days:
        return f"{days}d {hrs}h"
    if hrs:
        return f"{hrs}h {mins}m"
    return f"{mins}m"


def _convert_rate_limits(rl: dict | None) -> list[RateLimitInfo]:
    if not rl:
        return []
    limits: list[RateLimitInfo] = []
    for label, key in [("5-Hour", "primary"), ("7-Day", "secondary")]:
        window = rl.get(key) or {}
        try:
            pct = float(window.get("used_percent", 0.0) or 0.0)
        except (TypeError, ValueError):
            pct = 0.0
        resets_at = window.get("resets_at")
        wm = window.get("window_minutes")
        if wm is None and resets_at is None and pct == 0.0:
            continue
        limits.append(
            RateLimitInfo(label=label, utilization=pct, resets_in=_fmt_reset(resets_at))
        )
    return limits


def _dir_fingerprint(files: list[Path]) -> str:
    """Fast fingerprint: count + max mtime + total size."""
    total_size = 0
    max_mtime = 0
    for f in files:
        try:
            st = f.stat()
            total_size += st.st_size
            if st.st_mtime_ns > max_mtime:
                max_mtime = st.st_mtime_ns
        except OSError:
            pass
    return f"{len(files)}:{total_size}:{max_mtime}"


def _load_codex_cache(cache_path: Path) -> dict | None:
    if cache_path.exists():
        try:
            return orjson.loads(cache_path.read_bytes())
        except Exception:
            pass
    return None


def _save_codex_cache(cache_path: Path, fingerprint: str, ts: ToolStats) -> None:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(orjson.dumps({"fp": fingerprint, "data": ts.to_dict()}))
    except OSError:
        pass


def parse(*, sessions_dir: Path = SESSIONS_DIR) -> ToolStats | None:
    """Parse Codex CLI session logs. Returns None if no data available."""
    if not sessions_dir.exists():
        return None
    files = sorted(p for p in sessions_dir.rglob("*.jsonl") if p.is_file())
    if not files:
        return None

    # Check cache (stored in repo's .cache dir, not inside ~/.codex/)
    import hashlib
    base_hash = hashlib.md5(str(sessions_dir).encode()).hexdigest()[:12]
    cache_dir = Path(__file__).resolve().parent.parent.parent / ".cache"
    cache_path = cache_dir / f"codex-sessions-{base_hash}.json"
    fp = _dir_fingerprint(files)
    cached = _load_codex_cache(cache_path)
    if cached and cached.get("fp") == fp:
        try:
            return ToolStats.from_dict(cached["data"])
        except Exception:
            pass

    agg = _Aggregates()
    for f in files:
        _parse_session_file(f, agg, since=None)

    if not agg.sessions:
        return None

    # Convert to unified TokenBreakdown (normalize input_tokens to non-cached)
    total_tokens = TokenBreakdown(
        input_tokens=max(
            0, agg.totals.input_tokens - agg.totals.cached_input_tokens
        ),
        output_tokens=agg.totals.output_tokens,
        cache_read_tokens=agg.totals.cached_input_tokens,
        reasoning_tokens=agg.totals.reasoning_output_tokens,
    )

    # Per-model breakdown
    models: dict[str, TokenBreakdown] = {}
    model_costs: dict[str, float] = {}
    cb = CostBreakdown()
    unpriced_models: set[str] = set()
    unpriced_tokens = 0

    for model, usage in agg.tokens_by_model.items():
        cached = max(0, min(usage.cached_input_tokens, usage.input_tokens))
        non_cached = max(0, usage.input_tokens - cached)

        tb = TokenBreakdown(
            input_tokens=non_cached,
            output_tokens=usage.output_tokens,
            cache_read_tokens=cached,
            reasoning_tokens=usage.reasoning_output_tokens,
        )
        models[model] = tb

        pricing = _pricing_for(model)
        if pricing is None:
            unpriced_models.add(model)
            unpriced_tokens += usage.total_tokens
            model_costs[model] = 0.0
            continue

        cached_rate = (
            pricing.input_usd_per_mtok
            if pricing.cached_input_usd_per_mtok is None
            else pricing.cached_input_usd_per_mtok
        )
        ic = non_cached * pricing.input_usd_per_mtok / 1e6
        cc = cached * cached_rate / 1e6
        oc = usage.output_tokens * pricing.output_usd_per_mtok / 1e6
        model_costs[model] = ic + cc + oc

        cb.input_tokens += non_cached
        cb.output_tokens += usage.output_tokens
        cb.cache_read_tokens += cached
        cb.input_cost += ic
        cb.output_cost += oc
        cb.cache_read_cost += cc

    total_cost = sum(model_costs.values())

    # Daily activity (already accumulated in agg.daily)
    daily = sorted(agg.daily.values(), key=lambda d: d.day)

    # Totals
    total_sessions = len(agg.sessions)
    total_messages = sum(
        s.user_messages + s.assistant_messages for s in agg.sessions
    )
    total_tool_calls = sum(s.tool_calls for s in agg.sessions)
    total_turns = sum(s.turns for s in agg.sessions)

    # First date
    first_date = _local_day(agg.earliest) if agg.earliest else None

    # Longest session
    longest_dur_ms = 0
    longest_msgs = 0
    for s in agg.sessions:
        if s.started_at and s.ended_at:
            dur = int((s.ended_at - s.started_at).total_seconds() * 1000)
            if dur > longest_dur_ms:
                longest_dur_ms = dur
                longest_msgs = s.user_messages + s.assistant_messages

    # Rate limits — prefer the most recently started session that has data,
    # not the latest event timestamp (long-running sessions report stale limits)
    best_rl: dict | None = None
    best_rl_session_start: datetime | None = None
    for s in agg.sessions:
        if not s.rate_limits or not s.started_at:
            continue
        # Skip model-specific rate limits (e.g. codex_bengalfox for Spark);
        # only use the main "codex" bucket
        lid = s.rate_limits.get("limit_id", "")
        if lid and lid != "codex":
            continue
        if best_rl_session_start is None or s.started_at > best_rl_session_start:
            best_rl_session_start = s.started_at
            best_rl = s.rate_limits
    rate_limits = _convert_rate_limits(best_rl)

    # Tier
    tier = ""
    if isinstance(best_rl, dict):
        plan_type = best_rl.get("plan_type")
        if isinstance(plan_type, str) and plan_type.strip():
            tier = plan_type.strip()

    # Projects
    projects = _build_projects(agg)

    result = ToolStats(
        source="codex",
        total_tokens=total_tokens,
        total_sessions=total_sessions,
        total_messages=total_messages,
        total_tool_calls=total_tool_calls,
        total_turns=total_turns,
        total_cost=total_cost,
        first_date=first_date,
        models=models,
        model_costs=model_costs,
        cost_breakdown=cb,
        daily=daily,
        hour_counts=dict(agg.messages_by_hour),
        rate_limits=rate_limits,
        projects=projects,
        longest_session_duration_ms=longest_dur_ms,
        longest_session_messages=longest_msgs,
        unpriced_models=unpriced_models,
        unpriced_tokens=unpriced_tokens,
        extra={"tier": tier},
    )
    _save_codex_cache(cache_path, fp, result)
    return result


def _build_projects(agg: _Aggregates) -> list[ProjectInfo]:
    last_by_key: dict[str, _SessionSummary] = {}
    for s in agg.sessions:
        key = s.repo_url or s.cwd or "unknown"
        prev = last_by_key.get(key)
        if prev is None or _stamp(s) > _stamp(prev):
            last_by_key[key] = s

    rows: list[ProjectInfo] = []
    for key, sess in last_by_key.items():
        # Compute cost for this session
        cost = 0.0
        for model, usage in sess.tokens_by_model.items():
            pricing = _pricing_for(model)
            if pricing is None:
                continue
            cached = max(0, min(usage.cached_input_tokens, usage.input_tokens))
            non_cached = max(0, usage.input_tokens - cached)
            cached_rate = (
                pricing.input_usd_per_mtok
                if pricing.cached_input_usd_per_mtok is None
                else pricing.cached_input_usd_per_mtok
            )
            cost += (
                non_cached * pricing.input_usd_per_mtok
                + cached * cached_rate
                + usage.output_tokens * pricing.output_usd_per_mtok
            ) / 1e6
        if cost <= 0:
            continue
        shown = key.replace(str(Path.home()), "~")
        dur_ms = 0
        if sess.started_at and sess.ended_at:
            dur_ms = int((sess.ended_at - sess.started_at).total_seconds() * 1000)
        rows.append(
            ProjectInfo(
                path=shown,
                cost=cost,
                input_tokens=sess.tokens.input_tokens,
                output_tokens=sess.tokens.output_tokens,
                duration_ms=dur_ms,
            )
        )

    rows.sort(key=lambda x: x.cost, reverse=True)
    return rows[:10]


def _stamp(s: _SessionSummary) -> datetime:
    if s.ended_at is not None:
        return s.ended_at
    if s.started_at is not None:
        return s.started_at
    return datetime.min.replace(tzinfo=UTC)
