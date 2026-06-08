"""Shared data models for token-counter."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass
class TokenBreakdown:
    """Unified token counts across tools.

    input_tokens is always non-cached input.
    cache_read_tokens is cached input (Claude: cacheReadInputTokens, Codex: cached_input_tokens).
    cache_write_tokens is Claude-only (cacheCreationInputTokens), total across TTLs.
    cache_write_1h_tokens is the 1-hour-TTL subset of cache_write_tokens (remainder = 5-minute).
    reasoning_tokens is Codex-only (reasoning_output_tokens).
    """

    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    cache_write_1h_tokens: int = 0
    reasoning_tokens: int = 0

    @property
    def total(self) -> int:
        # reasoning_tokens is intentionally excluded: for Codex it is a subset
        # of output_tokens (on-disk total_tokens == input + output), so adding
        # it here would double-count. For Claude it is always 0.
        return (
            self.input_tokens
            + self.output_tokens
            + self.cache_read_tokens
            + self.cache_write_tokens
        )

    def add(self, other: TokenBreakdown) -> None:
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.cache_read_tokens += other.cache_read_tokens
        self.cache_write_tokens += other.cache_write_tokens
        self.cache_write_1h_tokens += other.cache_write_1h_tokens
        self.reasoning_tokens += other.reasoning_tokens


@dataclass
class CostBreakdown:
    """Per-category cost breakdown."""

    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0

    input_cost: float = 0.0
    output_cost: float = 0.0
    cache_read_cost: float = 0.0
    cache_write_cost: float = 0.0

    @property
    def total_cost(self) -> float:
        return (
            self.input_cost
            + self.output_cost
            + self.cache_read_cost
            + self.cache_write_cost
        )

    def add(self, other: CostBreakdown) -> None:
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.cache_read_tokens += other.cache_read_tokens
        self.cache_write_tokens += other.cache_write_tokens
        self.input_cost += other.input_cost
        self.output_cost += other.output_cost
        self.cache_read_cost += other.cache_read_cost
        self.cache_write_cost += other.cache_write_cost


@dataclass
class DayActivity:
    """Activity for a single day."""

    day: date
    messages: int = 0
    sessions: int = 0
    tool_calls: int = 0
    output_tokens: int = 0
    cost: float = 0.0

    def add(self, other: DayActivity) -> None:
        self.messages += other.messages
        self.sessions += other.sessions
        self.tool_calls += other.tool_calls
        self.output_tokens += other.output_tokens
        self.cost += other.cost


@dataclass
class RateLimitInfo:
    """Rate limit status."""

    label: str  # "5-Hour", "7-Day"
    utilization: float  # 0-100
    resets_in: str  # "2h 30m", "now", "?"


@dataclass
class ProjectInfo:
    """Per-project usage summary."""

    path: str
    cost: float
    input_tokens: int = 0
    output_tokens: int = 0
    lines_added: int = 0
    lines_removed: int = 0
    duration_ms: int = 0

    def add(self, other: ProjectInfo) -> None:
        self.cost += other.cost
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.lines_added += other.lines_added
        self.lines_removed += other.lines_removed
        self.duration_ms += other.duration_ms


def _pad_heatmap(vals) -> list[int]:
    """Normalize a deserialized heatmap to exactly 168 ints (defensive against an
    absent or malformed cached value)."""
    out = [0] * 168
    if isinstance(vals, list):
        for i, v in enumerate(vals[:168]):
            try:
                out[i] = int(v)
            except (TypeError, ValueError):
                pass
    return out


@dataclass
class ToolStats:
    """Unified stats from a single tool (Claude or Codex)."""

    source: str  # "claude" or "codex"
    total_tokens: TokenBreakdown = field(default_factory=TokenBreakdown)
    total_sessions: int = 0
    total_messages: int = 0
    total_tool_calls: int = 0
    total_turns: int = 0
    total_cost: float = 0.0
    first_date: date | None = None

    models: dict[str, TokenBreakdown] = field(default_factory=dict)
    model_costs: dict[str, float] = field(default_factory=dict)
    cost_breakdown: CostBreakdown = field(default_factory=CostBreakdown)

    daily: list[DayActivity] = field(default_factory=list)
    hour_counts: dict[int, int] = field(
        default_factory=lambda: {h: 0 for h in range(24)}
    )

    rate_limits: list[RateLimitInfo] = field(default_factory=list)
    projects: list[ProjectInfo] = field(default_factory=list)

    # Per-tool-name invocation counts (Claude: Bash/Edit/Read/…, Codex: exec_command/
    # apply_patch/…). Grok records only an aggregate tool-call count (no per-name
    # data), so it is not represented here. Raw tool names, deduped per source the
    # same way tool_calls is; merged across machines by summing per key.
    tool_calls_by_name: dict[str, int] = field(default_factory=dict)

    # Per-session $ costs (Codex: one per session_meta.id; Claude: one per session
    # transcript file). Retained so the dashboard can show the spend *distribution*
    # — the top-1%-of-sessions concentration — not just the average. Merged across
    # machines by concatenation; rounded to cents to stay compact.
    session_costs: list[float] = field(default_factory=list)
    # Per-session TOTAL token counts (same partition as session_costs). Token-native
    # so a cache-heavy runaway session — cheap per token yet enormous in volume —
    # surfaces even when its cost doesn't stand out. Powers the Tok/Sess avg-vs-max
    # row that makes a single thousands-of-turns session visible at a glance.
    session_tokens: list[int] = field(default_factory=list)

    # weekday*24 + hour -> message count, local time (activity heatmap).
    heatmap: list[int] = field(default_factory=lambda: [0] * 168)
    # stop_reason -> count (Claude only: end_turn/tool_use/max_tokens/refusal/…).
    stop_reasons: dict[str, int] = field(default_factory=dict)
    # model id -> earliest ISO day it was seen (model-adoption timeline).
    model_first_seen: dict[str, str] = field(default_factory=dict)
    # ISO day -> max Codex 5-Hour rate-limit utilization seen that day (history for a
    # utilization sparkline; Claude rate limits are live-only, so no history exists).
    rate_limit_history: dict[str, float] = field(default_factory=dict)

    longest_session_duration_ms: int = 0
    longest_session_messages: int = 0

    unpriced_models: set[str] = field(default_factory=set)
    unpriced_tokens: int = 0

    # Tool-specific extras (tier string, etc.)
    extra: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Serialize to JSON-friendly dict."""
        return {
            "source": self.source,
            "total_tokens": {
                "input": self.total_tokens.input_tokens,
                "output": self.total_tokens.output_tokens,
                "cache_read": self.total_tokens.cache_read_tokens,
                "cache_write": self.total_tokens.cache_write_tokens,
                "cache_write_1h": self.total_tokens.cache_write_1h_tokens,
                "reasoning": self.total_tokens.reasoning_tokens,
                "total": self.total_tokens.total,
            },
            "total_sessions": self.total_sessions,
            "total_messages": self.total_messages,
            "total_tool_calls": self.total_tool_calls,
            "total_turns": self.total_turns,
            "total_cost": round(self.total_cost, 2),
            "first_date": self.first_date.isoformat() if self.first_date else None,
            "models": {
                m: {
                    "input": tb.input_tokens,
                    "output": tb.output_tokens,
                    "cache_read": tb.cache_read_tokens,
                    "cache_write": tb.cache_write_tokens,
                    "cache_write_1h": tb.cache_write_1h_tokens,
                    "reasoning": tb.reasoning_tokens,
                    "total": tb.total,
                    "cost": round(self.model_costs.get(m, 0), 2),
                }
                for m, tb in self.models.items()
            },
            "cost_breakdown": {
                "input": round(self.cost_breakdown.input_cost, 2),
                "output": round(self.cost_breakdown.output_cost, 2),
                "cache_read": round(self.cost_breakdown.cache_read_cost, 2),
                "cache_write": round(self.cost_breakdown.cache_write_cost, 2),
                "total": round(self.cost_breakdown.total_cost, 2),
            },
            "daily": [
                {
                    "date": da.day.isoformat(),
                    "messages": da.messages,
                    "sessions": da.sessions,
                    "tool_calls": da.tool_calls,
                    "output_tokens": da.output_tokens,
                    "cost": round(da.cost, 4),
                }
                for da in self.daily
            ],
            "hour_counts": {str(k): v for k, v in self.hour_counts.items()},
            "rate_limits": [
                {
                    "label": rl.label,
                    "utilization": rl.utilization,
                    "resets_in": rl.resets_in,
                }
                for rl in self.rate_limits
            ],
            "projects": [
                {
                    "path": p.path,
                    "cost": round(p.cost, 2),
                    "input_tokens": p.input_tokens,
                    "output_tokens": p.output_tokens,
                    "lines_added": p.lines_added,
                    "lines_removed": p.lines_removed,
                    "duration_ms": p.duration_ms,
                }
                for p in self.projects
            ],
            "tool_calls_by_name": self.tool_calls_by_name,
            "session_costs": [round(c, 2) for c in self.session_costs],
            "session_tokens": self.session_tokens,
            "heatmap": self.heatmap,
            "stop_reasons": self.stop_reasons,
            "model_first_seen": self.model_first_seen,
            "rate_limit_history": self.rate_limit_history,
            "longest_session_duration_ms": self.longest_session_duration_ms,
            "longest_session_messages": self.longest_session_messages,
            "unpriced_models": list(self.unpriced_models),
            "unpriced_tokens": self.unpriced_tokens,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, data: dict) -> ToolStats:
        """Reconstruct from dict produced by to_dict()."""
        tt = data.get("total_tokens", {})
        models: dict[str, TokenBreakdown] = {}
        model_costs: dict[str, float] = {}
        for m, md in data.get("models", {}).items():
            models[m] = TokenBreakdown(
                input_tokens=md.get("input", 0),
                output_tokens=md.get("output", 0),
                cache_read_tokens=md.get("cache_read", 0),
                cache_write_tokens=md.get("cache_write", 0),
                cache_write_1h_tokens=md.get("cache_write_1h", 0),
                reasoning_tokens=md.get("reasoning", 0),
            )
            model_costs[m] = md.get("cost", 0.0)
        cbd = data.get("cost_breakdown", {})
        first_date_str = data.get("first_date")
        return cls(
            source=data.get("source", "unknown"),
            total_tokens=TokenBreakdown(
                input_tokens=tt.get("input", 0),
                output_tokens=tt.get("output", 0),
                cache_read_tokens=tt.get("cache_read", 0),
                cache_write_tokens=tt.get("cache_write", 0),
                cache_write_1h_tokens=tt.get("cache_write_1h", 0),
                reasoning_tokens=tt.get("reasoning", 0),
            ),
            total_sessions=data.get("total_sessions", 0),
            total_messages=data.get("total_messages", 0),
            total_tool_calls=data.get("total_tool_calls", 0),
            total_turns=data.get("total_turns", 0),
            total_cost=data.get("total_cost", 0.0),
            first_date=date.fromisoformat(first_date_str) if first_date_str else None,
            models=models,
            model_costs=model_costs,
            cost_breakdown=CostBreakdown(
                input_cost=cbd.get("input", 0.0),
                output_cost=cbd.get("output", 0.0),
                cache_read_cost=cbd.get("cache_read", 0.0),
                cache_write_cost=cbd.get("cache_write", 0.0),
            ),
            daily=[
                DayActivity(
                    day=date.fromisoformat(d["date"]),
                    messages=d.get("messages", 0),
                    sessions=d.get("sessions", 0),
                    tool_calls=d.get("tool_calls", 0),
                    output_tokens=d.get("output_tokens", 0),
                    cost=d.get("cost", 0.0),
                )
                for d in data.get("daily", [])
            ],
            hour_counts={int(k): v for k, v in data.get("hour_counts", {}).items()},
            rate_limits=[
                RateLimitInfo(label=r["label"], utilization=r["utilization"], resets_in=r["resets_in"])
                for r in data.get("rate_limits", [])
            ],
            projects=[
                ProjectInfo(
                    path=p["path"], cost=p.get("cost", 0.0),
                    input_tokens=p.get("input_tokens", 0), output_tokens=p.get("output_tokens", 0),
                    lines_added=p.get("lines_added", 0), lines_removed=p.get("lines_removed", 0),
                    duration_ms=p.get("duration_ms", 0),
                )
                for p in data.get("projects", [])
            ],
            tool_calls_by_name=dict(data.get("tool_calls_by_name", {})),
            session_costs=list(data.get("session_costs", [])),
            session_tokens=[int(t) for t in data.get("session_tokens", [])],
            heatmap=_pad_heatmap(data.get("heatmap", [])),
            stop_reasons=dict(data.get("stop_reasons", {})),
            model_first_seen=dict(data.get("model_first_seen", {})),
            rate_limit_history=dict(data.get("rate_limit_history", {})),
            longest_session_duration_ms=data.get("longest_session_duration_ms", 0),
            longest_session_messages=data.get("longest_session_messages", 0),
            unpriced_models=set(data.get("unpriced_models", [])),
            unpriced_tokens=data.get("unpriced_tokens", 0),
            extra=data.get("extra", {}),
        )
