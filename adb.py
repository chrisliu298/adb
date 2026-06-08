"""adb — CLI tool for Claude Code and Codex usage statistics."""

from __future__ import annotations

import argparse
import calendar
import json
import re
import sys
import time
from copy import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

from rich import box
from rich.align import Align
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()

sys.path.insert(0, str(Path(__file__).resolve().parent))

from parser.parsers import claude as claude_parser
from parser.parsers import codex as codex_parser
from parser.parsers import grok as grok_parser
from parser.types import CostBreakdown, DayActivity, ProjectInfo, TokenBreakdown, ToolStats
from parser import floor

REPO_DIR = Path(__file__).resolve().parent
REMOTE_CACHE = REPO_DIR / ".cache" / "remotes"
DATA_DIR = REPO_DIR / "data"  # in-repo append-only source of truth (gitignored)
REMOTES_CONF = REPO_DIR / "remotes.conf"
CODEX_SESSION_DIR_NAMES = ("sessions", "archived_sessions")


def _local_codex_session_dirs() -> list[Path]:
    codex_home = Path.home() / ".codex"
    return [codex_home / name for name in CODEX_SESSION_DIR_NAMES]


def _load_remote_hosts() -> list[str]:
    """Load remote hostnames from remotes.conf."""
    if not REMOTES_CONF.exists():
        return []
    hosts = []
    for line in REMOTES_CONF.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            hosts.append(line)
    return hosts


# ---------------------------------------------------------------------------
# Multi-machine loading
# ---------------------------------------------------------------------------


def _merge_two(a: ToolStats, b: ToolStats) -> ToolStats:
    """Merge two ToolStats of the same source type."""
    tokens = TokenBreakdown()
    tokens.add(a.total_tokens)
    tokens.add(b.total_tokens)
    models: dict[str, TokenBreakdown] = {}
    for src in (a, b):
        for m, tb in src.models.items():
            if m in models:
                models[m].add(tb)
            else:
                models[m] = copy(tb)
    model_costs: dict[str, float] = {}
    for src in (a, b):
        for m, c in src.model_costs.items():
            model_costs[m] = model_costs.get(m, 0.0) + c
    cb = CostBreakdown()
    cb.add(a.cost_breakdown)
    cb.add(b.cost_breakdown)
    daily_map: dict[date, DayActivity] = {}
    for src in (a, b):
        for d in src.daily:
            if d.day in daily_map:
                daily_map[d.day].add(d)
            else:
                daily_map[d.day] = copy(d)
    hour_counts = {h: a.hour_counts.get(h, 0) + b.hour_counts.get(h, 0) for h in range(24)}
    tool_calls_by_name: dict[str, int] = {}
    for src in (a, b):
        for name, cnt in src.tool_calls_by_name.items():
            tool_calls_by_name[name] = tool_calls_by_name.get(name, 0) + cnt
    heatmap = [a.heatmap[i] + b.heatmap[i] for i in range(168)]
    stop_reasons: dict[str, int] = {}
    for src in (a, b):
        for r, cnt in src.stop_reasons.items():
            stop_reasons[r] = stop_reasons.get(r, 0) + cnt
    model_first_seen: dict[str, str] = {}
    for src in (a, b):
        for m, d in src.model_first_seen.items():
            cur = model_first_seen.get(m)
            if cur is None or d < cur:
                model_first_seen[m] = d
    rate_limit_history: dict[str, float] = {}
    for src in (a, b):
        for d, pct in src.rate_limit_history.items():
            rate_limit_history[d] = max(rate_limit_history.get(d, 0.0), pct)
    first_dates = [s.first_date for s in (a, b) if s.first_date]
    proj_map: dict[str, ProjectInfo] = {}
    for p in a.projects + b.projects:
        if p.path in proj_map:
            proj_map[p.path].add(p)
        else:
            proj_map[p.path] = copy(p)
    projects = sorted(proj_map.values(), key=lambda p: p.cost, reverse=True)
    return ToolStats(
        source=a.source,
        total_tokens=tokens,
        total_sessions=a.total_sessions + b.total_sessions,
        total_messages=a.total_messages + b.total_messages,
        total_tool_calls=a.total_tool_calls + b.total_tool_calls,
        total_turns=a.total_turns + b.total_turns,
        total_cost=a.total_cost + b.total_cost,
        first_date=min(first_dates) if first_dates else None,
        models=models,
        model_costs=model_costs,
        cost_breakdown=cb,
        daily=sorted(daily_map.values(), key=lambda d: d.day),
        hour_counts=hour_counts,
        rate_limits=a.rate_limits or b.rate_limits,
        projects=projects,
        tool_calls_by_name=tool_calls_by_name,
        session_costs=a.session_costs + b.session_costs,
        heatmap=heatmap,
        stop_reasons=stop_reasons,
        model_first_seen=model_first_seen,
        rate_limit_history=rate_limit_history,
        longest_session_duration_ms=(a.longest_session_duration_ms if a.longest_session_duration_ms >= b.longest_session_duration_ms else b.longest_session_duration_ms),
        longest_session_messages=(a.longest_session_messages if a.longest_session_duration_ms >= b.longest_session_duration_ms else b.longest_session_messages),
        unpriced_models=a.unpriced_models | b.unpriced_models,
        unpriced_tokens=a.unpriced_tokens + b.unpriced_tokens,
        extra=a.extra or b.extra,
    )


def _merge_stats(stats: list[ToolStats]) -> ToolStats:
    result = stats[0]
    for s in stats[1:]:
        result = _merge_two(result, s)
    return result


MachineData = dict[str, tuple[ToolStats | None, ToolStats | None, ToolStats | None]]


def _load_machine(
    name: str,
    claude_kwargs: dict | None,
    codex_kwargs: dict | None,
    grok_kwargs: dict | None,
) -> tuple[str, ToolStats | None, ToolStats | None, ToolStats | None]:
    """Load stats for a single machine. Designed for parallel execution."""
    c = claude_parser.parse(**claude_kwargs) if claude_kwargs is not None else None
    x = codex_parser.parse(**codex_kwargs) if codex_kwargs is not None else None
    g = grok_parser.parse(**grok_kwargs) if grok_kwargs is not None else None
    return name, c, x, g


SYNC_SCRIPT = REPO_DIR / "sync.sh"
STALE_HOURS = 6


def _sync_remotes() -> None:
    """Run sync.sh to pull fresh data from remote machines."""
    if not SYNC_SCRIPT.exists():
        return
    import subprocess

    subprocess.run([str(SYNC_SCRIPT)], cwd=str(REPO_DIR))


def _cache_path_mtime(path: Path) -> float | None:
    if path.is_file():
        return path.stat().st_mtime
    if not path.is_dir():
        return None
    mtimes: list[float] = []
    for child in path.rglob("*"):
        if child.is_file():
            mtimes.append(child.stat().st_mtime)
    return max(mtimes) if mtimes else None


def _remote_cache_age_hours(hosts: list[str]) -> float | None:
    """Hours since the newest sync-touched file across hosts. None if no cache."""
    mtimes: list[float] = []
    for host in hosts:
        base = REMOTE_CACHE / host
        for rel in (
            "claude/history.jsonl",
            "claude/stats-cache.json",
            "codex/sessions",
            "codex/archived_sessions",
            "grok/sessions",
        ):
            p = base / rel
            mtime = _cache_path_mtime(p)
            if mtime is not None:
                mtimes.append(mtime)
    if not mtimes:
        return None
    return (time.time() - max(mtimes)) / 3600


def load_all(machines: list[str] | None = None, sync: bool = False) -> tuple[ToolStats | None, ToolStats | None, ToolStats | None, MachineData]:
    """Load and merge stats from local + remote machines.

    machines: list of machine names to include. None or ["all"] means local + all remotes.
              ["local"] means local only. Otherwise, include local + named remotes.
    sync: if True, force a fresh sync.sh run; otherwise sync only when the
          remote cache is missing or older than STALE_HOURS.
    """
    # Build work items: (name, claude_kwargs, codex_kwargs, grok_kwargs)
    work: list[tuple[str, dict | None, dict | None, dict | None]] = []

    # Local machine: read the in-repo durable store (data/) PLUS a live overlay so
    # sessions written since the last ingest show immediately. The parser dedup
    # (msg.id / session_meta.id) collapses the overlap for free; the store carries
    # history the live homes may have lost to the silently-reverting 30-day cleanup.
    # The live overlay (~/.claude/projects) auto-discovers the local-agent-mode
    # container, so the store's local-agent-mode bucket is NOT listed here —
    # listing both would double-count LAM sessions (different rel-paths defeat the
    # dedup). The store still backs LAM up; the floor guard backstops any loss.
    local_ck = dict(projects_base=[
        DATA_DIR / "claude" / "local",
        Path.home() / ".claude" / "projects",  # live overlay (freshness + LAM discovery)
    ])
    local_xk = dict(sessions_dirs=[
        DATA_DIR / "codex" / "local",
        *_local_codex_session_dirs(),  # live overlays
    ])
    local_gk = dict(sessions_dirs=[
        DATA_DIR / "grok" / "local",
        Path.home() / ".grok" / "sessions",  # live overlay
    ])
    work.append(("local", local_ck, local_xk, local_gk))

    # Determine which remotes to include
    all_remotes = _load_remote_hosts()
    if machines is None or machines == ["all"]:
        include_remotes = all_remotes
    elif machines == ["local"]:
        include_remotes = []
    else:
        include_remotes = [h for h in machines if h in all_remotes]

    # Sync remotes when forced (--sync) or when cache is stale/missing.
    if include_remotes:
        if sync:
            console.print("[grey50]Syncing remotes (forced)...[/grey50]")
            _sync_remotes()
        else:
            age = _remote_cache_age_hours(include_remotes)
            if age is None:
                console.print("[grey50]No remote cache found — syncing...[/grey50]")
                _sync_remotes()
            elif age > STALE_HOURS:
                console.print(f"[grey50]Remote cache {age:.0f}h old — syncing...[/grey50]")
                _sync_remotes()

    for host in include_remotes:
        # Read each remote's tokens from the in-repo durable store only. The store
        # already folds the rsync mirror, archived sessions, and recovery archive
        # into one append-only bucket per host (ingest.sh), so there is nothing to
        # combine here and nothing the live sources can silently delete out from
        # under the lifetime total.
        ck = None
        cdir = DATA_DIR / "claude" / host
        if cdir.is_dir():
            # stats-cache.json may be absent (a host whose .meta didn't sync); the
            # parser falls back to the session JSONL, so a missing 4 KB meta file no
            # longer silently drops the host's entire token total.
            ck = dict(
                stats_path=cdir / ".meta" / "stats-cache.json",
                history_path=cdir / ".meta" / "history.jsonl",
                projects_base=[cdir],
            )
        xdir = DATA_DIR / "codex" / host
        xk = dict(sessions_dirs=[xdir]) if xdir.is_dir() else None
        gdir = DATA_DIR / "grok" / host
        gk = dict(sessions_dir=gdir) if gdir.is_dir() else None
        if ck is not None or xk is not None or gk is not None:
            work.append((host, ck, xk, gk))

    # Parse all machines in parallel
    claude_list: list[ToolStats] = []
    codex_list: list[ToolStats] = []
    grok_list: list[ToolStats] = []
    per_machine: MachineData = {}

    with ThreadPoolExecutor(max_workers=len(work)) as pool:
        futures = [pool.submit(_load_machine, *w) for w in work]
        for f in futures:
            name, c, x, g = f.result()
            if c:
                claude_list.append(c)
            if x:
                codex_list.append(x)
            if g:
                grok_list.append(g)
            if c or x or g:
                per_machine[name] = (c, x, g)

    claude = _merge_stats(claude_list) if claude_list else None
    codex = _merge_stats(codex_list) if codex_list else None
    grok = _merge_stats(grok_list) if grok_list else None
    return claude, codex, grok, per_machine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def fmt_tokens(n: int) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return f"{n:,}"


def fmt_cost(c: float) -> str:
    return f"${c:,.2f}"


def fmt_duration(ms: int) -> str:
    secs = ms // 1000
    if secs < 60:
        return f"{secs}s"
    mins, secs = divmod(secs, 60)
    if mins < 60:
        return f"{mins}m {secs}s"
    hrs, mins = divmod(mins, 60)
    return f"{hrs}h {mins}m"


def fmt_pct(num: float, denom: float) -> str:
    if denom == 0:
        return "—"
    return f"{num / denom * 100:.1f}%"


def _fmt_weighted(num: float, den: float) -> str:
    """Token-weighted $/MTok average, or '—' when there is nothing to weigh."""
    return f"${num / den:.2f}" if den else "—"


def short_project_name(path: str) -> str:
    """Extract repo/directory name from a full path or git URL."""
    # git@github.com:user/repo.git -> repo
    m = re.search(r"/([^/]+?)(?:\.git)?$", path)
    if m:
        return m.group(1)
    # ~/Developer/GitHub/repo -> repo
    parts = path.rstrip("/").split("/")
    return parts[-1] if parts else path


def _model_family(name: str) -> str:
    """Map a raw model name to a display family.

    Claude models keep their tier split (Opus/Sonnet/Haiku) under a "Claude"
    prefix; GPT and Grok each get their own section; everything else (including
    non-Anthropic models routed through Claude Code) folds into a single "Others"
    section. Matching is anchored to the vendor prefix so a stray tier substring
    in another vendor's id can't mis-family it, and the GPT label carries the
    major version derived from the name so a future GPT-6 isn't called "GPT-5".
    """
    n = name.lower()
    if n.startswith("claude"):
        if "opus" in n:   return "Claude Opus"
        if "sonnet" in n: return "Claude Sonnet"
        if "haiku" in n:  return "Claude Haiku"
        return "Claude"  # other/future Claude models
    if n.startswith("gpt"):
        m = re.match(r"gpt-?(\d+)", n)
        return f"GPT-{m.group(1)}" if m else "GPT"
    if n.startswith("grok"):
        return "Grok"
    return "Others"


def _model_prices(name: str) -> tuple[float | None, float | None]:
    """List input/output price ($/MTok) for a model, or (None, None) if unpriced.

    Tries Grok -> Claude -> Codex pricing; the raw model names are disjoint
    across the three, so the lookup order only decides who answers first.
    """
    gp = grok_parser._pricing_for(name)
    if gp:
        return gp.input_usd_per_mtok, gp.output_usd_per_mtok
    pk = claude_parser._pkey(name)
    if pk and pk in claude_parser.PRICE:
        p = claude_parser.PRICE[pk]
        return p[0], p[1]
    cp = codex_parser._pricing_for(name)
    if cp:
        return cp.input_usd_per_mtok, cp.output_usd_per_mtok
    return None, None


class _Member(NamedTuple):
    name: str
    tb: TokenBreakdown
    cost: float
    in_p: float | None
    out_p: float | None


class _Family(NamedTuple):
    name: str
    tb: TokenBreakdown   # summed token breakdown across members
    cost: float          # summed cost across members
    in_num: float        # Σ in_price * (input + cache_read + cache_write)
    in_den: float        # Σ (input + cache_read + cache_write) over priced members
    out_num: float       # Σ out_price * output
    out_den: float       # Σ output over priced members
    members: list[_Member]


def _family_summaries(merged_models: dict[str, tuple[TokenBreakdown, float]]) -> list[_Family]:
    """Group merged models into families with rollup tokens/cost/weighted price.

    Pure (no rendering). Families sort by cost desc, then by total tokens, then
    name — a total order, so the output is reproducible even when costs tie
    (e.g. all-unpriced/zero-cost families). Members within a family sort the same
    way. The per-family weighted In/Out $/M numerators/denominators sum to the
    grand totals, so the family rollups stay consistent with the Total row.
    """
    groups: dict[str, list[tuple[str, TokenBreakdown, float]]] = {}
    for m, (tb, cost) in merged_models.items():
        groups.setdefault(_model_family(m), []).append((m, tb, cost))
    families: list[_Family] = []
    for fam, items in groups.items():
        items.sort(key=lambda x: (-x[2], -x[1].total, x[0].lower()))
        f_tb = TokenBreakdown()
        in_num = in_den = out_num = out_den = 0.0
        members: list[_Member] = []
        for m, tb, cost in items:
            in_p, out_p = _model_prices(m)
            if in_p is not None:
                in_toks = tb.input_tokens + tb.cache_read_tokens + tb.cache_write_tokens
                in_num += in_p * in_toks
                in_den += in_toks
            if out_p is not None:
                out_num += out_p * tb.output_tokens
                out_den += tb.output_tokens
            f_tb.add(tb)
            members.append(_Member(m, tb, cost, in_p, out_p))
        families.append(_Family(
            fam, f_tb, sum(c for _, _, c in items),
            in_num, in_den, out_num, out_den, members,
        ))
    families.sort(key=lambda f: (-f.cost, -f.tb.total, f.name.lower()))
    return families


def _history_active_days(history_path: Path) -> set[date]:
    """Extract active days from a history.jsonl file by timestamps."""
    days: set[date] = set()
    if not history_path.exists():
        return days
    try:
        import orjson
        with history_path.open("rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = orjson.loads(line)
                    ts = entry.get("timestamp")
                    if ts:
                        dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                        days.add(dt.date())
                except (orjson.JSONDecodeError, ValueError, TypeError):
                    continue
    except OSError:
        pass
    return days


HISTORY_PATH = Path.home() / ".claude" / "history.jsonl"


def compute_streak(
    claude: ToolStats | None,
    codex: ToolStats | None,
    grok: ToolStats | None = None,
    per_machine: MachineData | None = None,
) -> int:
    active_days: set[date] = set()
    for s in (claude, codex, grok):
        if s is None:
            continue
        for d in s.daily:
            if d.output_tokens > 0:
                active_days.add(d.day)

    # History.jsonl fallback: fill gaps from local + remote history files
    active_days |= _history_active_days(HISTORY_PATH)
    for host in _load_remote_hosts():
        active_days |= _history_active_days(
            REMOTE_CACHE / host / "claude" / "history.jsonl"
        )

    if not active_days:
        return 0
    today = date.today()
    day = today if today in active_days else today - timedelta(days=1)
    if day not in active_days:
        return 0
    streak = 0
    while day in active_days:
        streak += 1
        day -= timedelta(days=1)
    return streak


# ---------------------------------------------------------------------------
# Terminal output
# ---------------------------------------------------------------------------


def print_stats(
    claude: ToolStats | None,
    codex: ToolStats | None,
    grok: ToolStats | None = None,
    per_machine: MachineData | None = None,
    apply_floor: bool = False,
    rebaseline: bool = False,
) -> None:
    stats_list = [s for s in (claude, codex, grok) if s is not None]
    combined = _merge_stats(stats_list)
    total_cost = combined.total_cost
    total_tokens = combined.total_tokens.total
    # Floor guard: the lifetime per-tool token total must never decrease. Only on
    # the full run (a subset computes less, which must not trip it). On a drop,
    # HOLD ONLY the headline lifetime cell at the high-water — all derived math
    # (cost/1K, daily avg, per-machine) keeps the real computed totals so ratios
    # stay consistent; the banner explains the gap.
    lifetime_display = total_tokens
    if apply_floor:
        _computed = {
            "claude": claude.total_tokens.total if claude else 0,
            "codex": codex.total_tokens.total if codex else 0,
            "grok": grok.total_tokens.total if grok else 0,
        }
        _effective, _regressions = floor.apply(_computed, rebaseline=rebaseline)
        lifetime_display = sum(_effective.values())
        if _regressions:
            if any(t == floor.UNREADABLE for t, _, _ in _regressions):
                _msg = (
                    "[bold]FLOOR LEDGER UNREADABLE[/bold] — the data-loss guard is DEGRADED and was not "
                    "updated. Inspect data/.meta/adb-floor.json before trusting this number."
                )
            else:
                _parts = ", ".join(
                    f"{t} {fmt_tokens(fv)}→{fmt_tokens(cv)} (-{fmt_tokens(fv - cv)})"
                    for t, fv, cv in _regressions
                )
                _msg = (
                    f"[bold]DATA-LOSS ALERT[/bold] — lifetime total dropped: {_parts}.\n"
                    "The header cell is held at the recorded high-water; the sections below show the "
                    "real (lower) computed values. Investigate the store, or re-run with --rebaseline to accept it."
                )
            console.print(Panel(_msg, style="red", border_style="red"))
    total_sessions = combined.total_sessions
    total_messages = combined.total_messages
    total_tool_calls = combined.total_tool_calls
    total_turns = combined.total_turns
    total_output_tokens = combined.total_tokens.output_tokens
    first_date = combined.first_date
    _all_active_days = {d.day for d in combined.daily}
    _all_active_days |= _history_active_days(HISTORY_PATH)
    for _host in _load_remote_hosts():
        _all_active_days |= _history_active_days(DATA_DIR / "claude" / _host / ".meta" / "history.jsonl")
    days_active = len(_all_active_days)
    streak = compute_streak(claude, codex, grok)
    daily_by_date = {d.day: d for d in combined.daily}
    merged_models = {m: (tb, combined.model_costs.get(m, 0.0)) for m, tb in combined.models.items()}
    merged_hours = combined.hour_counts
    agg_cb = combined.cost_breakdown
    # Aggregate projects by short name for display
    projects_by_name: dict[str, ProjectInfo] = {}
    for p in combined.projects:
        name = short_project_name(p.path)
        if name in projects_by_name:
            projects_by_name[name].add(p)
        else:
            projects_by_name[name] = ProjectInfo(
                path=name, cost=p.cost, input_tokens=p.input_tokens,
                output_tokens=p.output_tokens, lines_added=p.lines_added,
                lines_removed=p.lines_removed, duration_ms=p.duration_ms,
            )

    # --- 1. RECENT (Today / This Week / This Month) ---
    today = date.today()
    cost_per_token = total_cost / total_output_tokens if total_output_tokens > 0 else 0

    def _sum_range(start: date, end: date) -> tuple[int, int, int, int]:
        """Sum messages, sessions, tool_calls, output_tokens for [start, end]."""
        msgs = sess = tools = otoks = 0
        for d, da in daily_by_date.items():
            if start <= d <= end:
                msgs += da.messages
                sess += da.sessions
                tools += da.tool_calls
                otoks += da.output_tokens
        return msgs, sess, tools, otoks

    # Today
    t_msgs, t_sess, t_tools, t_otoks = _sum_range(today, today)
    t_cost = t_otoks * cost_per_token

    # This week (Mon-today)
    week_start = today - timedelta(days=today.weekday())
    w_msgs, w_sess, w_tools, w_otoks = _sum_range(week_start, today)
    w_cost = w_otoks * cost_per_token
    # Last week (same number of elapsed days for fair comparison)
    n_week_days = (today - week_start).days + 1
    prev_week_start = week_start - timedelta(days=7)
    prev_week_end = prev_week_start + timedelta(days=n_week_days - 1)
    _, _, _, pw_otoks = _sum_range(prev_week_start, prev_week_end)
    pw_cost = pw_otoks * cost_per_token

    # This month (1st-today)
    month_start = today.replace(day=1)
    m_msgs, m_sess, m_tools, m_otoks = _sum_range(month_start, today)
    m_cost = m_otoks * cost_per_token
    # Last month (same number of elapsed days for fair comparison)
    prev_month_start = (month_start - timedelta(days=1)).replace(day=1)
    prev_month_end = prev_month_start + timedelta(days=today.day - 1)
    _, _, _, pm_otoks = _sum_range(prev_month_start, prev_month_end)
    pm_cost = pm_otoks * cost_per_token

    # Cost deltas
    def _delta(cur: float, prev: float) -> str:
        if prev <= 0:
            return ""
        pct = (cur - prev) / prev * 100
        sign = "+" if pct >= 0 else ""
        return f" ({sign}{pct:.0f}%)"

    w_delta = _delta(w_cost, pw_cost)
    m_delta = _delta(m_cost, pm_cost)

    # Date labels
    if week_start == today:
        w_label = f"{week_start.strftime('%b %d')}-"
    else:
        w_label = f"{week_start.strftime('%b %d')}-{today.strftime('%d')}"
    m_label = f"{month_start.strftime('%b %d')}-"

    # Daily averages
    avg_msgs = total_messages / days_active if days_active else 0
    avg_sessions = total_sessions / days_active if days_active else 0
    avg_cost = total_cost / days_active if days_active else 0

    # --- Display ---
    width = min(console.width, 105)
    BORDER = "grey30"
    ACCENT = "#d7af5f"
    ACCENT_BOLD = f"bold {ACCENT}"
    CLAUDE_COLOR = "#d77757"
    CODEX_COLOR = "#39c5cf"
    GROK_COLOR = "#c0c0c0"
    TOOL_COLORS = {"claude": CLAUDE_COLOR, "codex": CODEX_COLOR, "grok": GROK_COLOR}
    TOOL_NAMES = {"claude": "Claude", "codex": "Codex", "grok": "Grok"}
    COST_COLORS = {
        "Cache Rd": "#5f87d7",
        "Cache Wr": "#d75f87",
        "Input":    "#af87d7",
        "Output":   "#87d7af",
    }
    SPARK_GRADIENT = [
        "#2a5a5a", "#3a7a7a", "#4a9a9a", "#5ababa",
        "#6adada", "#7aeaea", "#8affff", "#afffff",
    ]
    SECTION_COLORS = {
        "Recent":          "bold cornflower_blue",
        "Models":          "bold medium_purple",
        "Cost":            f"bold {ACCENT}",
        "Growth":          "bold #5fd7af",
        "Activity":        "bold deep_sky_blue1",
        "Machines":        "bold #87afaf",
        "Projects (Top 10)": "bold #87d787",
        "Tools":           "bold #d7afd7",
        "Agents":          "bold #5fafd7",
        "Heatmap":         "bold deep_sky_blue1",
    }

    def fmt_cost_styled(c: float) -> str:
        return f"[{ACCENT}]{fmt_cost(c)}[/{ACCENT}]"

    def _section(content, title, **kwargs):
        tc = SECTION_COLORS.get(title, "bold")
        return Panel(
            content, title=f"[{tc}]{title}[/{tc}]", title_align="left",
            border_style=BORDER, box=box.ROUNDED, padding=(0, 1), width=width, **kwargs,
        )

    def _trunc(s: str, n: int) -> str:
        """Truncate to n chars with a trailing ellipsis, so a clipped name reads as
        clipped (vs a hard slice that looks like a real, shorter name)."""
        return s if len(s) <= n else s[: n - 1] + "…"

    def _panel(content, title):
        """A section panel WITHOUT a fixed width — sizes to its container cell, for
        use inside a paired 2-column row (vs _section which spans the full width)."""
        tc = SECTION_COLORS.get(title, "bold")
        return Panel(
            content, title=f"[{tc}]{title}[/{tc}]", title_align="left",
            border_style=BORDER, box=box.ROUNDED, padding=(0, 1),
        )

    def _body_rows(content) -> int:
        """Rendered line count of a panel body: a Table contributes its row_count,
        any other renderable (Text, sparkline, footnote) one line, a Group the sum.
        Used to balance two paired panels to equal height regardless of gated rows."""
        if isinstance(content, Group):
            return sum(_body_rows(r) for r in content.renderables)
        if isinstance(content, Table):
            return content.row_count
        return 1

    PAIR_MIN_WIDTH = 92  # below this, paired panels truncate — stack them full-width

    def _pair(left_content, left_title, right_content, right_title, ratio=(1, 1)):
        """Render two sections side-by-side, padded to equal height. Below
        PAIR_MIN_WIDTH the cells get too narrow (values truncate), so stack
        full-width instead. Height is balanced by padding the shorter body with
        blank lines computed from the ACTUAL row counts, so it survives the gated
        rows that vary the two panels' heights run-to-run."""
        if width < PAIR_MIN_WIDTH:
            console.print(_section(left_content, left_title))
            console.print(_section(right_content, right_title))
            return
        lr, rr = _body_rows(left_content), _body_rows(right_content)
        target = max(lr, rr)
        lc = Group(left_content, *(Text("") for _ in range(target - lr))) if lr < target else left_content
        rc = Group(right_content, *(Text("") for _ in range(target - rr))) if rr < target else right_content
        row = Table(box=None, show_header=False, show_edge=False, padding=0, width=width)
        row.add_column("left", ratio=ratio[0])
        row.add_column("right", ratio=ratio[1])
        row.add_row(_panel(lc, left_title), _panel(rc, right_title))
        console.print(row)

    def _styled_delta(cur: float, prev: float) -> str:
        if prev <= 0:
            return ""
        pct = (cur - prev) / prev * 100
        if pct > 0:
            return f" [#5fd787](\u2191{pct:.0f}%)[/#5fd787]"
        elif pct < 0:
            return f" [#d75f5f](\u2193{abs(pct):.0f}%)[/#d75f5f]"
        else:
            return f" [grey50](\u21920%)[/grey50]"

    def _kv_table() -> Table:
        t = Table(box=None, show_header=False, show_edge=False, padding=(0, 2, 0, 0), expand=True)
        t.add_column("key", style="bold", no_wrap=True, width=18)
        t.add_column("value", no_wrap=True, ratio=1)
        return t

    # --- Header ---
    def _gauge(pct: float, gauge_width: int = 10) -> tuple[str, str, str]:
        filled = round(pct / 100 * gauge_width)
        color = "green" if pct < 50 else ("yellow" if pct < 80 else "red")
        return ("█" * filled, "░" * (gauge_width - filled), color)

    blank = Text("")

    # Hero line — centered: total cost · per-tool breakdowns with tiers
    hero_text = Text()
    hero_text.append(fmt_cost(total_cost), style=ACCENT_BOLD)
    if len(stats_list) > 1:
        # Multiple tools: show each tool's name + cost (+ tier).
        for s in stats_list:
            tier = s.extra.get("tier", "")
            hero_text.append(" · ", style="grey37")
            hero_text.append(TOOL_NAMES.get(s.source, s.source.title()), style=TOOL_COLORS.get(s.source, "grey62"))
            hero_text.append(f" {fmt_cost(s.total_cost)}", style=ACCENT)
            if tier:
                hero_text.append(f" ({tier[:1].upper()}{tier[1:]})", style="grey62")
    else:
        # Single tool: total already equals its cost, so just name (+ tier).
        s = stats_list[0]
        tier = s.extra.get("tier", "")
        hero_text.append(" · ", style="grey37")
        hero_text.append(TOOL_NAMES.get(s.source, s.source.title()), style=TOOL_COLORS.get(s.source, "grey62"))
        if tier:
            hero_text.append(f" ({tier[:1].upper()}{tier[1:]})", style="grey62")
    hero = hero_text

    # Stats ribbon — left-aligned
    sep = " · "
    ribbon = Text()
    ribbon.append(f"{total_sessions:,} sess", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{fmt_tokens(lifetime_display)} tok", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{days_active}d", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{fmt_cost(avg_cost)}/d", style=ACCENT)
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{avg_msgs:,.0f} msg/d", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{avg_sessions:,.0f} sess/d", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{streak}d streak", style="grey62")

    # Rate limit gauges
    gauge_lines: list[Text] = []
    rl_sources: list[tuple[str, str, list]] = []
    for s in stats_list:
        if not s.rate_limits:
            continue
        prefix = TOOL_NAMES.get(s.source, s.source.title())
        color = TOOL_COLORS.get(s.source, "grey62")
        entries = []
        for rl in s.rate_limits:
            entries.append((rl.label, rl.utilization, rl.resets_in))
        if entries:
            rl_sources.append((prefix, color, entries))

    if rl_sources:
        show_prefix = len(rl_sources) > 1
        # Find max width per column index so separators align across rows
        max_cols = max(len(entries) for _, _, entries in rl_sources)
        col_widths = [0] * max_cols
        for _, _, entries in rl_sources:
            for i, (label, util, resets_in) in enumerate(entries):
                # width of: "label ██████████ XXX% (reset)"
                w = len(label) + 1 + 10 + 1 + 4 + 1 + len(f"({resets_in})")
                col_widths[i] = max(col_widths[i], w)
        for prefix, prefix_color, entries in rl_sources:
            line = Text()
            if show_prefix:
                line.append(f"{prefix:<8s}", style=prefix_color)
            for i, (label, util, resets_in) in enumerate(entries):
                if i > 0:
                    line.append(" · ", style="grey37")
                filled, empty, bar_color = _gauge(util)
                reset_str = f"({resets_in})"
                # Current entry width
                cur_w = len(label) + 1 + 10 + 1 + 4 + 1 + len(reset_str)
                pad = col_widths[i] - cur_w
                line.append(f"{label} ", style="grey62")
                line.append(filled, style=bar_color)
                line.append(empty, style="bright_black")
                line.append(f" {util:>3.0f}%", style=bar_color)
                line.append(f" {reset_str}", style="grey50")
                if pad > 0:
                    line.append(" " * pad)
            gauge_lines.append(line)

    # Assemble header panel
    header_parts = [hero, ribbon]
    if gauge_lines:
        header_parts.extend(gauge_lines)

    header_tc = "bold bright_white"
    console.print()
    console.print(Panel(
        Group(*header_parts),
        title=f"[{header_tc}]adb[/{header_tc}]", title_align="left",
        border_style=BORDER, box=box.ROUNDED, padding=(0, 1), width=width,
    ))

    # --- 1. RECENT ---
    w_delta_rich = _styled_delta(w_cost, pw_cost)
    m_delta_rich = _styled_delta(m_cost, pm_cost)
    if week_start == today:
        w_label = f"{week_start.strftime('%b %d')}-"
    else:
        w_label = f"{week_start.strftime('%b %d')}-{today.strftime('%d')}"
    m_label = f"{month_start.strftime('%b %d')}-"
    all_label = f"{first_date.isoformat()}~" if first_date else ""

    recent = Table(box=box.SIMPLE_HEAD, padding=(0, 1), show_edge=False, expand=True)
    recent.add_column("", style="bold", no_wrap=True)
    recent.add_column(f"Today\n{today.strftime('%b %d')}", justify="right", no_wrap=True)
    recent.add_column(f"This Week\n{w_label}", justify="right", no_wrap=True)
    recent.add_column(f"This Month\n{m_label}", justify="right", no_wrap=True)
    recent.add_column(f"All Time\n{all_label}", justify="right", no_wrap=True)
    recent.add_row("Messages", f"{t_msgs:,}", f"{w_msgs:,}", f"{m_msgs:,}", f"{total_messages:,}")
    recent.add_row("Sessions", f"{t_sess:,}", f"{w_sess:,}", f"{m_sess:,}", f"{total_sessions:,}")
    recent.add_row("Tool Calls", f"{t_tools:,}", f"{w_tools:,}", f"{m_tools:,}", f"{total_tool_calls:,}")
    tok_ratio = total_tokens / total_output_tokens if total_output_tokens > 0 else 0
    recent.add_row("Tokens", f"~{fmt_tokens(int(t_otoks * tok_ratio))}", f"~{fmt_tokens(int(w_otoks * tok_ratio))}", f"~{fmt_tokens(int(m_otoks * tok_ratio))}", fmt_tokens(total_tokens))
    w_cost_cell = Text.from_markup(f"[{ACCENT}]~{fmt_cost(w_cost)}[/{ACCENT}]{w_delta_rich}")
    m_cost_cell = Text.from_markup(f"[{ACCENT}]~{fmt_cost(m_cost)}[/{ACCENT}]{m_delta_rich}")
    recent.add_row("Est. Cost", Text.from_markup(f"[{ACCENT}]~{fmt_cost(t_cost)}[/{ACCENT}]"), w_cost_cell, m_cost_cell, Text.from_markup(fmt_cost_styled(total_cost)))
    # Run-rate projection: extrapolate elapsed-period spend to the full period.
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    proj_week = w_cost / n_week_days * 7 if n_week_days else 0
    proj_month = m_cost / today.day * days_in_month if today.day else 0
    proj = Text.from_markup(
        f"[dim]Pace → week ~{fmt_cost(proj_week)} · month ~{fmt_cost(proj_month)} "
        f"(elapsed-period run-rate)[/dim]"
    )
    console.print(_section(Group(recent, proj), "Recent"))

    # --- 1.5 GROWTH (cumulative tokens + cost over time) ---
    # The running total the snapshots above never show: how the lifetime token and
    # dollar figures accumulated, one calendar month per row. Tokens are spread by
    # daily-output share (the same tok_ratio Recent uses — no per-day input/cache
    # breakdown exists), so they reconcile to the lifetime total; cost is the REAL
    # per-day DayActivity.cost sum, so the two series diverge where the model mix
    # shifts. A global cost_scale (≈1.0) folds back any per-day attribution gap
    # (Claude stats-cache-only days, Grok undated sessions) so Σ hits the headline.
    if combined.daily:
        months: dict[tuple[int, int], list] = {}  # (year, month) -> [output_tokens, cost]
        for da in combined.daily:
            b = months.setdefault((da.day.year, da.day.month), [0, 0.0])
            b[0] += da.output_tokens
            b[1] += da.cost
        total_daily_out = sum(b[0] for b in months.values())
        total_daily_cost = sum(b[1] for b in months.values())
        # Spread the EXACT lifetime totals across months by each month's share, so
        # the monthly deltas sum to the final Σ AND the final Σ equals the headline.
        tok_scale = total_tokens / total_daily_out if total_daily_out else 0
        cost_scale = total_cost / total_daily_cost if total_daily_cost else 0
        ordered = sorted(months.items())

        gr_rows = []
        cum_out = 0
        cum_cost = 0.0
        for i, ((y, m), (mout, mcost)) in enumerate(ordered):
            cum_out += mout
            cum_cost += mcost
            last = i == len(ordered) - 1
            cum_cost_s = cum_cost * cost_scale
            gr_rows.append({
                "month": f"{calendar.month_abbr[m]} {y}",
                "dtok": f"~{fmt_tokens(int(mout * tok_scale))}",
                "stok": fmt_tokens(total_tokens) if last else f"~{fmt_tokens(int(cum_out * tok_scale))}",
                "dcost": fmt_cost(mcost * cost_scale),
                "scost": fmt_cost(total_cost) if last else fmt_cost(cum_cost_s),
                "share": (cum_cost_s / total_cost) if total_cost else 0,
            })

        def _gw(key, header):
            return max(len(header), max(len(r[key]) for r in gr_rows))
        month_w, dtok_w, stok_w, dcost_w, scost_w = (
            _gw("month", "Month"), _gw("dtok", "+Tokens"), _gw("stok", "Σ Tokens"),
            _gw("dcost", "+Cost"), _gw("scost", "Σ Cost"),
        )
        # Fill the panel: inner − 5 text cols − their pads − " 100%" suffix − safety
        # margin (rich's SIMPLE_HEAD width rounding truncates columns if too tight).
        GBAR = max(10, (width - 4) - month_w - dtok_w - stok_w - dcost_w - scost_w - 18)

        growth_tbl = Table(box=box.SIMPLE_HEAD, show_edge=False, padding=(0, 1, 0, 0), header_style="dim", expand=False)
        growth_tbl.add_column("Month", style="bold", no_wrap=True)
        growth_tbl.add_column("+Tokens", justify="right", no_wrap=True)
        growth_tbl.add_column("Σ Tokens", justify="right", no_wrap=True)
        growth_tbl.add_column("+Cost", justify="right", no_wrap=True)
        growth_tbl.add_column("Σ Cost", justify="right", no_wrap=True)
        growth_tbl.add_column("Cumulative $", no_wrap=True)
        for r in gr_rows:
            filled = int(round(r["share"] * GBAR))
            bar = f"[#5fd7af]{'█' * filled}[/#5fd7af][bright_black]{'░' * (GBAR - filled)}[/bright_black]"
            growth_tbl.add_row(
                r["month"], r["dtok"], r["stok"],
                Text.from_markup(f"[{ACCENT}]{r['dcost']}[/{ACCENT}]"),
                Text.from_markup(f"[{ACCENT}]{r['scost']}[/{ACCENT}]"),
                Text.from_markup(f"{bar} [dim]{r['share'] * 100:.0f}%[/dim]"),
            )
        since = first_date.isoformat() if first_date else "start"
        gfoot = Text(f"cumulative since {since} · tokens ~spread by daily output · bar = Σ cost share", style="dim")
        console.print(_section(Group(growth_tbl, gfoot), "Growth"))

    # --- 2. COST ---
    cost_total = agg_cb.total_cost
    total_input = combined.total_tokens.input_tokens
    total_cache_read = combined.total_tokens.cache_read_tokens
    cache_denom = total_cache_read + total_input
    cache_hit_rate = total_cache_read / cache_denom * 100 if cache_denom > 0 else 0
    cache_savings = 0.0
    if agg_cb.cache_read_cost > 0:
        cache_savings = agg_cb.cache_read_cost * 10 - agg_cb.cache_read_cost

    BAR_WIDTH = 12
    cost_bar_table = Table(box=None, show_header=False, show_edge=False, padding=(0, 1, 0, 0), expand=True)
    cost_bar_table.add_column("category", style="bold", no_wrap=True)
    cost_bar_table.add_column("amount", justify="right", no_wrap=True)
    cost_bar_table.add_column("bar", no_wrap=True)
    cost_bar_table.add_column("pct", justify="right", style="dim", no_wrap=True)
    for label, amount in [
        ("Cache Rd", agg_cb.cache_read_cost),
        ("Cache Wr", agg_cb.cache_write_cost),
        ("Input", agg_cb.input_cost),
        ("Output", agg_cb.output_cost),
    ]:
        pct = amount / cost_total if cost_total > 0 else 0
        filled = int(pct * BAR_WIDTH)
        color = COST_COLORS[label]
        bar_str = f"[{color}]{'\u2588' * filled}[/{color}][bright_black]{'\u2591' * (BAR_WIDTH - filled)}[/bright_black]"
        short_cost = f"${amount:,.0f}"
        cost_bar_table.add_row(label, short_cost, Text.from_markup(bar_str), fmt_pct(amount, cost_total))

    cost_summary = Table(box=None, show_header=False, show_edge=False, padding=(0, 1, 0, 0), expand=True)
    cost_summary.add_column("key", style="bold", no_wrap=True)
    cost_summary.add_column("value", no_wrap=True, ratio=1)
    cache_color = "green" if cache_hit_rate > 90 else ("yellow" if cache_hit_rate > 70 else "default")
    cost_summary.add_row("Cache Hit", Text.from_markup(f"[{cache_color}]{cache_hit_rate:.1f}%[/{cache_color}] saved [{ACCENT}]~{fmt_cost(cache_savings)}[/{ACCENT}]"))
    # Cache-write TTL split (1h costs 2x input, 5m 1.25x) + write:read ratio.
    # Both read straight off the already-parsed token breakdown.
    cw_total = combined.total_tokens.cache_write_tokens
    cw_1h = combined.total_tokens.cache_write_1h_tokens
    if cw_total > 0:
        h_pct = cw_1h / cw_total * 100
        wr = cw_total / total_cache_read if total_cache_read else 0
        cost_summary.add_row("Cache TTL", f"1h {h_pct:.0f}% \u00b7 5m {100 - h_pct:.0f}% \u00b7 W:R {wr:.2f}")
    cost_per_day = fmt_cost_styled(total_cost / days_active) if days_active else "\u2014"
    cost_per_session = fmt_cost_styled(total_cost / total_sessions) if total_sessions else "\u2014"
    cost_summary.add_row("Cost/Day", Text.from_markup(cost_per_day) if days_active else cost_per_day)
    cost_summary.add_row("Cost/Sess", Text.from_markup(cost_per_session) if total_sessions else cost_per_session)
    # Session-cost distribution: the average above hides the shape. The top-1%-of-
    # sessions share is the concentration headline \u2014 is spend a few runaway sessions
    # or evenly spread? (Claude entries are per-transcript; Codex per session.)
    sc = sorted(combined.session_costs)
    if sc:
        sc_total = sum(sc)
        k = max(1, int(0.01 * len(sc)))  # top 1% of sessions, at least one
        top1_share = sum(sc[-k:]) / sc_total * 100 if sc_total else 0
        cost_summary.add_row(
            "Sess Dist",
            f"[{ACCENT}]top 1% = {top1_share:.0f}%[/{ACCENT}] of spend",
        )
    cost_per_msg = fmt_cost_styled(total_cost / total_messages) if total_messages else "\u2014"
    cost_summary.add_row("Cost/Msg", Text.from_markup(cost_per_msg) if total_messages else cost_per_msg)
    cost_per_ktok = f"[{ACCENT}]${total_cost / total_tokens * 1000:.4f}[/{ACCENT}]" if total_tokens else "\u2014"
    cost_summary.add_row("Cost/1K Tok", Text.from_markup(cost_per_ktok) if total_tokens else cost_per_ktok)
    # Reasoning-token share (Codex-only; reasoning is a subset of output tokens
    # billed at the output rate but never user-visible). Gated on >0.
    reasoning_toks = combined.total_tokens.reasoning_tokens
    if reasoning_toks > 0 and total_output_tokens > 0:
        r_pct = reasoning_toks / total_output_tokens * 100
        cost_summary.add_row("Reasoning", f"{fmt_tokens(reasoning_toks)} \u00b7 {r_pct:.0f}% of output [grey50](Codex)[/grey50]")
    cost_body = Group(cost_bar_table, Text(""), cost_summary)

    # --- 4. MODELS ---
    if merged_models:
        models_table = Table(box=box.HORIZONTALS, border_style=BORDER, show_edge=False, padding=(0, 1), expand=True)
        models_table.add_column("Model", style="bold", no_wrap=True, ratio=1, min_width=18)
        models_table.add_column("Total", justify="right", no_wrap=True, min_width=6)
        models_table.add_column("Input", justify="right", no_wrap=True, min_width=6)
        models_table.add_column("Output", justify="right", no_wrap=True, min_width=6)
        models_table.add_column("Cache", justify="right", no_wrap=True, min_width=6)
        models_table.add_column("Cost", justify="right", no_wrap=True, min_width=10)
        models_table.add_column("In $/M", justify="right", no_wrap=True, min_width=7)
        models_table.add_column("Out $/M", justify="right", no_wrap=True, min_width=7)
        models_table.add_column("%", justify="right", style="dim", no_wrap=True, min_width=5)

        # Family rollups (grouping + weighted-price math) are computed by the
        # pure _family_summaries helper; this loop only renders. The grand
        # weighted In/Out $/M sum the per-family numerators/denominators, so the
        # Total row stays consistent with the rollups above it.
        families = _family_summaries(merged_models)
        w_in_num = w_in_den = w_out_num = w_out_den = 0.0
        for fi, fam in enumerate(families):
            if fi > 0:
                models_table.add_section()
            w_in_num += fam.in_num
            w_in_den += fam.in_den
            w_out_num += fam.out_num
            w_out_den += fam.out_den
            f_cache = fam.tb.cache_read_tokens + fam.tb.cache_write_tokens
            # Single-member families collapse to one row but keep the exact
            # version visible (dimmed) so a lone "Others"/"Claude Haiku" stays
            # identifiable. Built as Text so a stray '[' in a model id can't be
            # read as rich markup.
            if len(fam.members) == 1:
                label = Text(fam.name)
                label.append(f"  {fam.members[0].name}", style="dim")
            else:
                label = fam.name
            models_table.add_row(
                label, fmt_tokens(fam.tb.total), fmt_tokens(fam.tb.input_tokens),
                fmt_tokens(fam.tb.output_tokens), fmt_tokens(f_cache),
                Text.from_markup(fmt_cost_styled(fam.cost)),
                _fmt_weighted(fam.in_num, fam.in_den),
                _fmt_weighted(fam.out_num, fam.out_den),
                fmt_pct(fam.cost, total_cost), style="bold",
            )
            if len(fam.members) > 1:
                for mem in fam.members:
                    cache = mem.tb.cache_read_tokens + mem.tb.cache_write_tokens
                    in_str = f"${mem.in_p:.2f}" if mem.in_p is not None else "\u2014"
                    out_str = f"${mem.out_p:.2f}" if mem.out_p is not None else "\u2014"
                    models_table.add_row(
                        Text(f"  {mem.name}"), fmt_tokens(mem.tb.total),
                        fmt_tokens(mem.tb.input_tokens), fmt_tokens(mem.tb.output_tokens),
                        fmt_tokens(cache), Text.from_markup(fmt_cost_styled(mem.cost)),
                        in_str, out_str, fmt_pct(mem.cost, total_cost), style="dim",
                    )
        t_total = sum(tb.total for tb, _ in merged_models.values())
        t_in = sum(tb.input_tokens for tb, _ in merged_models.values())
        t_out = sum(tb.output_tokens for tb, _ in merged_models.values())
        t_cache = sum(tb.cache_read_tokens + tb.cache_write_tokens for tb, _ in merged_models.values())
        t_cost_sum = sum(c for _, c in merged_models.values())
        models_table.add_section()
        models_table.add_row("[bold]Total[/bold]", fmt_tokens(t_total), fmt_tokens(t_in), fmt_tokens(t_out), fmt_tokens(t_cache), Text.from_markup(fmt_cost_styled(t_cost_sum)), _fmt_weighted(w_in_num, w_in_den), _fmt_weighted(w_out_num, w_out_den), "", style="bold")
        if combined.unpriced_models:
            names = ", ".join(sorted(combined.unpriced_models))
            models_table.add_row(Text(f"Unpriced: {names} ({fmt_tokens(combined.unpriced_tokens)})", style="yellow"), "", "", "", "", "", "", "", "")
        footnote = Text("In/Out $/M = token-weighted list price (family rows blend versions) \u00b7 % = share of total cost", style="dim")
        model_extras = [Text(""), footnote]
        # Model-adoption timeline: the most recently first-seen models, so a cost
        # jump can be traced to "started using model X on date Y".
        if combined.model_first_seen:
            newest = sorted(combined.model_first_seen.items(), key=lambda kv: kv[1], reverse=True)[:3]
            parts = []
            for m, d in newest:
                try:
                    lbl = date.fromisoformat(d).strftime("%b %d")
                except ValueError:
                    lbl = d
                parts.append(f"{m} ({lbl})")
            model_extras.insert(1, Text("Newest: " + " \u00b7 ".join(parts), style="dim"))
        console.print(_section(Group(models_table, *model_extras), "Models"))

    # --- 3. ACTIVITY ---
    SPARK_BLOCKS = "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"

    def _sparkline(vals) -> Text:
        """8-level gradient sparkline scaled to the series max (0 \u2192 lowest block)."""
        mx = max(vals) or 1
        t = Text()
        for v in vals:
            idx = min(7, max(0, int(v / mx * 7))) if v > 0 else 0
            t.append(SPARK_BLOCKS[idx], style=SPARK_GRADIENT[idx])
        return t

    act = Table(box=None, show_header=False, show_edge=False, padding=(0, 1, 0, 0), expand=True)
    act.add_column("key", style="bold", no_wrap=True)
    act.add_column("value", no_wrap=True, ratio=1)
    if days_active > 0:
        tokens_day = fmt_tokens(int(total_tokens / days_active))
        act.add_row("Daily Avg", f"{total_messages / days_active:,.0f} msgs · {total_sessions / days_active:,.1f} sess · {tokens_day} tok")
    msg_by_date: dict[date, int] = {d.day: d.messages for d in daily_by_date.values()}
    wd_msgs = sum(v for k, v in msg_by_date.items() if k.weekday() < 5)
    we_msgs = sum(v for k, v in msg_by_date.items() if k.weekday() >= 5)
    wd_days = len([k for k in msg_by_date if k.weekday() < 5])
    we_days = len([k for k in msg_by_date if k.weekday() >= 5])
    if wd_days > 0 and we_days > 0:
        act.add_row("Weekday", f"{wd_msgs / wd_days:,.0f} msgs/day ({wd_days}d)")
        act.add_row("Weekend", f"{we_msgs / we_days:,.0f} msgs/day ({we_days}d)")
    if msg_by_date:
        busiest = max(msg_by_date, key=lambda k: msg_by_date[k])
        act.add_row("Busiest", f"{busiest.isoformat()} ({msg_by_date[busiest]:,})")
    total_hourly_msgs = sum(merged_hours.values())
    peak_h = max(merged_hours, key=lambda h: merged_hours[h])
    if merged_hours[peak_h] > 0:
        act.add_row("Peak Hour", f"{peak_h}:00-{(peak_h + 1) % 24}:00")
    if total_hourly_msgs > 0:
        tok_by_hour = [int(total_tokens * merged_hours.get(h, 0) / total_hourly_msgs) for h in range(24)]
        if any(v > 0 for v in tok_by_hour):
            act.add_row("Tok/Hour", _sparkline(tok_by_hour))
    act.add_row("Streak", f"{streak} day{'s' if streak != 1 else ''}")
    longest_dur = combined.longest_session_duration_ms
    longest_msgs = combined.longest_session_messages
    if longest_dur > 0:
        act.add_row("Longest", f"{fmt_duration(longest_dur)} ({longest_msgs:,} msgs)")
    if total_sessions > 0:
        sess_parts = [f"{total_messages / total_sessions:,.1f} msgs"]
        if total_turns > 0:
            sess_parts.append(f"{total_turns / total_sessions:,.1f} turns")

        act.add_row("Avg Sess", " · ".join(sess_parts))

    density_parts = []
    if total_messages > 0:
        density_parts.append(f"{total_tool_calls / total_messages:,.1f} tools/msg")
    if total_turns > 0:
        density_parts.append(f"{total_messages / total_turns:,.1f} msg/turn")
    if density_parts:
        act.add_row("Density", " · ".join(density_parts))

    # Stop-reason distribution (Claude): how assistant turns ended. A high
    # max_tokens/refusal share is a friction signal; tool_use dominates agentic work.
    if combined.stop_reasons:
        sr_total = sum(combined.stop_reasons.values())
        sr_sorted = sorted(combined.stop_reasons.items(), key=lambda kv: kv[1], reverse=True)
        # Abbreviate the long stop-reason labels so the row fits the half-width
        # Activity panel instead of truncating mid-word ("stop_s…").
        _SR_ABBR = {"tool_use": "tool", "end_turn": "end", "stop_sequence": "seq",
                    "max_tokens": "max", "pause_turn": "pause", "refusal": "refuse"}
        sr_parts = [f"{_SR_ABBR.get(r, r)} {c / sr_total * 100:.0f}%" for r, c in sr_sorted[:3]]
        act.add_row("Stops", " · ".join(sr_parts))

    # Sparkline for last 14 days
    last_14_values: list[int] = []
    last_14_dates: list[date] = []
    for i in range(13, -1, -1):
        d = today - timedelta(days=i)
        da = daily_by_date.get(d)
        last_14_values.append(da.output_tokens if da else 0)
        last_14_dates.append(d)
    if any(v > 0 for v in last_14_values):
        spark_14 = _sparkline(last_14_values)
        # Momentum: last 7 days vs the prior 7 days, as a colored arrow \u2014 the new
        # signal a flat sparkline can't convey at a glance.
        prior7, recent7 = sum(last_14_values[:7]), sum(last_14_values[7:])
        if prior7 > 0:
            tpct = (recent7 - prior7) / prior7 * 100
            if tpct >= 0:
                spark_14.append(f" \u2191{tpct:.0f}%/wk", style="#5fd787")
            else:
                spark_14.append(f" \u2193{abs(tpct):.0f}%/wk", style="#d75f5f")
        act.add_row("Last 14d", spark_14)

    # Tool-call sparkline (last 14d) \u2014 a genuinely different shape from output
    # tokens (a spike in tool calls often precedes a cost spike).
    last_14_tools: list[int] = []
    for i in range(13, -1, -1):
        da = daily_by_date.get(today - timedelta(days=i))
        last_14_tools.append(da.tool_calls if da else 0)
    if any(t > 0 for t in last_14_tools):
        spark_t = _sparkline(last_14_tools)
        spark_t.append(f" {sum(last_14_tools):,} calls")
        act.add_row("Tools 14d", spark_t)

    # Codex 5-Hour rate-limit utilization (last 14d): are you chronically redlining
    # the cap, or is the latest gauge just a transient spike?
    if combined.rate_limit_history:
        rl_vals = [combined.rate_limit_history.get((today - timedelta(days=i)).isoformat(), 0.0)
                   for i in range(13, -1, -1)]
        if any(v > 0 for v in rl_vals):
            spark_rl = _sparkline(rl_vals)
            spark_rl.append(f" peak {max(rl_vals):.0f}%")
            act.add_row("RL 5h", spark_rl)

    # --- Cost + Activity side by side, height-balanced (stacks below PAIR_MIN_WIDTH) ---
    _pair(cost_body, "Cost", act, "Activity")

    # --- 4b. AGENTS (per-CLI comparison) ---
    # Each tool's own ToolStats is still separate before the combined merge, so a
    # side-by-side "which CLI am I living in / which is cheaper per session" view
    # is a pure display aggregation. Grok cache-hit is — by construction — N/A.
    if len(stats_list) > 1:
        agents_table = Table(box=box.SIMPLE_HEAD, show_edge=False, padding=(0, 1), expand=True)
        agents_table.add_column("Agent", style="bold", no_wrap=True)
        agents_table.add_column("Cost", justify="right", no_wrap=True)
        agents_table.add_column("%", justify="right", style="dim", no_wrap=True)
        agents_table.add_column("Sessions", justify="right", no_wrap=True)
        agents_table.add_column("$/Sess", justify="right", no_wrap=True)
        agents_table.add_column("Messages", justify="right", no_wrap=True)
        agents_table.add_column("Tokens", justify="right", no_wrap=True)
        agents_table.add_column("Cache Hit", justify="right", no_wrap=True)
        for s in sorted(stats_list, key=lambda s: s.total_cost, reverse=True):
            tname = TOOL_NAMES.get(s.source, s.source.title())
            cps = fmt_cost_styled(s.total_cost / s.total_sessions) if s.total_sessions else "—"
            cr = s.total_tokens.cache_read_tokens
            denom = cr + s.total_tokens.input_tokens
            chit = f"{cr / denom * 100:.0f}%" if denom > 0 and cr > 0 else "—"
            agents_table.add_row(
                Text(tname, style=TOOL_COLORS.get(s.source, "grey62")),
                Text.from_markup(fmt_cost_styled(s.total_cost)),
                fmt_pct(s.total_cost, total_cost),
                f"{s.total_sessions:,}",
                Text.from_markup(cps) if s.total_sessions else cps,
                f"{s.total_messages:,}",
                fmt_tokens(s.total_tokens.total),
                chit,
            )
        console.print(_section(agents_table, "Agents"))

    # --- 5. TOOLS + HEATMAP, paired side-by-side (Tools wider; stacks when narrow) ---
    # Per-tool-name call counts (Claude + Codex; Grok records only per-session tool
    # presence, not call counts, so it is excluded to keep this a true call-count
    # breakdown). Denominator is the sum of these counts, not total_tool_calls
    # (which includes Grok), so the percentages are internally consistent.
    tools_body = None
    if combined.tool_calls_by_name:
        tools_sorted = sorted(combined.tool_calls_by_name.items(), key=lambda kv: (-kv[1], kv[0]))
        tool_total = sum(c for _, c in tools_sorted)
        TOP_N = 8
        rows = tools_sorted[:TOP_N]
        other = sum(c for _, c in tools_sorted[TOP_N:])
        if other > 0:
            rows = rows + [("other", other)]
        # Tag each tool with the agent framework that emits it (Claude vs Codex),
        # read from the per-agent maps before they were summed into `combined`. The
        # tail "other" row unions the frameworks of the tools it rolls up.
        tool_src: dict[str, set[str]] = {}
        for st, key in ((claude, "claude"), (codex, "codex")):
            if st:
                for nm in st.tool_calls_by_name:
                    tool_src.setdefault(nm, set()).add(key)
        tail_src = set().union(*(tool_src.get(n, set()) for n, _ in tools_sorted[TOP_N:])) if other > 0 else set()

        def _src_tag(name: str) -> Text:
            fws = tail_src if name == "other" else tool_src.get(name, set())
            if len(fws) == 1:
                k = next(iter(fws))
                return Text(TOOL_NAMES[k], style=TOOL_COLORS[k])
            return Text("both" if fws else "—", style="dim")

        # The bar is the panel's primary signal — a fixed 16-cell bar under-resolved
        # the distribution (everything > 6% maxed out) AND left dead space. Size it to
        # FILL the panel: budget = inner width − src − name − count − pct − pads. The
        # panel is the ratio-3 half of the 3:2 pair (or full width when stacked <
        # PAIR_MIN), estimated from `width`; a 1-col safety margin keeps it from
        # overflowing if rich's ratio rounding differs. Name truncates to 18.
        names = [_trunc(n, 18) for n, _ in rows]
        counts = [f"{c:,}" for _, c in rows]
        src_tags = [_src_tag(n) for n, _ in rows]
        name_w = max((len(n) for n in names), default=4)
        count_w = max((len(c) for c in counts), default=1)
        src_w = max((len(t.plain) for t in src_tags), default=1)
        tools_inner = (round(width * 3 / 5) if width >= PAIR_MIN_WIDTH else width) - 4
        TBAR = max(10, tools_inner - src_w - name_w - count_w - 6 - 5)  # 6=pct ("100.0%"), 5=pads+safety
        tools_table = Table(box=None, show_header=False, show_edge=False, padding=(0, 1, 0, 0), expand=False)
        tools_table.add_column("src", no_wrap=True)
        tools_table.add_column("tool", style="bold", no_wrap=True)
        tools_table.add_column("count", justify="right", no_wrap=True)
        tools_table.add_column("bar", no_wrap=True)
        tools_table.add_column("pct", justify="right", style="dim", no_wrap=True)
        for (name, cnt), disp_name, tag in zip(rows, names, src_tags):
            pct = cnt / tool_total if tool_total else 0
            filled = int(pct * TBAR)
            bar = f"[#d7afd7]{'█' * filled}[/#d7afd7][bright_black]{'░' * (TBAR - filled)}[/bright_black]"
            tools_table.add_row(tag, disp_name, f"{cnt:,}", Text.from_markup(bar), fmt_pct(cnt, tool_total))
        footnote = Text("by agent · % of named calls", style="dim")
        tools_body = Group(tools_table, footnote)

    # Weekday × hour message activity, local time.
    heat_body = None
    if any(combined.heatmap):
        DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        hmax = max(combined.heatmap) or 1
        # Shade ramp gives contrast even where color is unavailable; the gradient
        # color reinforces it where it is.
        HEAT_CHARS = " ░░▒▒▓▓█"

        def _heat_cells(values, vmax) -> Text:
            # sqrt scale: a peaked distribution otherwise flattens every non-peak
            # cell to the lightest shade, hiding the focus windows.
            t = Text()
            for v in values:
                if v <= 0:
                    t.append("·", style="grey23")
                else:
                    idx = min(7, max(1, int((v / vmax) ** 0.5 * 7)))
                    t.append(HEAT_CHARS[idx], style=SPARK_GRADIENT[idx])
            return t

        heat_tbl = Table(box=None, show_header=False, show_edge=False, padding=(0, 1, 0, 0), expand=False)
        heat_tbl.add_column("d", style="bold", no_wrap=True)
        heat_tbl.add_column("cells", no_wrap=True)
        heat_tbl.add_column("tot", justify="right", style="dim", no_wrap=True)  # per-row magnitude
        ruler = Text()
        for h in range(24):
            ruler.append("┊" if h % 6 == 0 else " ", style="grey42")
        heat_tbl.add_row(Text(""), ruler, Text(""))
        for wd in range(7):
            day = combined.heatmap[wd * 24:wd * 24 + 24]
            heat_tbl.add_row(DOW[wd], _heat_cells(day, hmax), fmt_tokens(sum(day)))
        # Σ row: the hour-of-day marginal (column sums of the grid above), on its own
        # scale so the 7×-larger totals don't saturate. Adds the "which hours overall"
        # read the per-day grid can't give, and lifts the body to match Tools' height
        # so the pairing helper's blank padding line disappears.
        hour_marg = [sum(combined.heatmap[wd * 24 + h] for wd in range(7)) for h in range(24)]
        heat_tbl.add_row(Text("Σ", style="bold"), _heat_cells(hour_marg, max(hour_marg) or 1), fmt_tokens(sum(hour_marg)))
        # Hour axis: place the labels directly under their ┊ ticks instead of a legend.
        hour_lbl = [" "] * 24
        for h in range(0, 24, 6):
            for j, ch in enumerate(str(h)):
                if h + j < 24:
                    hour_lbl[h + j] = ch
        heat_tbl.add_row(Text(""), Text("".join(hour_lbl), style="grey42"), Text(""))
        heat_body = heat_tbl

    # Tools needs the width (long names + bars); the heatmap is a fixed 24-col grid,
    # so a 3:2 split fits both. If only one has data, render it full-width.
    if tools_body is not None and heat_body is not None:
        _pair(tools_body, "Tools", heat_body, "Heatmap", ratio=(3, 2))
    elif tools_body is not None:
        console.print(_section(tools_body, "Tools"))
    elif heat_body is not None:
        console.print(_section(heat_body, "Heatmap"))

    # --- 6. MACHINES ---
    if per_machine and len(per_machine) > 1:
        machine_rows = []
        for name, (mc, mx, mg) in per_machine.items():
            m_stats = [s for s in (mc, mx, mg) if s is not None]
            m_tokens = sum(s.total_tokens.total for s in m_stats)
            m_sessions = sum(s.total_sessions for s in m_stats)
            m_messages = sum(s.total_messages for s in m_stats)
            m_cost = sum(s.total_cost for s in m_stats)
            machine_rows.append((name, m_cost, m_sessions, m_messages, m_tokens))
        machine_rows.sort(key=lambda r: r[1], reverse=True)

        machines_table = Table(box=box.SIMPLE_HEAD, show_edge=False, padding=(0, 1), expand=True)
        machines_table.add_column("Machine", style="bold", no_wrap=True)
        machines_table.add_column("Cost", justify="right", no_wrap=True)
        machines_table.add_column("%", justify="right", style="dim", no_wrap=True)
        machines_table.add_column("Sessions", justify="right", no_wrap=True)
        machines_table.add_column("$/Sess", justify="right", no_wrap=True)
        machines_table.add_column("Messages", justify="right", no_wrap=True)
        machines_table.add_column("Tokens", justify="right", no_wrap=True)
        for name, m_cost, m_sessions, m_messages, m_tokens in machine_rows:
            cps = fmt_cost_styled(m_cost / m_sessions) if m_sessions > 0 else "\u2014"
            pct = fmt_pct(m_cost, total_cost)
            machines_table.add_row(name, Text.from_markup(fmt_cost_styled(m_cost)), pct, f"{m_sessions:,}", Text.from_markup(cps) if m_sessions > 0 else cps, f"{m_messages:,}", fmt_tokens(m_tokens))
        console.print(_section(machines_table, "Machines"))

    # --- 7. PROJECTS ---
    if projects_by_name:
        sorted_projects = sorted(projects_by_name.values(), key=lambda p: p.cost, reverse=True)[:10]
        proj_table = Table(box=box.SIMPLE_HEAD, show_edge=False, padding=(0, 1), expand=True)
        proj_table.add_column("Project", style="bold", no_wrap=True)
        proj_table.add_column("Cost", justify="right", no_wrap=True)
        proj_table.add_column("Output Tokens", justify="right", no_wrap=True)
        proj_table.add_column("Duration", justify="right", no_wrap=True)
        proj_table.add_column("$/Hour", justify="right", no_wrap=True)
        for p in sorted_projects:
            name = _trunc(p.path, 30)
            dur = fmt_duration(p.duration_ms) if p.duration_ms > 0 else "—"
            cph = fmt_cost_styled(p.cost / (p.duration_ms / 3_600_000)) if p.duration_ms > 0 else "—"
            proj_table.add_row(name, Text.from_markup(fmt_cost_styled(p.cost)), fmt_tokens(p.output_tokens), dur, Text.from_markup(cph) if p.duration_ms > 0 else cph)
        # Concentration footer over ALL projects (not just the top 10): top-3 cost
        # share + Herfindahl index, so a power user sees whether spend is one repo
        # or spread thin. HHI ranges 1/n (even) → 1 (single project).
        all_costs = sorted((p.cost for p in projects_by_name.values() if p.cost > 0), reverse=True)
        proj_total = sum(all_costs)
        if proj_total > 0 and len(all_costs) > 1:
            top3_share = sum(all_costs[:3]) / proj_total * 100
            hhi = sum((c / proj_total) ** 2 for c in all_costs)
            conc = Text(f"{len(all_costs)} projects · top 3 = {top3_share:.0f}% of cost · concentration {hhi:.2f}", style="dim")
            console.print(_section(Group(proj_table, conc), "Projects (Top 10)"))
        else:
            console.print(_section(proj_table, "Projects (Top 10)"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description="Claude Code and Codex usage statistics")
    ap.add_argument(
        "machines", nargs="*", default=["all"],
        help="machines to include (default: all). Use 'local' for local only, or list specific remote names.",
    )
    ap.add_argument(
        "--sync", action="store_true",
        help=f"force a remote sync (otherwise auto-runs only when cache > {STALE_HOURS}h old)",
    )
    ap.add_argument(
        "--rebaseline", action="store_true",
        help="accept the current (possibly lower) totals as the new lifetime floor",
    )
    args = ap.parse_args()

    claude, codex, grok, per_machine = load_all(args.machines, sync=args.sync)
    stats_list = [s for s in (claude, codex, grok) if s is not None]
    if not stats_list:
        print("No usage data found.")
        sys.exit(1)
    # The floor (lifetime high-water) applies to the FULL scope — local + every
    # remote — however requested (["all"], or local + all hosts named explicitly).
    _remotes = set(_load_remote_hosts())
    if args.machines == ["all"]:
        is_full = True
    elif args.machines == ["local"]:
        is_full = not _remotes
    else:
        is_full = _remotes.issubset(set(args.machines))
    if args.rebaseline and not is_full:
        console.print("[yellow]--rebaseline ignored: it applies only to the full all-machines run.[/yellow]")
    print_stats(claude, codex, grok, per_machine, apply_floor=is_full, rebaseline=args.rebaseline and is_full)


if __name__ == "__main__":
    main()
