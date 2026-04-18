"""adb — CLI tool for Claude Code and Codex usage statistics."""

from __future__ import annotations

import argparse
import json
import re
import sys
from copy import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

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
from parser.types import CostBreakdown, DayActivity, ProjectInfo, TokenBreakdown, ToolStats

REPO_DIR = Path(__file__).resolve().parent
REMOTE_CACHE = REPO_DIR / ".cache" / "remotes"
REMOTES_CONF = REPO_DIR / "remotes.conf"


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


MachineData = dict[str, tuple[ToolStats | None, ToolStats | None]]


def _load_machine(
    name: str,
    claude_kwargs: dict | None,
    codex_kwargs: dict | None,
) -> tuple[str, ToolStats | None, ToolStats | None]:
    """Load stats for a single machine. Designed for parallel execution."""
    c = claude_parser.parse(**claude_kwargs) if claude_kwargs is not None else None
    x = codex_parser.parse(**codex_kwargs) if codex_kwargs is not None else None
    return name, c, x


SYNC_SCRIPT = REPO_DIR / "sync.sh"


def _sync_remotes() -> None:
    """Run sync.sh to pull fresh data from remote machines."""
    if not SYNC_SCRIPT.exists():
        return
    import subprocess

    subprocess.run(
        [str(SYNC_SCRIPT)],
        cwd=str(REPO_DIR),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def load_all(machines: list[str] | None = None, sync: bool = False) -> tuple[ToolStats | None, ToolStats | None, MachineData]:
    """Load and merge stats from local + remote machines.

    machines: list of machine names to include. None or ["all"] means local + all remotes.
              ["local"] means local only. Otherwise, include local + named remotes.
    sync: if True, run sync.sh to pull fresh data from remotes before reading.
    """
    # Build work items: (name, claude_kwargs, codex_kwargs)
    work: list[tuple[str, dict | None, dict | None]] = []

    # Local machine is always included
    work.append(("local", {}, {}))

    # Determine which remotes to include
    all_remotes = _load_remote_hosts()
    if machines is None or machines == ["all"]:
        include_remotes = all_remotes
    elif machines == ["local"]:
        include_remotes = []
    else:
        include_remotes = [h for h in machines if h in all_remotes]

    # Sync remotes if explicitly requested
    if sync and include_remotes:
        _sync_remotes()

    for host in include_remotes:
        base = REMOTE_CACHE / host
        ck = None
        claude_stats = base / "claude" / "stats-cache.json"
        if claude_stats.exists():
            # Prefer recall-sync staging dir (.remote-<host>) over adb cache —
            # it's a superset (never deletes) and avoids double-counting since
            # _iter_project_dirs skips .remote-* dirs in the local parse.
            staging = Path.home() / ".claude" / "projects" / f".remote-{host}"
            projects = staging if staging.is_dir() else base / "claude" / "projects"
            ck = dict(
                stats_path=claude_stats,
                history_path=base / "claude" / "history.jsonl",
                projects_base=projects,
            )
        xk = None
        codex_sessions = base / "codex" / "sessions"
        if codex_sessions.exists():
            xk = dict(sessions_dir=codex_sessions)
        if ck is not None or xk is not None:
            work.append((host, ck, xk))

    # Parse all machines in parallel
    claude_list: list[ToolStats] = []
    codex_list: list[ToolStats] = []
    per_machine: MachineData = {}

    with ThreadPoolExecutor(max_workers=len(work)) as pool:
        futures = [pool.submit(_load_machine, *w) for w in work]
        for f in futures:
            name, c, x = f.result()
            if c:
                claude_list.append(c)
            if x:
                codex_list.append(x)
            if c or x:
                per_machine[name] = (c, x)

    claude = _merge_stats(claude_list) if claude_list else None
    codex = _merge_stats(codex_list) if codex_list else None
    return claude, codex, per_machine


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


def short_project_name(path: str) -> str:
    """Extract repo/directory name from a full path or git URL."""
    # git@github.com:user/repo.git -> repo
    m = re.search(r"/([^/]+?)(?:\.git)?$", path)
    if m:
        return m.group(1)
    # ~/Developer/GitHub/repo -> repo
    parts = path.rstrip("/").split("/")
    return parts[-1] if parts else path


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
    per_machine: MachineData | None = None,
) -> int:
    active_days: set[date] = set()
    for s in (claude, codex):
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
    per_machine: MachineData | None = None,
) -> None:
    stats_list = [s for s in (claude, codex) if s is not None]
    combined = _merge_stats(stats_list)
    total_cost = combined.total_cost
    total_tokens = combined.total_tokens.total
    total_sessions = combined.total_sessions
    total_messages = combined.total_messages
    total_tool_calls = combined.total_tool_calls
    total_turns = combined.total_turns
    total_output_tokens = combined.total_tokens.output_tokens
    first_date = combined.first_date
    _all_active_days = {d.day for d in combined.daily}
    _all_active_days |= _history_active_days(HISTORY_PATH)
    for _host in _load_remote_hosts():
        _all_active_days |= _history_active_days(REMOTE_CACHE / _host / "claude" / "history.jsonl")
    days_active = len(_all_active_days)
    streak = compute_streak(claude, codex)
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
        "Activity":        "bold deep_sky_blue1",
        "Machines":        "bold #87afaf",
        "Projects (Top 10)": "bold #87d787",
    }

    def fmt_cost_styled(c: float) -> str:
        return f"[{ACCENT}]{fmt_cost(c)}[/{ACCENT}]"

    def _section(content, title, **kwargs):
        tc = SECTION_COLORS.get(title, "bold")
        return Panel(
            content, title=f"[{tc}]{title}[/{tc}]", title_align="left",
            border_style=BORDER, box=box.ROUNDED, padding=(0, 1), width=width, **kwargs,
        )

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

    # Hero line — centered: total cost · tool breakdowns with tiers
    both_tools = claude is not None and codex is not None
    hero_text = Text()
    hero_text.append(fmt_cost(total_cost), style=ACCENT_BOLD)
    if both_tools:
        claude_tier = claude.extra.get("tier", "") if claude else ""
        codex_tier = codex.extra.get("tier", "") if codex else ""
        hero_text.append(" · ", style="grey37")
        hero_text.append("Claude", style=CLAUDE_COLOR)
        hero_text.append(f" {fmt_cost(claude.total_cost)}", style=ACCENT)
        if claude_tier:
            hero_text.append(f" ({claude_tier[:1].upper()}{claude_tier[1:]})", style="grey62")
        hero_text.append(" · ", style="grey37")
        hero_text.append("Codex", style=CODEX_COLOR)
        hero_text.append(f" {fmt_cost(codex.total_cost)}", style=ACCENT)
        if codex_tier:
            hero_text.append(f" ({codex_tier[:1].upper()}{codex_tier[1:]})", style="grey62")
    else:
        s = claude or codex
        tool_name = "Claude" if s.source == "claude" else "Codex"
        tool_color = CLAUDE_COLOR if s.source == "claude" else CODEX_COLOR
        tier = s.extra.get("tier", "")
        hero_text.append(" · ", style="grey37")
        hero_text.append(tool_name, style=tool_color)
        if tier:
            hero_text.append(f" ({tier[:1].upper()}{tier[1:]})", style="grey62")
    hero = hero_text

    # Stats ribbon — left-aligned
    sep = " · "
    ribbon = Text()
    ribbon.append(f"{total_sessions:,} sessions", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{fmt_tokens(total_tokens)} tokens", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{days_active} days", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{fmt_cost(avg_cost)}/day", style=ACCENT)
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{avg_msgs:,.0f} msgs/day", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{avg_sessions:,.0f} sess/day", style="grey62")
    ribbon.append(sep, style="grey37")
    ribbon.append(f"{streak}-day streak", style="grey62")

    # Rate limit gauges
    gauge_lines: list[Text] = []
    rl_sources: list[tuple[str, str, list]] = []
    for s in stats_list:
        if not s.rate_limits:
            continue
        prefix = "Claude" if s.source == "claude" else "Codex"
        color = CLAUDE_COLOR if s.source == "claude" else CODEX_COLOR
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
    console.print(_section(recent, "Recent"))

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
    cost_per_day = fmt_cost_styled(total_cost / days_active) if days_active else "\u2014"
    cost_per_session = fmt_cost_styled(total_cost / total_sessions) if total_sessions else "\u2014"
    cost_summary.add_row("Cost/Day", Text.from_markup(cost_per_day) if days_active else cost_per_day)
    cost_summary.add_row("Cost/Sess", Text.from_markup(cost_per_session) if total_sessions else cost_per_session)
    cost_per_msg = fmt_cost_styled(total_cost / total_messages) if total_messages else "\u2014"
    cost_summary.add_row("Cost/Msg", Text.from_markup(cost_per_msg) if total_messages else cost_per_msg)
    cost_per_ktok = f"[{ACCENT}]${total_cost / total_tokens * 1000:.4f}[/{ACCENT}]" if total_tokens else "\u2014"
    cost_summary.add_row("Cost/1K Tok", Text.from_markup(cost_per_ktok) if total_tokens else cost_per_ktok)
    _cost_tc = SECTION_COLORS.get("Cost", "bold")
    cost_panel = Panel(
        Group(cost_bar_table, Text(""), cost_summary),
        title=f"[{_cost_tc}]Cost[/{_cost_tc}]", title_align="left",
        border_style=BORDER, box=box.ROUNDED, padding=(0, 1),
    )

    # --- 4. MODELS ---
    if merged_models:
        models_table = Table(box=box.SIMPLE_HEAD, show_edge=False, padding=(0, 1), expand=True)
        models_table.add_column("Model", style="bold", no_wrap=True, ratio=1)
        models_table.add_column("Total", justify="right", no_wrap=True, min_width=6)
        models_table.add_column("Input", justify="right", no_wrap=True, min_width=6)
        models_table.add_column("Output", justify="right", no_wrap=True, min_width=6)
        models_table.add_column("Cache", justify="right", no_wrap=True, min_width=6)
        models_table.add_column("Cost", justify="right", no_wrap=True, min_width=10)
        models_table.add_column("In $/M", justify="right", no_wrap=True, min_width=7)
        models_table.add_column("Out $/M", justify="right", no_wrap=True, min_width=7)
        models_table.add_column("%", justify="right", style="dim", no_wrap=True, min_width=5)

        def _model_prices(name: str) -> tuple[float | None, float | None]:
            pk = claude_parser._pkey(name)
            if pk and pk in claude_parser.PRICE:
                p = claude_parser.PRICE[pk]
                return p[0], p[1]
            cp = codex_parser._pricing_for(name)
            if cp:
                return cp.input_usd_per_mtok, cp.output_usd_per_mtok
            return None, None

        w_in_num = w_in_den = w_out_num = w_out_den = 0.0
        for rank, (m, (tb, cost)) in enumerate(sorted(
            merged_models.items(), key=lambda x: x[1][1], reverse=True
        )):
            cache = tb.cache_read_tokens + tb.cache_write_tokens
            pct = fmt_pct(cost, total_cost)
            row_style = "dim" if rank >= 3 else None
            in_p, out_p = _model_prices(m)
            in_str = f"${in_p:.2f}" if in_p is not None else "\u2014"
            out_str = f"${out_p:.2f}" if out_p is not None else "\u2014"
            if in_p is not None:
                in_toks = tb.input_tokens + tb.cache_read_tokens + tb.cache_write_tokens
                w_in_num += in_p * in_toks
                w_in_den += in_toks
            if out_p is not None:
                w_out_num += out_p * tb.output_tokens
                w_out_den += tb.output_tokens
            models_table.add_row(m, fmt_tokens(tb.total), fmt_tokens(tb.input_tokens), fmt_tokens(tb.output_tokens), fmt_tokens(cache), Text.from_markup(fmt_cost_styled(cost)), in_str, out_str, pct, style=row_style)
        t_total = sum(tb.total for tb, _ in merged_models.values())
        t_in = sum(tb.input_tokens for tb, _ in merged_models.values())
        t_out = sum(tb.output_tokens for tb, _ in merged_models.values())
        t_cache = sum(tb.cache_read_tokens + tb.cache_write_tokens for tb, _ in merged_models.values())
        t_cost_sum = sum(c for _, c in merged_models.values())
        models_table.add_section()
        w_in_avg = f"${w_in_num / w_in_den:.2f}" if w_in_den else "\u2014"
        w_out_avg = f"${w_out_num / w_out_den:.2f}" if w_out_den else "\u2014"
        models_table.add_row("[bold]Total[/bold]", fmt_tokens(t_total), fmt_tokens(t_in), fmt_tokens(t_out), fmt_tokens(t_cache), Text.from_markup(fmt_cost_styled(t_cost_sum)), w_in_avg, w_out_avg, "", style="bold")
        if combined.unpriced_models:
            names = ", ".join(sorted(combined.unpriced_models))
            models_table.add_row(Text(f"Unpriced: {names} ({fmt_tokens(combined.unpriced_tokens)})", style="yellow"), "", "", "", "", "", "", "", "")
        console.print(_section(models_table, "Models"))

    # --- 3. ACTIVITY ---
    SPARK_BLOCKS = "\u2581\u2582\u2583\u2584\u2585\u2586\u2587\u2588"
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
        mx_h = max(tok_by_hour)
        if mx_h > 0:
            spark_h = Text()
            for v in tok_by_hour:
                idx = min(7, max(0, int(v / mx_h * 7))) if v > 0 else 0
                spark_h.append(SPARK_BLOCKS[idx], style=SPARK_GRADIENT[idx])
            act.add_row("Tok/Hour", spark_h)
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

    # Sparkline for last 14 days
    last_14_values: list[int] = []
    last_14_dates: list[date] = []
    for i in range(13, -1, -1):
        d = today - timedelta(days=i)
        da = daily_by_date.get(d)
        last_14_values.append(da.output_tokens if da else 0)
        last_14_dates.append(d)
    if any(v > 0 for v in last_14_values):
        mx = max(last_14_values)
        spark_14 = Text()
        for v in last_14_values:
            idx = min(7, max(0, int(v / mx * 7))) if v > 0 else 0
            spark_14.append(SPARK_BLOCKS[idx], style=SPARK_GRADIENT[idx])
        start_lbl = last_14_dates[0].strftime("%b %d")
        end_lbl = last_14_dates[-1].strftime("%b %d")
        spark_14.append(f" {start_lbl} \u2192 {end_lbl}")
        act.add_row("Last 14d", spark_14)

    _act_tc = SECTION_COLORS.get("Activity", "bold")
    activity_panel = Panel(
        act, title=f"[{_act_tc}]Activity[/{_act_tc}]", title_align="left",
        border_style=BORDER, box=box.ROUNDED, padding=(0, 1),
    )

    # --- Cost + Activity side by side ---
    side_by_side = Table(box=None, show_header=False, show_edge=False, padding=0, width=width)
    side_by_side.add_column("left", ratio=1)
    side_by_side.add_column("right", ratio=1)
    side_by_side.add_row(cost_panel, activity_panel)
    console.print(side_by_side)

    # --- 6. MACHINES ---
    if per_machine and len(per_machine) > 1:
        machine_rows = []
        for name, (mc, mx) in per_machine.items():
            m_stats = [s for s in (mc, mx) if s is not None]
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
            name = p.path[:30]
            dur = fmt_duration(p.duration_ms) if p.duration_ms > 0 else "—"
            cph = fmt_cost_styled(p.cost / (p.duration_ms / 3_600_000)) if p.duration_ms > 0 else "—"
            proj_table.add_row(name, Text.from_markup(fmt_cost_styled(p.cost)), fmt_tokens(p.output_tokens), dur, Text.from_markup(cph) if p.duration_ms > 0 else cph)
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
        help="sync remote machine data (via rsync) before reading",
    )
    args = ap.parse_args()

    claude, codex, per_machine = load_all(args.machines, sync=args.sync)
    stats_list = [s for s in (claude, codex) if s is not None]
    if not stats_list:
        print("No usage data found.")
        sys.exit(1)
    print_stats(claude, codex, per_machine)


if __name__ == "__main__":
    main()
