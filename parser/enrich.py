"""Enriched per-(day, model) usage facts for the token dashboard.

ADDITIVE module: it does NOT change adb's CLI, ToolStats, or existing parser
behavior. It reuses the existing parsers' pricing tables, dedup keys, and
parsing conventions so the dashboard's numbers stay consistent with `adb`,
while emitting the richer per-day x per-model breakdown (token components +
cost components) that `ToolStats.to_dict()` aggregates away.

The dashboard is the only consumer. To avoid contending with `adb`'s own
`.cache/` files, every function takes an explicit `cache_dir` and keeps its
per-file `(size, mtime_ns)` caches there.

Returned grain:
  EnrichedStats.cube:     {(day_iso, model): Cell}     # tokens + micro-USD cost
  EnrichedStats.activity: {day_iso: DayCounts}         # messages / sessions / tool_calls
Both Claude and Codex emit this same shape so the dashboard can fold them
uniformly per machine and per tool.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import orjson

from parser.parsers import claude as _claude
from parser.parsers import codex as _codex
from parser.types import RateLimitInfo

# Bump whenever PRICE / CACHE_OVERRIDES / MODEL_PRICING change, so the dashboard
# can annotate that historical costs were computed under a given pricing table.
PRICING_VERSION = "2026-05-29"


# ---------------------------------------------------------------------------
# Common cube cell (normalized tokens + cost components in integer micro-USD)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Cell:
    input: int = 0          # non-cached input tokens
    output: int = 0
    cache_read: int = 0
    cache_write: int = 0
    cache_write_1h: int = 0
    reasoning: int = 0
    # cost components, integer micro-USD (1e-6 USD)
    input_cost: int = 0
    output_cost: int = 0
    cache_read_cost: int = 0
    cache_write_cost: int = 0
    # What the input-side tokens (input + cache_read + cache_write) would cost
    # at the base (uncached) input rate — for exact cache-economics in derive.
    uncached_input_cost: int = 0

    @property
    def total_tokens(self) -> int:
        return (
            self.input + self.output + self.cache_read + self.cache_write + self.reasoning
        )

    @property
    def total_cost(self) -> int:
        return (
            self.input_cost + self.output_cost + self.cache_read_cost + self.cache_write_cost
        )

    def as_dict(self) -> dict:
        return {
            "input": self.input,
            "output": self.output,
            "cache_read": self.cache_read,
            "cache_write": self.cache_write,
            "cache_write_1h": self.cache_write_1h,
            "reasoning": self.reasoning,
            "input_cost": self.input_cost,
            "output_cost": self.output_cost,
            "cache_read_cost": self.cache_read_cost,
            "cache_write_cost": self.cache_write_cost,
            "uncached_input_cost": self.uncached_input_cost,
        }


@dataclass(slots=True)
class DayCounts:
    messages: int = 0
    sessions: int = 0
    tool_calls: int = 0


@dataclass(slots=True)
class EnrichedStats:
    source: str  # "claude" | "codex"
    cube: dict = field(default_factory=dict)       # (day_iso, model) -> Cell
    activity: dict = field(default_factory=dict)   # day_iso -> DayCounts
    rate_limits: list = field(default_factory=list)
    rate_limits_observed_at: str | None = None     # ISO8601 of the observation
    rate_limits_source: str = ""                   # "oauth_live" | "session_log"
    unpriced_models: set = field(default_factory=set)
    unpriced_tokens: int = 0
    tier: str = ""
    pricing_version: str = PRICING_VERSION
    # Lifetime "headline" totals that match `adb` exactly. The cube is
    # session-JSONL-only (authoritative for the time-series the dashboard
    # charts); these floor it against stats-cache.json so the all-time
    # numbers reconcile with `adb` even when old sessions were pruned off disk.
    lifetime_cost_micro: int = 0
    lifetime_tokens: dict = field(default_factory=dict)  # input/output/cache_read/cache_write/reasoning/total
    lifetime_messages: int = 0
    lifetime_sessions: int = 0
    # weekday*24 + hour -> message count, in LOCAL time (activity heatmap)
    heatmap: list = field(default_factory=lambda: [0] * 168)
    # project path -> {input,output,cache_read,cache_write,reasoning,cost_micro,
    #                  lines_added,lines_removed}
    projects: dict = field(default_factory=dict)


def _micro(usd: float) -> int:
    return int(round(usd * 1_000_000))


def _fkey(path: Path) -> str | None:
    try:
        st = path.stat()
    except OSError:
        return None
    return f"{st.st_size}:{st.st_mtime_ns}"


def _cache_load(cache_dir: Path, name: str) -> dict:
    p = cache_dir / name
    if p.exists():
        try:
            return orjson.loads(p.read_bytes())
        except Exception:
            pass
    return {}


def _cache_save(cache_dir: Path, name: str, data: dict) -> None:
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp = cache_dir / (name + ".tmp")
        tmp.write_bytes(orjson.dumps(data))
        tmp.replace(cache_dir / name)  # atomic
    except OSError:
        pass


def _fingerprint(files, extra=()) -> str:
    """count:total_size:max_mtime_ns over all source files. A cheap stat-only
    walk that tells us whether anything changed without reading file contents."""
    count = 0
    total = 0
    mx = 0
    for f in list(files) + list(extra):
        if f is None:
            continue
        try:
            st = f.stat()
        except OSError:
            continue
        count += 1
        total += st.st_size
        if st.st_mtime_ns > mx:
            mx = st.st_mtime_ns
    return f"{count}:{total}:{mx}"


def _es_to_fold(es: EnrichedStats) -> dict:
    """Serialize the *folded* result (tiny: ~hundreds of cube cells) so a
    steady-state refresh never reloads the per-message cache."""
    return {
        "cube": [
            [d, m, c.input, c.output, c.cache_read, c.cache_write, c.cache_write_1h,
             c.reasoning, c.input_cost, c.output_cost, c.cache_read_cost,
             c.cache_write_cost, c.uncached_input_cost]
            for (d, m), c in es.cube.items()
        ],
        "activity": {d: [a.messages, a.sessions, a.tool_calls] for d, a in es.activity.items()},
        "lifetime_tokens": es.lifetime_tokens,
        "lifetime_cost_micro": es.lifetime_cost_micro,
        "lifetime_messages": es.lifetime_messages,
        "lifetime_sessions": es.lifetime_sessions,
        "unpriced_models": list(es.unpriced_models),
        "unpriced_tokens": es.unpriced_tokens,
        "tier": es.tier,
        "rate_limits": [[r.label, r.utilization, r.resets_in] for r in es.rate_limits],
        "rate_limits_observed_at": es.rate_limits_observed_at,
        "rate_limits_source": es.rate_limits_source,
        "heatmap": es.heatmap,
        "projects": es.projects,
    }


def _fold_to_es(d: dict, source: str) -> EnrichedStats:
    es = EnrichedStats(source=source)
    for row in d["cube"]:
        es.cube[(row[0], row[1])] = Cell(*row[2:])
    es.activity = {day: DayCounts(*v) for day, v in d["activity"].items()}
    es.lifetime_tokens = d.get("lifetime_tokens", {})
    es.lifetime_cost_micro = d.get("lifetime_cost_micro", 0)
    es.lifetime_messages = d.get("lifetime_messages", 0)
    es.lifetime_sessions = d.get("lifetime_sessions", 0)
    es.unpriced_models = set(d.get("unpriced_models", []))
    es.unpriced_tokens = d.get("unpriced_tokens", 0)
    es.tier = d.get("tier", "")
    es.rate_limits = [RateLimitInfo(label=r[0], utilization=r[1], resets_in=r[2]) for r in d.get("rate_limits", [])]
    es.rate_limits_observed_at = d.get("rate_limits_observed_at")
    es.rate_limits_source = d.get("rate_limits_source", "")
    es.heatmap = d.get("heatmap", [0] * 168)
    es.projects = d.get("projects", {})
    return es


def _attach_claude_rl(es: EnrichedStats) -> None:
    """Live OAuth rate limits — fetched fresh (cheap, 10-min disk-cached) even
    on a fold-cache hit, so runway samples keep accruing."""
    cr_data = _claude._get_creds()
    if cr_data:
        es.rate_limits = _claude._fetch_rate_limits(cr_data)
        es.rate_limits_source = "oauth_live"
        es.rate_limits_observed_at = datetime.now(timezone.utc).isoformat()
        es.tier = _claude._get_tier(cr_data)


# ---------------------------------------------------------------------------
# Claude
# ---------------------------------------------------------------------------


def _local_wh(ts) -> int | None:
    """weekday*24 + hour in LOCAL time, from an ISO string or epoch-ms."""
    try:
        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone()
        elif isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(ts / 1000)
        else:
            return None
    except (ValueError, OSError):
        return None
    return dt.weekday() * 24 + dt.hour


def _nlines(s) -> int:
    return s.count("\n") + 1 if isinstance(s, str) and s else 0


_EDIT_TOOLS = ("Edit", "Write", "MultiEdit", "NotebookEdit")


def _count_edit_lines(content) -> tuple[int, int]:
    """Rough lines added/removed from Edit/Write/MultiEdit tool_use blocks."""
    la = lr = 0
    if not isinstance(content, list):
        return 0, 0
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "tool_use":
            continue
        name = block.get("name", "")
        inp = block.get("input")
        if name not in _EDIT_TOOLS or not isinstance(inp, dict):
            continue
        if name == "Write":
            la += _nlines(inp.get("content"))
        elif name == "Edit":
            la += _nlines(inp.get("new_string"))
            lr += _nlines(inp.get("old_string"))
        elif name == "NotebookEdit":
            la += _nlines(inp.get("new_source"))
        elif name == "MultiEdit":
            for e in inp.get("edits") or []:
                if isinstance(e, dict):
                    la += _nlines(e.get("new_string"))
                    lr += _nlines(e.get("old_string"))
    return la, lr


def _claude_file_data(path: Path) -> dict:
    """Single-pass extraction from one Claude session JSONL.

    Returns tok (cost/token rows, messageId:requestId-deduped within file), act
    (activity turns, msg.id-deduped), heat (local weekday×hour message counts),
    cwd (project path), and la/lr (lines added/removed from edit tools).
    """
    tok: list = []
    seen_tok: set = set()
    act_user: list = []
    asst_by_mid: dict = {}  # mid -> [day, tool_count, otoks, terminal]
    heat: dict = {}         # wh -> count
    cwd: str | None = None
    la = lr = 0

    try:
        with path.open("rb") as f:
            for line in f:
                has_ts = b'"timestamp"' in line
                has_usage = b'"usage"' in line
                if not has_ts:
                    if cwd is None and b'"cwd"' in line:
                        try:
                            cwd = orjson.loads(line).get("cwd") or cwd
                        except Exception:
                            pass
                    continue
                try:
                    obj = orjson.loads(line)
                except (orjson.JSONDecodeError, ValueError):
                    continue
                ts = obj.get("timestamp")
                if not ts:
                    continue
                if isinstance(ts, str):
                    day = ts[:10]
                elif isinstance(ts, (int, float)):
                    day = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime(
                        "%Y-%m-%d"
                    )
                else:
                    continue
                if cwd is None:
                    cwd = obj.get("cwd") or None
                msg = obj.get("message")
                if not msg:
                    continue
                role = msg.get("role")

                # --- activity path + heatmap ---
                if role == "user":
                    act_user.append([day, "u", "", 0, 0])
                    wh = _local_wh(ts)
                    if wh is not None:
                        heat[str(wh)] = heat.get(str(wh), 0) + 1
                elif role == "assistant":
                    mid = msg.get("id", "") or ""
                    if mid:
                        content = msg.get("content", [])
                        tool_count = 0
                        if isinstance(content, list):
                            tool_count = sum(
                                1
                                for c in content
                                if isinstance(c, dict) and c.get("type") == "tool_use"
                            )
                        usage = msg.get("usage") or {}
                        otoks = usage.get("output_tokens", 0) or 0
                        terminal = msg.get("stop_reason") in _claude._TERMINAL_STOP_REASONS
                        cur = asst_by_mid.get(mid)
                        if cur is None:
                            asst_by_mid[mid] = [day, tool_count, otoks, terminal]
                            # heatmap + edit-line counting once per first-seen mid
                            wh = _local_wh(ts)
                            if wh is not None:
                                heat[str(wh)] = heat.get(str(wh), 0) + 1
                            ela, elr = _count_edit_lines(content)
                            la += ela
                            lr += elr
                        else:
                            cur_otoks, cur_terminal = cur[2], cur[3]
                            if (terminal and not cur_terminal) or (
                                terminal == cur_terminal and otoks > cur_otoks
                            ):
                                asst_by_mid[mid] = [day, tool_count, otoks, terminal]

                # --- token/cost path (claude._parse_file_tokens_loose) ---
                if not has_usage:
                    continue
                u = msg.get("usage")
                if not u:
                    continue
                mid = msg.get("id", "")
                rid = obj.get("requestId", "")
                h = ""
                if mid and rid:
                    h = f"{mid}:{rid}"
                    if h in seen_tok:
                        continue
                    seen_tok.add(h)
                inp = u.get("input_tokens", 0)
                out = u.get("output_tokens", 0)
                cr = u.get("cache_read_input_tokens", 0)
                cw_total, cw_1h = _claude._cw_split(u)
                if inp + out + cr + cw_total == 0:
                    continue
                model = msg.get("model", "") or "unknown"
                if model == "<synthetic>":
                    model = "unknown"
                tok.append([day, model, inp, out, cr, cw_total, cw_1h, h])
    except OSError:
        pass

    act = list(act_user)
    for mid, v in asst_by_mid.items():
        act.append([v[0], "a", mid, v[1], v[2]])  # day, role, mid, tool_count, output
    return {"tok": tok, "act": act, "heat": heat, "cwd": cwd, "la": la, "lr": lr}


def _price_claude_cell(model: str, inp: int, out: int, cr: int, cw_total: int, cw_1h: int):
    """Return (input_cost, output_cost, cr_cost, cw_cost, uncached_input_cost)
    micro-USD, or None if unpriced."""
    pk = _claude._pkey(model)
    if not pk or pk not in _claude.PRICE:
        return None
    p = _claude.PRICE[pk]
    cr_price, cw_5m_price, cw_1h_price = _claude._cache_rates(pk, p[0])
    input_cost = _micro(inp * p[0] / 1e6)
    output_cost = _micro(out * p[1] / 1e6)
    cr_cost = _micro(cr * cr_price / 1e6)
    cw_cost = _micro(_claude._cw_cost(cw_total, cw_1h, cw_5m_price, cw_1h_price))
    uncached = _micro((inp + cr + cw_total) * p[0] / 1e6)
    return input_cost, output_cost, cr_cost, cw_cost, uncached


def _empty_project() -> dict:
    return {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "reasoning": 0,
            "cost_micro": 0, "lines_added": 0, "lines_removed": 0}


def _build_projects_claude(proj_raw: dict, proj_lines: dict) -> dict:
    out: dict = {}
    for (proj, model), v in proj_raw.items():
        inp, out_t, cr, cw, cw1h = v
        p = out.setdefault(proj, _empty_project())
        p["input"] += inp
        p["output"] += out_t
        p["cache_read"] += cr
        p["cache_write"] += cw
        priced = _price_claude_cell(model, inp, out_t, cr, cw, cw1h)
        if priced:
            p["cost_micro"] += sum(priced[:4])
    for proj, (la, lr) in proj_lines.items():
        p = out.setdefault(proj, _empty_project())
        p["lines_added"] += la
        p["lines_removed"] += lr
    return out


def enrich_claude(projects_base, cache_dir: Path, stats_path: Path | None = None) -> EnrichedStats:
    """Build the enriched Claude cube + activity from session JSONL.

    `projects_base` matches claude.parse(): a Path (local) or list of Paths
    (remote rsync cache + recall staging). The per-(day, model) cube is built
    from session JSONL only (authoritative for the charted window). `stats_path`
    (stats-cache.json) is read solely to floor the *lifetime headline* totals so
    they reconcile with `adb` when old sessions have been pruned off disk.
    """
    bases = [projects_base] if isinstance(projects_base, Path) else list(projects_base)
    projects_dirs: list[Path] = []
    seen_resolved: set[str] = set()
    for b in bases:
        for d in _claude._discover_project_dirs(b):
            r = str(d.resolve())
            if r not in seen_resolved:
                seen_resolved.add(r)
                projects_dirs.append(d)

    key = hashlib.md5(":".join(sorted(str(d) for d in projects_dirs)).encode()).hexdigest()[:12]

    files: list[Path] = []
    for base in projects_dirs:
        if not base.exists():
            continue
        for _name, proj_dir in _claude._iter_project_dirs(base):
            files.extend(proj_dir.rglob("*.jsonl"))

    # Fingerprint gate: if no source file changed, return the tiny folded
    # result (a few hundred cube cells) without loading the per-message cache —
    # this keeps steady-state RAM low instead of re-folding ~200k rows each time.
    fp = _fingerprint(files, [stats_path])
    folded_name = f"claude-folded-{key}.json"
    folded = _cache_load(cache_dir, folded_name)
    if folded.get("fp") == fp and "data" in folded:
        es = _fold_to_es(folded["data"], "claude")
        _attach_claude_rl(es)
        return es

    cache_name = f"claude-facts-{key}.json"
    cache = _cache_load(cache_dir, cache_name)
    dirty = False

    raw: dict = {}            # (day, model) -> [inp, out, cr, cw, cw1h]
    seen_tok: set = set()     # global messageId:requestId dedup (cost/tokens)
    seen_asst: set = set()    # global msg.id dedup (activity)
    act_agg: dict = {}        # day -> [messages, sessions, tool_calls]
    heat_total = [0] * 168    # weekday*24 + hour
    proj_raw: dict = {}       # (project, model) -> [inp, out, cr, cw, cw1h]
    proj_lines: dict = {}     # project -> [lines_added, lines_removed]

    for jf in files:
        fpath = str(jf)
        fkey = _fkey(jf)
        if fkey is None:
            continue
        cached = cache.get(fpath)
        if cached and cached.get("k") == fkey:
            data = cached["d"]
        else:
            data = _claude_file_data(jf)
            cache[fpath] = {"k": fkey, "d": data}
            dirty = True

        proj = data.get("cwd")
        for e in data["tok"]:
            day, model, inp, out, cr, cw, cw1h, h = e
            if h:
                if h in seen_tok:
                    continue
                seen_tok.add(h)
            cell = raw.setdefault((day, model), [0, 0, 0, 0, 0])
            cell[0] += inp
            cell[1] += out
            cell[2] += cr
            cell[3] += cw
            cell[4] += cw1h
            if proj:
                pc = proj_raw.setdefault((proj, model), [0, 0, 0, 0, 0])
                pc[0] += inp
                pc[1] += out
                pc[2] += cr
                pc[3] += cw
                pc[4] += cw1h

        for k, c in (data.get("heat") or {}).items():
            heat_total[int(k)] += c
        if proj and (data.get("la") or data.get("lr")):
            pl = proj_lines.setdefault(proj, [0, 0])
            pl[0] += data.get("la", 0)
            pl[1] += data.get("lr", 0)

        file_days: set = set()
        for ev in data["act"]:
            day, role, mid, tool_count, _otoks = ev
            file_days.add(day)
            d = act_agg.setdefault(day, [0, 0, 0])
            if role == "u":
                d[0] += 1
            elif role == "a":
                if mid:
                    if mid in seen_asst:
                        continue
                    seen_asst.add(mid)
                d[0] += 1
                d[2] += tool_count
        if file_days:
            act_agg.setdefault(min(file_days), [0, 0, 0])[1] += 1

    if dirty:
        _cache_save(cache_dir, cache_name, cache)

    es = EnrichedStats(source="claude")
    unpriced_tokens = 0
    for (day, model), v in raw.items():
        inp, out, cr, cw, cw1h = v
        cell = Cell(
            input=inp, output=out, cache_read=cr, cache_write=cw,
            cache_write_1h=cw1h,
        )
        priced = _price_claude_cell(model, inp, out, cr, cw, cw1h)
        if priced is None:
            es.unpriced_models.add(model)
            unpriced_tokens += inp + out + cr + cw
        else:
            (cell.input_cost, cell.output_cost, cell.cache_read_cost,
             cell.cache_write_cost, cell.uncached_input_cost) = priced
        es.cube[(day, model)] = cell
    es.unpriced_tokens = unpriced_tokens
    es.activity = {
        day: DayCounts(messages=v[0], sessions=v[1], tool_calls=v[2])
        for day, v in act_agg.items()
    }
    es.heatmap = heat_total
    es.projects = _build_projects_claude(proj_raw, proj_lines)

    # --- Lifetime floor (matches adb): max-merge per-model jsonl totals with
    #     stats-cache.json, then price with the same tables. ---
    jsonl_models: dict[str, list[int]] = {}  # model -> [inp, out, cr, cw, cw1h]
    for (day, model), cell in es.cube.items():
        jm = jsonl_models.setdefault(model, [0, 0, 0, 0, 0])
        jm[0] += cell.input
        jm[1] += cell.output
        jm[2] += cell.cache_read
        jm[3] += cell.cache_write
        jm[4] += cell.cache_write_1h

    sc_models: dict[str, list[int]] = {}
    sc_messages = sc_sessions = 0
    if stats_path is not None and Path(stats_path).exists():
        try:
            st = orjson.loads(Path(stats_path).read_bytes())
        except Exception:
            st = {}
        for m, u in (st.get("modelUsage", {}) or {}).items():
            cw = u.get("cacheCreationInputTokens", 0)
            sc_models[m] = [
                u.get("inputTokens", 0), u.get("outputTokens", 0),
                u.get("cacheReadInputTokens", 0), cw, cw,
            ]
        sc_messages = st.get("totalMessages", 0) or 0
        sc_sessions = st.get("totalSessions", 0) or 0

    lt_tokens = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "reasoning": 0}
    lt_cost = 0
    for m in set(jsonl_models) | set(sc_models):
        j = jsonl_models.get(m)
        s = sc_models.get(m)
        # adb compares TokenBreakdown.total = input+output+cache_read+cache_write
        chosen = j if (j and (not s or sum(j[:4]) >= sum(s[:4]))) else s
        if not chosen:
            continue
        inp, out, cr, cw, cw1h = chosen
        lt_tokens["input"] += inp
        lt_tokens["output"] += out
        lt_tokens["cache_read"] += cr
        lt_tokens["cache_write"] += cw
        priced = _price_claude_cell(m, inp, out, cr, cw, cw1h)
        if priced:
            lt_cost += sum(priced[:4])  # exclude the uncached-equivalent
    lt_tokens["total"] = sum(v for k, v in lt_tokens.items() if k != "total")
    es.lifetime_tokens = lt_tokens
    es.lifetime_cost_micro = lt_cost
    es.lifetime_messages = max(sum(d.messages for d in es.activity.values()), sc_messages)
    es.lifetime_sessions = max(sum(d.sessions for d in es.activity.values()), sc_sessions)

    # Persist the folded result so the next unchanged refresh hits the gate.
    _cache_save(cache_dir, folded_name, {"fp": fp, "data": _es_to_fold(es)})

    _attach_claude_rl(es)  # live OAuth, fetched fresh
    return es


# ---------------------------------------------------------------------------
# Codex
# ---------------------------------------------------------------------------


def _build_projects_codex(proj_raw: dict) -> dict:
    """Per-project tokens + cost for Codex (no lines_added/removed — Codex
    apply_patch line-counting is out of scope)."""
    out: dict = {}
    for (proj, model), v in proj_raw.items():
        input_raw, cached_in, out_t, reason = v
        cached = max(0, min(cached_in, input_raw))
        non_cached = max(0, input_raw - cached)
        p = out.setdefault(proj, _empty_project())
        p["input"] += non_cached
        p["output"] += out_t
        p["cache_read"] += cached
        p["reasoning"] += reason
        pricing = _codex._pricing_for(model)
        if pricing is None:
            continue
        cached_rate = (
            pricing.input_usd_per_mtok
            if pricing.cached_input_usd_per_mtok is None
            else pricing.cached_input_usd_per_mtok
        )
        p["cost_micro"] += (
            _micro(non_cached * pricing.input_usd_per_mtok / 1e6)
            + _micro(cached * cached_rate / 1e6)
            + _micro(out_t * pricing.output_usd_per_mtok / 1e6)
        )
    return out


def _codex_file_data(path: Path) -> dict:
    """Single-pass per-file extraction from one Codex session JSONL.

    Reuses codex's cumulative->delta logic (per-file/self-contained), and
    attributes deltas to (local_day, model). Returns a JSON-able fragment.
    """
    contexts: list = []           # (epoch, model)
    token_snaps: list = []        # (epoch, _TokenUsage)
    act_days: dict = {}           # day_iso -> [messages, tool_calls]
    active_days: set = set()
    rl_at: float | None = None
    rl: dict | None = None
    heat: dict = {}               # wh -> count
    cwd: str | None = None

    try:
        with path.open("rb") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = orjson.loads(line)
                except orjson.JSONDecodeError:
                    continue
                dt = _codex._parse_ts(obj.get("timestamp"))
                if dt is None:
                    continue
                epoch = dt.timestamp()
                typ = obj.get("type")
                payload = obj.get("payload") or {}

                if typ == "session_meta":
                    if cwd is None:
                        cwd = payload.get("cwd")
                    continue

                if typ == "turn_context":
                    model = payload.get("model") or "unknown"
                    contexts.append((epoch, str(model)))
                    day = _codex._local_day(dt).isoformat()
                    active_days.add(day)
                    act_days.setdefault(day, [0, 0])
                    continue

                if typ == "event_msg" and payload.get("type") == "token_count":
                    r = payload.get("rate_limits")
                    if r and (rl_at is None or epoch > rl_at):
                        rl_at = epoch
                        rl = r
                    active_days.add(_codex._local_day(dt).isoformat())
                    info = payload.get("info")
                    if not info:
                        continue
                    total = info.get("total_token_usage")
                    if not isinstance(total, dict):
                        continue
                    token_snaps.append((epoch, _codex._TokenUsage.from_dict(total)))
                    continue

                if typ == "response_item":
                    item_type = payload.get("type")
                    day = _codex._local_day(dt).isoformat()
                    if item_type == "function_call":
                        active_days.add(day)
                        act_days.setdefault(day, [0, 0])[1] += 1
                    elif item_type == "message":
                        role = payload.get("role")
                        if role in ("user", "assistant"):
                            active_days.add(day)
                            act_days.setdefault(day, [0, 0])[0] += 1
                            lt = _codex._to_local(dt)
                            wh = lt.weekday() * 24 + lt.hour
                            heat[str(wh)] = heat.get(str(wh), 0) + 1
                    continue
    except OSError:
        return {"cube": [], "act": {}, "active_days": [], "rl_at": None,
                "rl": None, "heat": {}, "cwd": None}

    # cumulative -> delta, attributed to (day, model)
    contexts.sort(key=lambda x: x[0])
    ctx_times = [c[0] for c in contexts]
    ctx_models = [c[1] for c in contexts]

    def model_for(epoch: float) -> str:
        if not ctx_times:
            return "unknown"
        from bisect import bisect_right
        i = bisect_right(ctx_times, epoch) - 1
        return ctx_models[i] if i >= 0 else ctx_models[0]

    token_snaps.sort(key=lambda x: x[0])
    prev = _codex._TokenUsage()
    cube: dict = {}  # (day, model) -> [input, cached_input, output, reasoning]
    for epoch, totals in token_snaps:
        d_in = max(0, totals.input_tokens - prev.input_tokens)
        d_cached = max(0, totals.cached_input_tokens - prev.cached_input_tokens)
        d_out = max(0, totals.output_tokens - prev.output_tokens)
        d_reason = max(0, totals.reasoning_output_tokens - prev.reasoning_output_tokens)
        if totals.input_tokens < prev.input_tokens:
            d_in = totals.input_tokens
        if totals.cached_input_tokens < prev.cached_input_tokens:
            d_cached = totals.cached_input_tokens
        if totals.output_tokens < prev.output_tokens:
            d_out = totals.output_tokens
        if totals.reasoning_output_tokens < prev.reasoning_output_tokens:
            d_reason = totals.reasoning_output_tokens
        prev = totals
        dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        day = _codex._local_day(dt).isoformat()
        model = model_for(epoch)
        c = cube.setdefault(f"{day}\t{model}", [0, 0, 0, 0])
        c[0] += d_in
        c[1] += d_cached
        c[2] += d_out
        c[3] += d_reason

    return {
        "cube": [[k, v] for k, v in cube.items()],
        "act": act_days,
        "active_days": list(active_days),
        "rl_at": rl_at,
        "rl": rl,
        "heat": heat,
        "cwd": cwd,
    }


def enrich_codex(sessions_dir: Path, cache_dir: Path) -> EnrichedStats:
    """Build the enriched Codex cube + activity, with per-file caching.

    This is the per-file fragment caching the review flagged as a hard
    prerequisite: unlike codex.parse() (one whole-ToolStats cache keyed by a
    directory fingerprint, so any append re-parses every file), here each
    session file is cached by (size, mtime_ns) and only changed files re-parse.
    """
    es = EnrichedStats(source="codex")
    if not sessions_dir.exists():
        return es
    files = sorted(p for p in sessions_dir.rglob("*.jsonl") if p.is_file())
    if not files:
        return es

    key = hashlib.md5(str(sessions_dir).encode()).hexdigest()[:12]
    fp = _fingerprint(files)
    folded_name = f"codex-folded-{key}.json"
    folded = _cache_load(cache_dir, folded_name)
    if folded.get("fp") == fp and "data" in folded:
        return _fold_to_es(folded["data"], "codex")

    cache_name = f"codex-facts-{key}.json"
    cache = _cache_load(cache_dir, cache_name)
    dirty = False

    raw: dict = {}        # (day, model) -> [input, cached, output, reasoning]
    act_agg: dict = {}    # day -> [messages, sessions, tool_calls]
    best_rl_at: float | None = None
    best_rl: dict | None = None
    heat_total = [0] * 168
    proj_raw: dict = {}   # (project, model) -> [input, cached, output, reasoning]

    for jf in files:
        fpath = str(jf)
        fkey = _fkey(jf)
        if fkey is None:
            continue
        cached = cache.get(fpath)
        if cached and cached.get("k") == fkey:
            data = cached["d"]
        else:
            data = _codex_file_data(jf)
            cache[fpath] = {"k": fkey, "d": data}
            dirty = True

        proj = data.get("cwd")
        for k, v in data["cube"]:
            day, model = k.split("\t", 1)
            cell = raw.setdefault((day, model), [0, 0, 0, 0])
            cell[0] += v[0]
            cell[1] += v[1]
            cell[2] += v[2]
            cell[3] += v[3]
            if proj:
                pc = proj_raw.setdefault((proj, model), [0, 0, 0, 0])
                pc[0] += v[0]
                pc[1] += v[1]
                pc[2] += v[2]
                pc[3] += v[3]
        for kk, hc in (data.get("heat") or {}).items():
            heat_total[int(kk)] += hc
        for day, mt in data["act"].items():
            d = act_agg.setdefault(day, [0, 0, 0])
            d[0] += mt[0]
            d[2] += mt[1]
        for day in data["active_days"]:
            act_agg.setdefault(day, [0, 0, 0])[1] += 1  # 1 session per active day
        rl_at = data.get("rl_at")
        rl = data.get("rl")
        if rl and rl_at is not None:
            lid = rl.get("limit_id", "")
            if (not lid or lid == "codex") and (best_rl_at is None or rl_at > best_rl_at):
                best_rl_at = rl_at
                best_rl = rl

    if dirty:
        _cache_save(cache_dir, cache_name, cache)

    unpriced_tokens = 0
    for (day, model), v in raw.items():
        input_raw, cached_in, out, reason = v
        cached = max(0, min(cached_in, input_raw))
        non_cached = max(0, input_raw - cached)
        cell = Cell(
            input=non_cached, output=out, cache_read=cached, reasoning=reason,
        )
        pricing = _codex._pricing_for(model)
        if pricing is None:
            es.unpriced_models.add(model)
            unpriced_tokens += input_raw + out + reason
        else:
            cached_rate = (
                pricing.input_usd_per_mtok
                if pricing.cached_input_usd_per_mtok is None
                else pricing.cached_input_usd_per_mtok
            )
            cell.input_cost = _micro(non_cached * pricing.input_usd_per_mtok / 1e6)
            cell.cache_read_cost = _micro(cached * cached_rate / 1e6)
            cell.output_cost = _micro(out * pricing.output_usd_per_mtok / 1e6)
            cell.uncached_input_cost = _micro(
                (non_cached + cached) * pricing.input_usd_per_mtok / 1e6
            )
        es.cube[(day, model)] = cell
    es.unpriced_tokens = unpriced_tokens
    es.activity = {
        day: DayCounts(messages=v[0], sessions=v[1], tool_calls=v[2])
        for day, v in act_agg.items()
    }
    es.heatmap = heat_total
    es.projects = _build_projects_codex(proj_raw)

    # Codex cube is the full lifetime (no separate stale cache), so the
    # lifetime headline is just the cube sum.
    lt = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0, "reasoning": 0}
    for cell in es.cube.values():
        lt["input"] += cell.input
        lt["output"] += cell.output
        lt["cache_read"] += cell.cache_read
        lt["reasoning"] += cell.reasoning
    lt["total"] = sum(v for k, v in lt.items() if k != "total")
    es.lifetime_tokens = lt
    es.lifetime_cost_micro = sum(c.total_cost for c in es.cube.values())
    es.lifetime_messages = sum(d.messages for d in es.activity.values())
    es.lifetime_sessions = sum(d.sessions for d in es.activity.values())

    if best_rl:
        es.rate_limits = _codex._convert_rate_limits(best_rl)
        es.rate_limits_source = "session_log"
        if best_rl_at is not None:
            es.rate_limits_observed_at = datetime.fromtimestamp(
                best_rl_at, tz=timezone.utc
            ).isoformat()
        plan_type = best_rl.get("plan_type")
        if isinstance(plan_type, str) and plan_type.strip():
            es.tier = plan_type.strip()

    _cache_save(cache_dir, folded_name, {"fp": fp, "data": _es_to_fold(es)})
    return es
