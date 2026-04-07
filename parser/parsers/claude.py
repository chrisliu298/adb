"""Parse Claude Code stats from ~/.claude/stats-cache.json and related files."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import orjson

try:
    import httpx
except ImportError:
    httpx = None  # type: ignore[assignment]

from parser.types import (
    CostBreakdown,
    DayActivity,
    ProjectInfo,
    RateLimitInfo,
    TokenBreakdown,
    ToolStats,
)

STATS = Path.home() / ".claude" / "stats-cache.json"
HISTORY = Path.home() / ".claude" / "history.jsonl"

CACHE_READ_MULTIPLIER = 0.1
CACHE_WRITE_MULTIPLIER = float(os.getenv("CLAUDE_CACHE_WRITE_MULTIPLIER", "1.25"))
PRICE: dict[str, list[float]] = {
    "opus-4-6": [5, 25],
    "opus-4-5": [5, 25],
    "sonnet-4-6": [3, 15],
    "sonnet-4-5": [3, 15],
    "haiku-4-5": [1, 5],
}


def _pkey(model: str) -> str | None:
    m = model.lower()
    for k in PRICE:
        if k in m or k.replace("-4-5", "-4.5").replace("-4-1", "-4.1").replace(
            "-3-5", "-3.5"
        ).replace("-3-7", "-3.7") in m:
            return k
    return None


def _model_cost(u: dict, pk: str | None) -> float:
    if not pk or pk not in PRICE:
        return 0.0
    p = PRICE[pk]
    cr = p[0] * CACHE_READ_MULTIPLIER
    cw = p[0] * CACHE_WRITE_MULTIPLIER
    return (
        u.get("inputTokens", 0) * p[0]
        + u.get("outputTokens", 0) * p[1]
        + u.get("cacheReadInputTokens", 0) * cr
        + u.get("cacheCreationInputTokens", 0) * cw
    ) / 1e6


def _freset(s: str) -> str:
    try:
        secs = (
            datetime.fromisoformat(s.replace("Z", "+00:00"))
            - datetime.now(datetime.fromisoformat(s.replace("Z", "+00:00")).tzinfo)
        ).total_seconds()
        if secs <= 0:
            return "now"
        h, m = divmod(int(secs) // 60, 60)
        d, h = divmod(h, 24)
        return f"{d}d {h}h" if d else f"{h}h {m}m" if h else f"{m}m"
    except Exception:
        return "?"


def _get_creds() -> dict | None:
    if sys.platform != "darwin":
        return None
    try:
        r = subprocess.run(
            ["security", "find-generic-password", "-s", "Claude Code-credentials"],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            return None
        acct = next(
            (
                line.split("=")[1].strip().strip('"')
                for line in r.stdout.split("\n")
                if '"acct"<blob>=' in line
            ),
            None,
        )
        if not acct:
            return None
        r = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-s",
                "Claude Code-credentials",
                "-a",
                acct,
                "-w",
            ],
            capture_output=True,
            text=True,
        )
        if r.returncode != 0:
            return None
        return orjson.loads(r.stdout.strip()).get("claudeAiOauth")
    except Exception:
        return None


_RL_CACHE_PATH = Path(__file__).resolve().parent.parent.parent / ".cache" / "rate-limits.json"
_RL_TTL = 600  # 10 minutes


def _fetch_rate_limits(cr: dict) -> list[RateLimitInfo]:
    import time

    # Check disk cache
    if _RL_CACHE_PATH.exists():
        try:
            cached = orjson.loads(_RL_CACHE_PATH.read_bytes())
            if time.time() - cached.get("ts", 0) < _RL_TTL:
                return [RateLimitInfo(**r) for r in cached["limits"]]
        except Exception:
            pass

    limits: list[RateLimitInfo] = []
    if httpx is None:
        return limits
    try:
        resp = httpx.get(
            "https://api.anthropic.com/api/oauth/usage",
            headers={
                "Authorization": f"Bearer {cr['accessToken']}",
                "anthropic-beta": "oauth-2025-04-20",
            },
            timeout=30,
        )
        if resp.status_code == 429:
            # On 429, write cache with current time to back off
            _save_rl_cache([], time.time())
            return _load_rl_cache_limits()
        data = resp.json()
        for nm, ky in [("5-Hour", "five_hour"), ("7-Day", "seven_day")]:
            d = data.get(ky)
            if not d:
                continue
            u = d.get("utilization")
            if u is None:
                continue
            limits.append(
                RateLimitInfo(
                    label=nm,
                    utilization=float(u),
                    resets_in=_freset(d.get("resets_at", "")),
                )
            )
        if limits:
            _save_rl_cache(limits, time.time())
    except Exception:
        pass
    return limits or _load_rl_cache_limits()


def _save_rl_cache(limits: list[RateLimitInfo], ts: float) -> None:
    try:
        _RL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        data = {"ts": ts, "limits": [{"label": r.label, "utilization": r.utilization, "resets_in": r.resets_in} for r in limits]}
        _RL_CACHE_PATH.write_bytes(orjson.dumps(data))
    except Exception:
        pass


def _load_rl_cache_limits() -> list[RateLimitInfo]:
    try:
        if _RL_CACHE_PATH.exists():
            cached = orjson.loads(_RL_CACHE_PATH.read_bytes())
            return [RateLimitInfo(**r) for r in cached.get("limits", [])]
    except Exception:
        pass
    return []


def _get_tier(cr: dict | None) -> str:
    if not cr:
        return ""
    t = str(cr.get("rateLimitTier", "") or "")
    if "max_5x" in t:
        return "Max 5x"
    if "max_20x" in t:
        return "Max 20x"
    if "pro" in t.lower():
        return "Pro"
    return ""


def _count_turns(history_path: Path) -> int | None:
    if not history_path.exists():
        return None
    try:
        with history_path.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)
    except Exception:
        return None


def _parse_date(s: str) -> date | None:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            return None


def _parse_one_session_file(
    jsonl_file: Path,
) -> tuple[str | None, float, int, set[str], int]:
    """Parse a single JSONL file, return (cwd, cost, output_tokens, seen_ids, duration_ms)."""
    cwd: str | None = None
    cost = 0.0
    output_tokens = 0
    seen_ids: set[str] = set()
    first_ts: str | None = None
    last_ts: str | None = None
    try:
        with jsonl_file.open("rb") as f:
            for line in f:
                if b'"usage"' in line and b'"assistant"' in line:
                    try:
                        obj = orjson.loads(line)
                        if cwd is None:
                            c = obj.get("cwd")
                            if c:
                                cwd = c
                        ts = obj.get("timestamp")
                        if ts:
                            if first_ts is None:
                                first_ts = ts
                            last_ts = ts
                        msg = obj.get("message")
                        if not msg:
                            continue
                        if (
                            msg.get("role") == "assistant"
                            and msg.get("stop_reason") in ("end_turn", "tool_use")
                            and msg.get("usage")
                        ):
                            mid = msg.get("id", "")
                            if mid in seen_ids:
                                continue
                            seen_ids.add(mid)
                            model = msg.get("model", "")
                            u = msg["usage"]
                            pk = _pkey(model)
                            if not pk or pk not in PRICE:
                                continue
                            p = PRICE[pk]
                            inp = u.get("input_tokens", 0)
                            out = u.get("output_tokens", 0)
                            cr = u.get("cache_read_input_tokens", 0)
                            cw = u.get("cache_creation_input_tokens", 0)
                            cost += (
                                inp * p[0]
                                + out * p[1]
                                + cr * p[0] * CACHE_READ_MULTIPLIER
                                + cw * p[0] * CACHE_WRITE_MULTIPLIER
                            ) / 1e6
                            output_tokens += out
                    except (orjson.JSONDecodeError, ValueError, TypeError):
                        continue
                elif b'"timestamp"' in line:
                    try:
                        obj = orjson.loads(line)
                        if cwd is None:
                            c = obj.get("cwd")
                            if c:
                                cwd = c
                        ts = obj.get("timestamp")
                        if ts:
                            if first_ts is None:
                                first_ts = ts
                            last_ts = ts
                    except Exception:
                        pass
                elif cwd is None and b'"cwd"' in line:
                    try:
                        obj = orjson.loads(line)
                        c = obj.get("cwd")
                        if c:
                            cwd = c
                    except Exception:
                        pass
    except OSError:
        pass
    duration_ms = 0
    if first_ts and last_ts and first_ts != last_ts:
        try:
            t0 = datetime.fromisoformat(first_ts.replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
            duration_ms = max(0, int((t1 - t0).total_seconds() * 1000))
        except Exception:
            pass
    return cwd, cost, output_tokens, seen_ids, duration_ms


def _load_cache(cache_path: Path) -> dict:
    if cache_path.exists():
        try:
            return orjson.loads(cache_path.read_bytes())
        except Exception:
            pass
    return {}


def _save_cache(cache_path: Path, cache: dict) -> None:
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(orjson.dumps(cache))
    except OSError:
        pass


def _load_projects_from_sessions(projects_base: Path) -> list[ProjectInfo]:
    """Compute cumulative per-project costs from session conversation logs.

    Uses an incremental file-level cache keyed by (size, mtime_ns) to avoid
    re-parsing unchanged files on subsequent runs.
    """
    if not projects_base.exists():
        return []

    # Store cache in repo's .cache dir, not inside ~/.claude/projects/
    import hashlib
    base_hash = hashlib.md5(str(projects_base).encode()).hexdigest()[:12]
    cache_dir = Path(__file__).resolve().parent.parent.parent / ".cache"
    cache_path = cache_dir / f"claude-projects-{base_hash}.json"
    cache = _load_cache(cache_path)
    cache_dirty = False

    from collections import defaultdict
    project_costs: dict[str, float] = defaultdict(float)
    project_output: dict[str, int] = defaultdict(int)
    project_duration: dict[str, int] = defaultdict(int)
    project_paths: dict[str, str] = {}
    seen_ids: set[str] = set()

    for proj_key, proj_dir in _iter_project_dirs(projects_base):
        for jsonl_file in proj_dir.rglob("*.jsonl"):
            fpath = str(jsonl_file)
            try:
                st = jsonl_file.stat()
                fkey = f"{st.st_size}:{st.st_mtime_ns}"
            except OSError:
                continue

            cached = cache.get(fpath)
            if cached and cached.get("k") == fkey:
                # Cache hit — use stored results
                cost = cached.get("c", 0.0)
                out = cached.get("o", 0)
                cwd = cached.get("w")
                dur = cached.get("d", 0)
                file_ids = cached.get("ids", [])
                if cost > 0:
                    # Check for ID collisions with already-seen IDs
                    new_ids = [mid for mid in file_ids if mid not in seen_ids]
                    if len(new_ids) == len(file_ids):
                        # No collisions, use cached totals directly
                        project_costs[proj_key] += cost
                        project_output[proj_key] += out
                        seen_ids.update(file_ids)
                    else:
                        # Some IDs already seen; must re-derive cost
                        # This is rare (subagent files sharing message IDs)
                        seen_ids.update(new_ids)
                        if new_ids:
                            # Proportional estimate
                            ratio = len(new_ids) / len(file_ids)
                            project_costs[proj_key] += cost * ratio
                            project_output[proj_key] += int(out * ratio)
                project_duration[proj_key] += dur
                if cwd and proj_key not in project_paths:
                    project_paths[proj_key] = cwd
            else:
                # Cache miss — parse file
                cwd, cost, out, file_ids, dur = _parse_one_session_file(jsonl_file)
                # Deduplicate IDs against global seen set
                new_ids = file_ids - seen_ids
                if len(new_ids) < len(file_ids) and file_ids:
                    ratio = len(new_ids) / len(file_ids)
                    project_costs[proj_key] += cost * ratio
                    project_output[proj_key] += int(out * ratio)
                else:
                    project_costs[proj_key] += cost
                    project_output[proj_key] += out
                project_duration[proj_key] += dur
                seen_ids.update(file_ids)
                if cwd and proj_key not in project_paths:
                    project_paths[proj_key] = cwd
                # Store in cache
                cache[fpath] = {
                    "k": fkey,
                    "c": cost,
                    "o": out,
                    "w": cwd,
                    "d": dur,
                    "ids": list(file_ids),
                }
                cache_dirty = True

    if cache_dirty:
        _save_cache(cache_path, cache)

    result = []
    for proj_key, cost in project_costs.items():
        path = project_paths.get(proj_key, proj_key)
        result.append(
            ProjectInfo(
                path=path,
                cost=cost,
                output_tokens=project_output.get(proj_key, 0),
                duration_ms=project_duration.get(proj_key, 0),
            )
        )
    return sorted(result, key=lambda p: p.cost, reverse=True)


PROJECTS_BASE = Path.home() / ".claude" / "projects"


def _iter_project_dirs(projects_base: Path):
    """Yield (proj_key, proj_dir) for each project directory.

    Handles both regular project dirs and .remote-<host>/ staging dirs
    created by recall-sync.
    """
    for entry in projects_base.iterdir():
        if not entry.is_dir():
            continue
        if entry.name.startswith(".remote-"):
            # Staging dir: .remote-<host>/<project>/<session>.jsonl
            for sub in entry.iterdir():
                if sub.is_dir():
                    yield sub.name, sub
        elif not entry.name.startswith("."):
            yield entry.name, entry


def _discover_project_dirs(projects_base: Path) -> list[Path]:
    """Discover all Claude Code project directories.

    For local (default projects_base), also checks CLAUDE_CONFIG_DIR,
    XDG_CONFIG_HOME, and ~/.claude-* dirs.
    For remote (custom projects_base), just returns [projects_base].
    """
    if projects_base != PROJECTS_BASE:
        return [projects_base] if projects_base.is_dir() else []

    home = Path.home()
    dirs: list[Path] = []
    seen: set[str] = set()

    def _add(d: Path) -> None:
        resolved = str(d.resolve())
        if resolved not in seen and d.is_dir():
            seen.add(resolved)
            dirs.append(d)

    env_dirs = os.environ.get("CLAUDE_CONFIG_DIR", "").strip()
    if env_dirs:
        for p in env_dirs.split(","):
            p = p.strip()
            if p:
                _add(Path(p).resolve() / "projects")

    _add(projects_base)

    xdg = os.environ.get("XDG_CONFIG_HOME", "").strip() or str(home / ".config")
    _add(Path(xdg) / "claude" / "projects")

    try:
        for entry in home.iterdir():
            if entry.is_dir() and entry.name.startswith(".claude-"):
                _add(entry / "projects")
    except OSError:
        pass

    return dirs


def _parse_file_tokens_loose(
    jsonl_file: Path,
) -> list[tuple[str, str, int, int, int, int]]:
    """Parse a single JSONL file with loose filtering for tokens.

    Accepts any entry with timestamp + message.usage (no role/stop_reason check).
    Deduplicates within the file by messageId:requestId composite key.
    Returns list of (hash_or_empty, model, inp, out, cr, cw) per unique entry.
    """
    entries: list[tuple[str, str, int, int, int, int]] = []
    seen: set[str] = set()

    try:
        with jsonl_file.open("rb") as f:
            for line in f:
                if b'"usage"' not in line:
                    continue
                try:
                    obj = orjson.loads(line)
                except (orjson.JSONDecodeError, ValueError):
                    continue

                if not obj.get("timestamp"):
                    continue
                msg = obj.get("message")
                if not msg or not msg.get("usage"):
                    continue

                mid = msg.get("id", "")
                rid = obj.get("requestId", "")
                h = ""
                if mid and rid:
                    h = f"{mid}:{rid}"
                    if h in seen:
                        continue
                    seen.add(h)

                u = msg["usage"]
                inp = u.get("input_tokens", 0)
                out = u.get("output_tokens", 0)
                cr = u.get("cache_read_input_tokens", 0)
                cw = u.get("cache_creation_input_tokens", 0)

                if inp + out + cr + cw == 0:
                    continue

                model = msg.get("model", "") or "unknown"
                if model == "<synthetic>":
                    model = "unknown"

                entries.append((h, model, inp, out, cr, cw))
    except OSError:
        pass

    return entries


def _load_all_tokens(
    projects_dirs: list[Path],
) -> tuple[TokenBreakdown, dict[str, TokenBreakdown]]:
    """Load token totals from all JSONL files across project dirs.

    Uses loose filtering (any entry with timestamp + message.usage) and
    messageId:requestId composite dedup. Per-file entry lists are cached.
    """
    import hashlib

    dirs_str = ":".join(sorted(str(d) for d in projects_dirs))
    dirs_hash = hashlib.md5(dirs_str.encode()).hexdigest()[:12]
    cache_dir = Path(__file__).resolve().parent.parent.parent / ".cache"
    cache_path = cache_dir / f"claude-tokens-{dirs_hash}.json"
    cache = _load_cache(cache_path)
    cache_dirty = False

    seen_hashes: set[str] = set()
    total_tokens = TokenBreakdown()
    models: dict[str, TokenBreakdown] = {}

    for projects_base in projects_dirs:
        if not projects_base.exists():
            continue
        for _, proj_dir in _iter_project_dirs(projects_base):
            for jsonl_file in proj_dir.rglob("*.jsonl"):
                fpath = str(jsonl_file)
                try:
                    st = jsonl_file.stat()
                    fkey = f"{st.st_size}:{st.st_mtime_ns}"
                except OSError:
                    continue

                cached_entry = cache.get(fpath)
                if cached_entry and cached_entry.get("k") == fkey:
                    file_entries = cached_entry.get("e", [])
                else:
                    raw_entries = _parse_file_tokens_loose(jsonl_file)
                    file_entries = [list(e) for e in raw_entries]
                    cache[fpath] = {"k": fkey, "e": file_entries}
                    cache_dirty = True

                # Exact global dedup per entry
                for entry in file_entries:
                    h, model = entry[0], entry[1]
                    inp, out, cr, cw = entry[2], entry[3], entry[4], entry[5]
                    if h:
                        if h in seen_hashes:
                            continue
                        seen_hashes.add(h)
                    tb = TokenBreakdown(
                        input_tokens=inp,
                        output_tokens=out,
                        cache_read_tokens=cr,
                        cache_write_tokens=cw,
                    )
                    total_tokens.add(tb)
                    if model not in models:
                        models[model] = TokenBreakdown()
                    models[model].add(tb)

    if cache_dirty:
        _save_cache(cache_path, cache)

    return total_tokens, models


def parse(
    *,
    stats_path: Path = STATS,
    history_path: Path = HISTORY,
    projects_base: Path = PROJECTS_BASE,
) -> ToolStats | None:
    """Parse Claude Code stats. Returns None if no data available."""
    if not stats_path.exists():
        return None
    try:
        st = orjson.loads(stats_path.read_bytes())
    except (orjson.JSONDecodeError, OSError):
        return None

    mu = st.get("modelUsage", {}) or {}
    da = st.get("dailyActivity", []) or []
    dm = st.get("dailyModelTokens", []) or []
    hc = st.get("hourCounts", {}) or {}

    # Build daily activity map
    daily_map: dict[date, DayActivity] = {}
    for d in da:
        dt = _parse_date(d.get("date", ""))
        if dt is None:
            continue
        daily_map[dt] = DayActivity(
            day=dt,
            messages=d.get("messageCount", 0),
            sessions=d.get("sessionCount", 0),
            tool_calls=d.get("toolCallCount", 0),
        )

    # Add output tokens from dailyModelTokens
    for d in dm:
        dt = _parse_date(d.get("date", ""))
        if dt is None:
            continue
        ot = sum(d.get("tokensByModel", {}).values())
        if dt in daily_map:
            daily_map[dt].output_tokens = ot
        else:
            daily_map[dt] = DayActivity(day=dt, output_tokens=ot)

    daily = sorted(daily_map.values(), key=lambda x: x.day)

    total_sessions = st.get("totalSessions", 0)
    total_messages = st.get("totalMessages", 0)
    total_tool_calls = sum(d.get("toolCallCount", 0) for d in da)

    # Token breakdown: merge JSONL session files + stats-cache, taking max per model
    projects_dirs = _discover_project_dirs(projects_base)
    jsonl_tokens, jsonl_models = _load_all_tokens(projects_dirs)

    # Build stats-cache per-model breakdowns
    sc_models: dict[str, TokenBreakdown] = {}
    for m, u in mu.items():
        sc_models[m] = TokenBreakdown(
            input_tokens=u.get("inputTokens", 0),
            output_tokens=u.get("outputTokens", 0),
            cache_read_tokens=u.get("cacheReadInputTokens", 0),
            cache_write_tokens=u.get("cacheCreationInputTokens", 0),
        )

    # Merge: for each model, pick whichever source has a higher total
    models: dict[str, TokenBreakdown] = {}
    all_model_names = set(jsonl_models.keys()) | set(sc_models.keys())
    for m in all_model_names:
        jtb = jsonl_models.get(m)
        stb = sc_models.get(m)
        if jtb and stb:
            models[m] = jtb if jtb.total >= stb.total else stb
        elif jtb:
            models[m] = jtb
        else:
            models[m] = stb  # type: ignore[assignment]

    total_tokens = TokenBreakdown()
    model_costs: dict[str, float] = {}
    cb = CostBreakdown()
    for m, tb in models.items():
        total_tokens.add(tb)
        pk = _pkey(m)
        c = 0.0
        if pk and pk in PRICE:
            p = PRICE[pk]
            c = (
                tb.input_tokens * p[0]
                + tb.output_tokens * p[1]
                + tb.cache_read_tokens * (p[0] * CACHE_READ_MULTIPLIER)
                + tb.cache_write_tokens * (p[0] * CACHE_WRITE_MULTIPLIER)
            ) / 1e6
            cb.input_tokens += tb.input_tokens
            cb.output_tokens += tb.output_tokens
            cb.cache_read_tokens += tb.cache_read_tokens
            cb.cache_write_tokens += tb.cache_write_tokens
            cb.input_cost += tb.input_tokens * p[0] / 1e6
            cb.output_cost += tb.output_tokens * p[1] / 1e6
            cb.cache_read_cost += tb.cache_read_tokens * (p[0] * CACHE_READ_MULTIPLIER) / 1e6
            cb.cache_write_cost += tb.cache_write_tokens * (p[0] * CACHE_WRITE_MULTIPLIER) / 1e6
        model_costs[m] = c

    total_cost = sum(model_costs.values())

    # Hour counts
    hour_counts = {h: 0 for h in range(24)}
    for h_str, cnt in hc.items():
        try:
            hour_counts[int(h_str)] = cnt
        except (ValueError, KeyError):
            pass

    # First date
    first_date = None
    if st.get("firstSessionDate"):
        first_date = _parse_date(st["firstSessionDate"])

    # Turns
    turns = _count_turns(history_path)

    # Credentials and rate limits
    cr_data = _get_creds()
    rate_limits = _fetch_rate_limits(cr_data) if cr_data else []
    tier = _get_tier(cr_data)

    # Longest session
    ls = st.get("longestSession") or {}
    longest_dur = ls.get("duration", 0)
    longest_msgs = ls.get("messageCount", 0)

    # Projects (from session logs for cumulative costs)
    projects = _load_projects_from_sessions(projects_base)

    return ToolStats(
        source="claude",
        total_tokens=total_tokens,
        total_sessions=total_sessions,
        total_messages=total_messages,
        total_tool_calls=total_tool_calls,
        total_turns=turns or 0,
        total_cost=total_cost,
        first_date=first_date,
        models=models,
        model_costs=model_costs,
        cost_breakdown=cb,
        daily=daily,
        hour_counts=hour_counts,
        rate_limits=rate_limits,
        projects=projects,
        longest_session_duration_ms=longest_dur,
        longest_session_messages=longest_msgs,
        extra={"tier": tier},
    )
