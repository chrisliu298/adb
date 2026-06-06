"""Parse Claude Code stats from ~/.claude/stats-cache.json and related files."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, date, datetime, timezone
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
# Claude prompt-cache write pricing: 5-min TTL is 1.25x input, 1-hour TTL is 2x input.
# Claude Code currently uses 1-hour caching exclusively; parsers read the per-message
# cache_creation.ephemeral_{5m,1h}_input_tokens breakdown and price each bucket correctly.
CACHE_WRITE_5M_MULTIPLIER = 1.25
CACHE_WRITE_1H_MULTIPLIER = 2.0
PRICE: dict[str, list[float]] = {
    "opus-4-8": [5, 25],
    "opus-4-7": [5, 25],
    "opus-4-6": [5, 25],
    "opus-4-5": [5, 25],
    "sonnet-4-6": [3, 15],
    "sonnet-4-5": [3, 15],
    "haiku-4-5": [1, 5],
    "deepseek-v4-pro": [0.435, 0.87],
    "deepseek-v4-flash": [0.14, 0.28],
    # mimo-v2.5-pro must precede mimo-v2.5: the latter is a substring of the
    # former and _pkey() returns the first matching key.
    "mimo-v2.5-pro": [0.435, 0.87],
    "mimo-v2.5": [0.14, 0.28],
    "minimax-m3": [0.6, 2.4],
    "kimi-k2.6": [0.95, 4.0],
}

# Per-model cache pricing overrides as absolute $/MTok: (cache_read, cw_5m, cw_1h).
# Default (when key absent) derives from input price via the multipliers above.
# DeepSeek publishes explicit cache-hit pricing and has no cache-write premium, so
# writes are priced at the cache-miss (input) rate for both TTL buckets.
CACHE_OVERRIDES: dict[str, tuple[float, float, float]] = {
    "deepseek-v4-pro": (0.003625, 0.435, 0.435),
    "deepseek-v4-flash": (0.0028, 0.14, 0.14),
    # MiMo publishes explicit cache-hit pricing and has no cache-write premium,
    # so writes price at the cache-miss (input) rate for both TTL buckets.
    "mimo-v2.5-pro": (0.0036, 0.435, 0.435),
    "mimo-v2.5": (0.0028, 0.14, 0.14),
    # MiniMax M3 publishes an explicit cache-read price ($0.12/MTok) and no
    # cache-write premium, so writes price at the cache-miss (input) rate.
    # Prices are the standard (non-promo) rates for input <= 512k tokens.
    "minimax-m3": (0.12, 0.6, 0.6),
    # Kimi K2.6 publishes an explicit cache-hit price ($0.16/MTok) and no
    # cache-write premium, so writes price at the cache-miss (input) rate.
    "kimi-k2.6": (0.16, 0.95, 0.95),
}


def _cache_rates(pk: str, input_price: float) -> tuple[float, float, float]:
    """Return ($/MTok) for (cache_read, cw_5m, cw_1h) for the given model key."""
    if pk in CACHE_OVERRIDES:
        return CACHE_OVERRIDES[pk]
    return (
        input_price * CACHE_READ_MULTIPLIER,
        input_price * CACHE_WRITE_5M_MULTIPLIER,
        input_price * CACHE_WRITE_1H_MULTIPLIER,
    )


def _pkey(model: str) -> str | None:
    m = model.lower()
    for k in PRICE:
        if k in m or k.replace("-4-5", "-4.5").replace("-4-1", "-4.1").replace(
            "-3-5", "-3.5"
        ).replace("-3-7", "-3.7") in m:
            return k
    return None


def _cw_split(u: dict) -> tuple[int, int]:
    """Return (cache_write_total, cache_write_1h) from a JSONL usage dict."""
    cw_total = u.get("cache_creation_input_tokens", 0) or 0
    cc = u.get("cache_creation")
    cw_1h = 0
    if isinstance(cc, dict):
        cw_1h = cc.get("ephemeral_1h_input_tokens", 0) or 0
    return cw_total, cw_1h


def _cw_cost(cw_total: int, cw_1h: int, cw_5m_price: float, cw_1h_price: float) -> float:
    """Cost for cache-write tokens, priced per TTL bucket. Prices are $/MTok."""
    cw_5m = max(0, cw_total - cw_1h)
    return (cw_5m * cw_5m_price + cw_1h * cw_1h_price) / 1e6


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


_TERMINAL_STOP_REASONS = frozenset(
    ("end_turn", "tool_use", "stop_sequence", "max_tokens", "pause_turn", "refusal")
)
_IDLE_GAP_S = 30 * 60  # 30 minutes — anything longer is treated as user idle


def _parse_one_session_file(
    jsonl_file: Path,
) -> tuple[str | None, float, int, set[str], int]:
    """Parse a single JSONL file, return (cwd, cost, output_tokens, seen_ids, duration_ms).

    For each unique msg.id, picks the most informative variant: a terminal
    stop_reason beats None, and among ties the entry with the largest
    output_tokens wins (handles streaming partial snapshots correctly).
    Duration sums consecutive timestamp deltas below _IDLE_GAP_S so a session
    left open overnight doesn't inflate the total.
    """
    cwd: str | None = None
    timestamps: list[datetime] = []
    msgs: dict[str, tuple[str, dict, bool]] = {}  # mid -> (model, usage, has_terminal_stop_reason)

    try:
        with jsonl_file.open("rb") as f:
            for line in f:
                has_usage_assistant = b'"usage"' in line and b'"assistant"' in line
                has_timestamp = b'"timestamp"' in line
                if not has_usage_assistant and not has_timestamp:
                    if cwd is None and b'"cwd"' in line:
                        try:
                            obj = orjson.loads(line)
                            c = obj.get("cwd")
                            if c:
                                cwd = c
                        except Exception:
                            pass
                    continue
                try:
                    obj = orjson.loads(line)
                except (orjson.JSONDecodeError, ValueError, TypeError):
                    continue
                if cwd is None:
                    c = obj.get("cwd")
                    if c:
                        cwd = c
                ts = obj.get("timestamp")
                if ts:
                    try:
                        timestamps.append(datetime.fromisoformat(ts.replace("Z", "+00:00")))
                    except Exception:
                        pass
                if not has_usage_assistant:
                    continue
                msg = obj.get("message")
                if not msg or msg.get("role") != "assistant":
                    continue
                u = msg.get("usage")
                if not u:
                    continue
                mid = msg.get("id", "")
                if not mid:
                    continue
                terminal = msg.get("stop_reason") in _TERMINAL_STOP_REASONS
                cur = msgs.get(mid)
                if cur is None:
                    msgs[mid] = (msg.get("model", ""), u, terminal)
                else:
                    _, cur_u, cur_terminal = cur
                    if (terminal and not cur_terminal) or (
                        terminal == cur_terminal
                        and u.get("output_tokens", 0) > cur_u.get("output_tokens", 0)
                    ):
                        msgs[mid] = (msg.get("model", ""), u, terminal)
    except OSError:
        pass

    cost = 0.0
    output_tokens = 0
    for model, u, _ in msgs.values():
        pk = _pkey(model)
        if not pk or pk not in PRICE:
            continue
        p = PRICE[pk]
        cr_price, cw_5m_price, cw_1h_price = _cache_rates(pk, p[0])
        inp = u.get("input_tokens", 0)
        out = u.get("output_tokens", 0)
        cr = u.get("cache_read_input_tokens", 0)
        cw_total, cw_1h = _cw_split(u)
        cost += (inp * p[0] + out * p[1] + cr * cr_price) / 1e6 + _cw_cost(
            cw_total, cw_1h, cw_5m_price, cw_1h_price
        )
        output_tokens += out

    duration_ms = 0
    if len(timestamps) >= 2:
        timestamps.sort()
        active_s = 0.0
        for i in range(1, len(timestamps)):
            delta = (timestamps[i] - timestamps[i - 1]).total_seconds()
            if 0 < delta < _IDLE_GAP_S:
                active_s += delta
        duration_ms = int(active_s * 1000)
    return cwd, cost, output_tokens, set(msgs.keys()), duration_ms


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


def _load_projects_from_sessions(projects_base) -> list[ProjectInfo]:
    """Compute cumulative per-project costs from session conversation logs.

    Accepts a Path or a list of Paths. Uses an incremental file-level cache
    keyed by (size, mtime_ns) to avoid re-parsing unchanged files.
    """
    bases = [projects_base] if isinstance(projects_base, Path) else list(projects_base)
    bases = [b for b in bases if b.exists()]
    if not bases:
        return []

    # Store cache in repo's .cache dir, not inside ~/.claude/projects/
    # v3: per-mid dedup picks largest-output variant (no stop_reason filter)
    # and duration is gap-aware, so cached costs/durations from v2 are stale.
    import hashlib
    base_key = "|".join(str(b) for b in bases)
    base_hash = hashlib.md5(base_key.encode()).hexdigest()[:12]
    cache_dir = Path(__file__).resolve().parent.parent.parent / ".cache"
    cache_path = cache_dir / f"claude-projects3-{base_hash}.json"
    cache = _load_cache(cache_path)
    cache_dirty = False

    from collections import defaultdict
    project_costs: dict[str, float] = defaultdict(float)
    project_output: dict[str, int] = defaultdict(int)
    project_duration: dict[str, int] = defaultdict(int)
    project_paths: dict[str, str] = {}
    seen_ids: set[str] = set()

    for proj_key, proj_dir in _iter_project_dirs(bases):
        for jsonl_file in _iter_session_files(proj_dir):
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


def _iter_project_dirs(projects_base):
    """Yield (proj_key, proj_dir) for each project directory.

    Accepts a Path or a list of Paths; iterates each base in order and skips
    dot-prefixed directories (including .remote-<host>/ staging dirs).
    Remote staging dirs are handled by load_all() in adb.py, which passes
    them as projects_base for the corresponding remote host.
    """
    bases = [projects_base] if isinstance(projects_base, Path) else list(projects_base)
    for base in bases:
        if not base.exists():
            continue
        for entry in base.iterdir():
            if not entry.is_dir() or entry.name.startswith("."):
                continue
            yield entry.name, entry


def _iter_session_files(proj_dir: Path):
    """Yield session JSONL files under a project dir, skipping audit.jsonl.

    Claude's local-agent-mode writes a sibling audit.jsonl that replays the same
    messages as the session log; msg.id dedup keeps it from inflating tokens/cost,
    but it would still inflate session and (id-less) user-message counts, so it is
    excluded here. No real session log is named audit.jsonl.
    """
    for f in proj_dir.rglob("*.jsonl"):
        if f.name == "audit.jsonl":
            continue
        yield f


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

    # macOS "local agent mode" stores sessions outside ~/.claude, under the app
    # container: local-agent-mode-sessions/<...>/.claude/projects/ (nested a few
    # uuid levels deep). Discover each embedded projects tree (audit.jsonl mirrors
    # live above the projects dir and are skipped at read time regardless).
    if sys.platform == "darwin":
        lam = home / "Library" / "Application Support" / "Claude" / "local-agent-mode-sessions"
        if lam.is_dir():
            try:
                for projects in lam.glob("**/.claude/projects"):
                    _add(projects)
            except OSError:
                pass

    return dirs


def _parse_file_tokens_loose(
    jsonl_file: Path,
) -> list[list]:
    """Parse a single JSONL file with loose filtering for tokens.

    Accepts any entry with timestamp + message.usage (no role/stop_reason check).
    Deduplicates within the file by msg.id, keeping the largest-output snapshot
    (streaming emits growing partials for one message; input/cache counts stay
    constant across them, so the largest-output line is the most complete).
    Entries without a msg.id can't be deduped and are all kept. Global cross-file
    dedup by msg.id is applied later in _aggregate_loose.
    Returns list of [mid, model, inp, out, cr, cw_total, cw_1h].
    """
    by_mid: dict[str, list] = {}
    nomid: list[list] = []

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

                u = msg["usage"]
                inp = u.get("input_tokens", 0)
                out = u.get("output_tokens", 0)
                cr = u.get("cache_read_input_tokens", 0)
                cw_total, cw_1h = _cw_split(u)

                if inp + out + cr + cw_total == 0:
                    continue

                model = msg.get("model", "") or "unknown"
                if model == "<synthetic>":
                    model = "unknown"

                mid = msg.get("id", "") or ""
                entry = [mid, model, inp, out, cr, cw_total, cw_1h]
                if mid:
                    cur = by_mid.get(mid)
                    if cur is None or out > cur[3]:  # cur[3] = output_tokens
                        by_mid[mid] = entry
                else:
                    nomid.append(entry)
    except OSError:
        pass

    return list(by_mid.values()) + nomid


def _aggregate_loose(
    file_entry_lists: list[list],
) -> tuple[TokenBreakdown, dict[str, TokenBreakdown]]:
    """Aggregate per-file loose entries with global msg.id dedup.

    For each msg.id, keep only the largest-output snapshot across all files.
    Subagent sessions replay a parent message under a *new* requestId, so keying
    on msg.id alone (not messageId:requestId) counts it once — matching the cost
    and daily paths, which already dedup by msg.id. Entries without a msg.id
    can't be deduped and are all counted.
    """
    best: dict[str, list] = {}
    nomid: list[list] = []
    for entries in file_entry_lists:
        for e in entries:
            mid = e[0]
            if not mid:
                nomid.append(e)
                continue
            cur = best.get(mid)
            if cur is None or e[3] > cur[3]:  # e[3] = output_tokens
                best[mid] = e

    total_tokens = TokenBreakdown()
    models: dict[str, TokenBreakdown] = {}
    for e in list(best.values()) + nomid:
        model = e[1]
        cw_1h = e[6] if len(e) > 6 else 0
        tb = TokenBreakdown(
            input_tokens=e[2],
            output_tokens=e[3],
            cache_read_tokens=e[4],
            cache_write_tokens=e[5],
            cache_write_1h_tokens=cw_1h,
        )
        total_tokens.add(tb)
        if model not in models:
            models[model] = TokenBreakdown()
        models[model].add(tb)
    return total_tokens, models


def _load_all_tokens(
    projects_dirs: list[Path],
) -> tuple[TokenBreakdown, dict[str, TokenBreakdown]]:
    """Load token totals from all JSONL files across project dirs.

    Uses loose filtering (any entry with timestamp + message.usage). Per-file
    entry lists are cached by (size, mtime); global dedup by msg.id (keeping the
    largest-output snapshot) is applied in _aggregate_loose.
    """
    import hashlib

    dirs_str = ":".join(sorted(str(d) for d in projects_dirs))
    dirs_hash = hashlib.md5(dirs_str.encode()).hexdigest()[:12]
    cache_dir = Path(__file__).resolve().parent.parent.parent / ".cache"
    # v3: dedup by msg.id keeping the largest-output snapshot. v2 keyed on the
    # messageId:requestId composite and kept the first occurrence, which dropped
    # later (larger) streaming output and double-counted requestId-less replays.
    cache_path = cache_dir / f"claude-tokens3-{dirs_hash}.json"
    cache = _load_cache(cache_path)
    cache_dirty = False

    file_entry_lists: list[list] = []
    for projects_base in projects_dirs:
        if not projects_base.exists():
            continue
        for _, proj_dir in _iter_project_dirs(projects_base):
            for jsonl_file in _iter_session_files(proj_dir):
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
                    file_entries = _parse_file_tokens_loose(jsonl_file)
                    cache[fpath] = {"k": fkey, "e": file_entries}
                    cache_dirty = True
                file_entry_lists.append(file_entries)

    if cache_dirty:
        _save_cache(cache_path, cache)

    return _aggregate_loose(file_entry_lists)


def _parse_session_events(
    jsonl_file: Path,
) -> list[tuple[str, str, str, int, int]]:
    """Parse one JSONL file into per-message events for daily aggregation.

    Returns a list of tuples: (day_str, role, mid, tool_count, output_tokens).
    - role is "u" for user turns, "a" for assistant turns.
    - Each unique msg.id emits exactly one assistant event, picking the
      variant with a terminal stop_reason and largest output_tokens (handles
      streaming partials and recovers messages whose stop_reason was never
      finalized in the log).
    - Assistant mids let the aggregator dedup messages that appear in multiple
      session files (e.g. subagent replay), which otherwise inflates daily counts.
    """
    user_events: list[tuple[str, str, str, int, int]] = []
    # mid -> (day_str, tool_count, output_tokens, terminal)
    asst_by_mid: dict[str, tuple[str, int, int, bool]] = {}
    try:
        with jsonl_file.open("rb") as f:
            for line in f:
                if b'"timestamp"' not in line:
                    continue
                try:
                    obj = orjson.loads(line)
                except (orjson.JSONDecodeError, ValueError):
                    continue

                ts = obj.get("timestamp")
                if not ts:
                    continue

                if isinstance(ts, str):
                    day_str = ts[:10]
                elif isinstance(ts, (int, float)):
                    day_str = datetime.fromtimestamp(
                        ts / 1000, tz=timezone.utc
                    ).strftime("%Y-%m-%d")
                else:
                    continue

                msg = obj.get("message")
                if not msg:
                    continue
                role = msg.get("role")
                if role == "user":
                    user_events.append((day_str, "u", "", 0, 0))
                elif role == "assistant":
                    mid = msg.get("id", "") or ""
                    if not mid:
                        continue
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
                    terminal = msg.get("stop_reason") in _TERMINAL_STOP_REASONS
                    cur = asst_by_mid.get(mid)
                    if cur is None:
                        asst_by_mid[mid] = (day_str, tool_count, otoks, terminal)
                    else:
                        _, cur_tools, cur_otoks, cur_terminal = cur
                        if (terminal and not cur_terminal) or (
                            terminal == cur_terminal and otoks > cur_otoks
                        ):
                            asst_by_mid[mid] = (day_str, tool_count, otoks, terminal)
    except OSError:
        pass

    events = list(user_events)
    for mid, (day_str, tool_count, otoks, _) in asst_by_mid.items():
        events.append((day_str, "a", mid, tool_count, otoks))
    return events


def _build_daily_from_sessions(
    projects_dirs: list[Path],
) -> list[DayActivity]:
    """Build daily activity from session JSONL files with caching and global dedup.

    Assistant messages are deduped by msg.id across files to avoid inflating
    daily output_tokens/messages/tool_calls when subagent sessions replay
    messages from a parent session.
    """
    import hashlib

    dirs_str = ":".join(sorted(str(d) for d in projects_dirs))
    dirs_hash = hashlib.md5(dirs_str.encode()).hexdigest()[:12]
    cache_dir = Path(__file__).resolve().parent.parent.parent / ".cache"
    # v3: per-mid dedup in _parse_session_events picks largest-output variant
    # and admits messages whose stop_reason was never finalized in the log.
    cache_path = cache_dir / f"claude-daily3-{dirs_hash}.json"
    cache = _load_cache(cache_path)
    cache_dirty = False

    agg: dict[str, list[int]] = {}  # date → [msgs, sessions, tools, otoks]
    seen_asst_ids: set[str] = set()
    # Dedup the same session FILE across this host's bases, keyed on its path
    # relative to the base. A remote host is read as its rsync mirror PLUS its
    # .remote-<host> staging dir, which hold copies of the same session at the
    # same relative path; counting both would inflate session/message counts (the
    # token path already dedups by msg.id). Relative-path keying collapses those
    # copies while keeping distinct files counted once each — including subagent
    # transcripts, whose agent-<hash>.jsonl basenames are NOT unique, so a
    # basename key would wrongly merge them.
    seen_rel_paths: set[str] = set()

    for projects_base in projects_dirs:
        if not projects_base.exists():
            continue
        for _, proj_dir in _iter_project_dirs(projects_base):
            for jsonl_file in _iter_session_files(proj_dir):
                relkey = str(jsonl_file.relative_to(projects_base))
                if relkey in seen_rel_paths:
                    continue
                seen_rel_paths.add(relkey)
                fpath = str(jsonl_file)
                try:
                    st = jsonl_file.stat()
                    fkey = f"{st.st_size}:{st.st_mtime_ns}"
                except OSError:
                    continue

                cached = cache.get(fpath)
                if cached and cached.get("k") == fkey:
                    file_events = cached.get("e", [])
                else:
                    file_events = [list(e) for e in _parse_session_events(jsonl_file)]
                    cache[fpath] = {"k": fkey, "e": file_events}
                    cache_dirty = True

                file_days: set[str] = set()
                for ev in file_events:
                    day_str, role, mid = ev[0], ev[1], ev[2]
                    tool_count, otoks = ev[3], ev[4]
                    file_days.add(day_str)
                    if day_str not in agg:
                        agg[day_str] = [0, 0, 0, 0]
                    if role == "u":
                        agg[day_str][0] += 1
                    elif role == "a":
                        if mid:
                            if mid in seen_asst_ids:
                                continue
                            seen_asst_ids.add(mid)
                        agg[day_str][0] += 1
                        agg[day_str][2] += tool_count
                        agg[day_str][3] += otoks

                # Count session: 1 per file, attributed to first day
                if file_days:
                    first_day = min(file_days)
                    if first_day not in agg:
                        agg[first_day] = [0, 0, 0, 0]
                    agg[first_day][1] += 1

    if cache_dirty:
        _save_cache(cache_path, cache)

    result = []
    for d, v in agg.items():
        try:
            dt = date.fromisoformat(d)
        except ValueError:
            continue
        result.append(
            DayActivity(
                day=dt,
                messages=v[0],
                sessions=v[1],
                tool_calls=v[2],
                output_tokens=v[3],
            )
        )
    return sorted(result, key=lambda x: x.day)


def parse(
    *,
    stats_path: Path = STATS,
    history_path: Path = HISTORY,
    projects_base = PROJECTS_BASE,
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

    # Seed daily output_tokens from stats-cache.json as a fallback for days whose
    # session JSONL has since been pruned. Session-derived values (computed below)
    # win wherever they exist, because stats-cache's dailyModelTokens is not
    # globally deduped across files and overcounts on days with subagent replay.
    sc_daily_output: dict[date, int] = {}
    for d in dm:
        dt = _parse_date(d.get("date", ""))
        if dt is None:
            continue
        sc_daily_output[dt] = sum(d.get("tokensByModel", {}).values())

    # Token breakdown: merge JSONL session files + stats-cache, taking max per model.
    # projects_base may be a single Path (default/local) or a list of Paths
    # (remote: rsync cache + recall-sync staging dir).
    _bases = [projects_base] if isinstance(projects_base, Path) else list(projects_base)
    projects_dirs: list[Path] = []
    _seen_resolved: set[str] = set()
    for _b in _bases:
        for _d in _discover_project_dirs(_b):
            r = str(_d.resolve())
            if r not in _seen_resolved:
                _seen_resolved.add(r)
                projects_dirs.append(_d)

    # Session-derived daily — the authoritative source when available.
    session_daily = _build_daily_from_sessions(projects_dirs)
    session_days = {sd.day for sd in session_daily}
    for sd in session_daily:
        if sd.day in daily_map:
            d = daily_map[sd.day]
            d.messages = max(d.messages, sd.messages)
            d.sessions = max(d.sessions, sd.sessions)
            d.tool_calls = max(d.tool_calls, sd.tool_calls)
            d.output_tokens = sd.output_tokens
        else:
            daily_map[sd.day] = sd

    # For days with no session data, fall back to the stats-cache figure.
    for dt, ot in sc_daily_output.items():
        if dt in session_days:
            continue
        if dt in daily_map:
            daily_map[dt].output_tokens = ot
        else:
            daily_map[dt] = DayActivity(day=dt, output_tokens=ot)

    daily = sorted(daily_map.values(), key=lambda x: x.day)

    total_sessions = st.get("totalSessions", 0)
    total_messages = st.get("totalMessages", 0)
    total_tool_calls = sum(d.get("toolCallCount", 0) for d in da)

    # Update totals from session-derived daily (stats-cache may be stale)
    session_total_msgs = sum(sd.messages for sd in session_daily)
    session_total_sess = sum(sd.sessions for sd in session_daily)
    session_total_tools = sum(sd.tool_calls for sd in session_daily)
    total_sessions = max(total_sessions, session_total_sess)
    total_messages = max(total_messages, session_total_msgs)
    total_tool_calls = max(total_tool_calls, session_total_tools)
    jsonl_tokens, jsonl_models = _load_all_tokens(projects_dirs)

    # Build stats-cache per-model breakdowns.
    # stats-cache.json doesn't expose the 5m/1h TTL split; Claude Code uses 1h
    # exclusively today, so we treat all cache-write tokens from this source as 1h.
    sc_models: dict[str, TokenBreakdown] = {}
    for m, u in mu.items():
        cw = u.get("cacheCreationInputTokens", 0)
        sc_models[m] = TokenBreakdown(
            input_tokens=u.get("inputTokens", 0),
            output_tokens=u.get("outputTokens", 0),
            cache_read_tokens=u.get("cacheReadInputTokens", 0),
            cache_write_tokens=cw,
            cache_write_1h_tokens=cw,
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
    # Track models with no PRICE match (mirrors the Codex parser) so unpriced
    # token usage surfaces as a warning instead of silently costing $0.
    unpriced_models: set[str] = set()
    unpriced_tokens = 0
    for m, tb in models.items():
        total_tokens.add(tb)
        pk = _pkey(m)
        c = 0.0
        if pk and pk in PRICE:
            p = PRICE[pk]
            cr_price, cw_5m_price, cw_1h_price = _cache_rates(pk, p[0])
            cw_cost = _cw_cost(
                tb.cache_write_tokens, tb.cache_write_1h_tokens, cw_5m_price, cw_1h_price
            )
            c = (
                tb.input_tokens * p[0]
                + tb.output_tokens * p[1]
                + tb.cache_read_tokens * cr_price
            ) / 1e6 + cw_cost
            cb.input_tokens += tb.input_tokens
            cb.output_tokens += tb.output_tokens
            cb.cache_read_tokens += tb.cache_read_tokens
            cb.cache_write_tokens += tb.cache_write_tokens
            cb.input_cost += tb.input_tokens * p[0] / 1e6
            cb.output_cost += tb.output_tokens * p[1] / 1e6
            cb.cache_read_cost += tb.cache_read_tokens * cr_price / 1e6
            cb.cache_write_cost += cw_cost
        else:
            unpriced_models.add(m)
            unpriced_tokens += tb.total
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
    projects = _load_projects_from_sessions(_bases)

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
        unpriced_models=unpriced_models,
        unpriced_tokens=unpriced_tokens,
        extra={"tier": tier},
    )
