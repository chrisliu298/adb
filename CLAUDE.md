# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

CLI tool that aggregates Claude Code, Codex, and Grok Build CLI usage statistics across multiple machines. Reads local files written by Claude Code (`~/.claude/`), Codex (`~/.codex/`), and Grok Build (`~/.grok/`) — no API keys needed for basic stats. Optionally fetches rate limit data via Anthropic OAuth (auto-detected from macOS Keychain).

## Commands

```bash
# Setup
uv sync

# Run (all machines)
uv run python adb.py

# Local only / specific remotes
uv run python adb.py local
uv run python adb.py local server1

# Sync remote machine data before running
./sync.sh

# Tests
uv run pytest
```

Python 3.10+. Runtime dependencies: `httpx`, `orjson`, `rich`. Dev dependency: `pytest`.

## Architecture

**Data flow:** `sync.sh` pulls remotes → `.cache/remotes/<host>/` (disposable airlock) → `ingest.sh` folds local homes, remote mirrors, Codex archived sessions, and the Time Machine recovery archive into **`data/<agent>/<bucket>/`** (the in-repo, gitignored, append-only **source of truth**). `adb.py` reads tokens **only from `data/`** (plus a live-local overlay for freshness), merges multi-machine `ToolStats`, and prints 6 terminal sections + a header. This decouples the lifetime total from the live homes/mirrors, which silently lose sessions (Claude Code's 30-day `cleanupPeriodDays` keeps reverting). A **floor guard** (`parser/floor.py`) records each full run's per-tool high-water and refuses to ever display a lower lifetime total (loud banner + hold-at-floor; `--rebaseline` to accept a lower value).

**Source of truth (`data/`) is never deleted or shortened.** `ingest.sh` copies append-only Claude/Codex JSONL with `rsync --append-verify` (only ever extends a file) and never `--delete`s, so a session lost from a live source stays counted forever. Read-side dedup (Claude `msg.id` / Codex `session_meta.id`) collapses the overlap between buckets and the live overlay.

### Key files

- `adb.py` — Entry point. Loads local + remote data, merges `ToolStats` from multiple machines, formats and prints all output sections using `rich` (panels, tables, bar charts, sparklines). All formatting helpers (`fmt_tokens`, `fmt_cost`, `fmt_duration`) live here.
- `parser/types.py` — Shared dataclasses: `ToolStats` (the unified stats container), `TokenBreakdown`, `CostBreakdown`, `DayActivity`, `ProjectInfo`, `RateLimitInfo`.
- `parser/parsers/claude.py` — Parses `~/.claude/stats-cache.json`, `~/.claude/history.jsonl`, and session JSONL files under `~/.claude/projects/`. Handles cost computation using hardcoded model pricing (`PRICE` dict). Fetches rate limits from Anthropic OAuth API via macOS Keychain credentials.
- `parser/parsers/codex.py` — Parses Codex session JSONL under `~/.codex/sessions/` and `~/.codex/archived_sessions/`. Uses delta-based token accounting from cumulative snapshots. Normalizes model names (strips `-codex`, `-latest`, date suffixes). Has its own `MODEL_PRICING` dict. `parse()` accepts `sessions_dirs` (a list of bases) so a remote host can be read as its rsync mirror plus archived sources together; overlapping sessions are collapsed by `session_meta.id`, mirroring the Claude `projects_base` list pattern.
- `parser/parsers/grok.py` — Parses `~/.grok/sessions/<enc-cwd>/<uuid>/` dirs. Grok Build CLI does NOT persist a token breakdown — the only token figure is a per-session context-window snapshot (`signals.json → contextTokensUsed`, falling back to `max(updates.jsonl _meta.totalTokens)` when signals is absent). That snapshot is treated as input tokens (no output/cache split exists). Has its own `MODEL_PRICING` dict.
- `sync.sh` — pulls Claude/Codex/Grok data from the hosts in `remotes.conf` into `.cache/remotes/<host>/` (a **disposable** rsync landing zone; append-only, no `--delete`), then calls `./ingest.sh`. The mirror is no longer authoritative — it is the corruption airlock between the wire and the truth.
- `ingest.sh` — folds every source adb counts (local homes + `.cache/remotes` mirror + Codex archived sessions + the `~/.claude-session-recovery-20260606` recovery archive) into the `data/` source of truth. **Never-shrink**: append-only Claude/Codex JSONL via `rsync --append-verify` (extends only, never truncates); Grok's rewritten `signals.json` via plain copy. Never `--delete`s. Idempotent; run by `sync.sh`.
- `data/` — gitignored, append-only **source of truth**; `data/<agent>/{local,local-agent-mode,<host>}/`, each bucket a parser-native tree (`.meta/` holds `stats-cache.json`/`history.jsonl`). adb reads tokens from here, not the live/mirror sources. Per-host buckets preserve the MACHINES breakdown; cross-bucket overlap is deduped by `msg.id`/`session_meta.id`.
- `parser/floor.py` — the floor guard: records each full run's per-tool token high-water in **`data/.meta/adb-floor.json`** (the DURABLE store, not the disposable `.cache/`, so a routine cache clear can't disarm it; atomic write + re-read-merge for races; fail-closed if unreadable). If a later full run computes lower, it holds the displayed *header* lifetime cell at the high-water (the sections below still show the real computed values) and prints a DATA-LOSS banner; `--rebaseline` accepts a lower value (full run only). The hard "never silently decrease" guarantee, independent of storage.

### Multi-machine merge

`load_all()` in `adb.py` collects `ToolStats` from local + each remote host, then `_merge_two()` combines them pairwise. Per-machine breakdown is preserved in `MachineData` for the MACHINES section.

### Cost computation

- **Claude:** Cost = tokens × per-model price from `PRICE` dict. By default cache read is at 0.1× input price and cache write is TTL-aware: 5-minute buckets at 1.25× input, 1-hour buckets at 2× input. The split is read per-message from `usage.cache_creation.ephemeral_{5m,1h}_input_tokens`; when only the summed `cacheCreationInputTokens` is available (stats-cache.json path), tokens are treated as 1-hour since that's what Claude Code currently uses. Non-Anthropic models routed through Claude Code (e.g. DeepSeek via `ANTHROPIC_BASE_URL`) override cache pricing via `CACHE_OVERRIDES` — absolute `(cache_read, cw_5m, cw_1h)` $/MTok per model. Per-project costs are computed by parsing individual session JSONL files.
- **Codex:** Token snapshots are cumulative counters; the parser computes deltas between consecutive snapshots, handling counter resets. Model names are normalized before pricing lookup.
- **Grok:** No input/output/cache breakdown exists in the data, so cost is a *notional* estimate: per-session `contextTokensUsed` × the model's full input rate (the most defensible single number). `grok-build` is tiered by context size (≤200K → $1/M, >200K → $2/M), applied per session. `grok-composer-2.5-fast` has no public xAI per-token rate — it's priced with the Cursor Composer 2.5 "fast" tier ($3/$15) as a proxy. Output and cache are always 0 (not recorded), so Grok cost is a lower bound for multi-call agentic sessions.
- **Recent section:** Estimates cost by multiplying output tokens by a global cost-per-output-token ratio (total_cost / total_output_tokens), not by re-pricing each model.
- **Per-project rollup invariant:** Every parser's project rollup (`_build_projects` / `_load_projects_from_sessions`) must be *cumulative across all sessions* sharing a repo/cwd and return all projects (the dashboard does the cross-machine merge + top-N). A per-session or per-bucket-truncated rollup silently undercounts the shared Projects panel. Match this when adding a new agent's project support.

### Performance

Parsing is optimized with: `orjson` for fast JSON parsing, byte-level pre-filtering to skip irrelevant JSONL lines before parsing, incremental disk caches in `.cache/` keyed by file size/mtime (warm runs ~0.3s vs ~6s cold), and `ThreadPoolExecutor` to parse machines in parallel. Cache files are auto-generated; delete `.cache/*.json` to force a full re-parse.

### Adding new model pricing

- Claude: Add entry to `PRICE` dict in `parser/parsers/claude.py` (key format: `"model-name"`, value: `[input_per_mtok, output_per_mtok]`). Update `_pkey()` if the model name pattern differs. If the model has non-Anthropic cache pricing (cache_read ≠ 0.1× input, or no cache-write premium), add an entry to `CACHE_OVERRIDES` with absolute `(cache_read, cw_5m, cw_1h)` $/MTok prices.
- Codex: Add entry to `MODEL_PRICING` dict in `parser/parsers/codex.py`. Update `_normalize_model()` if the model has a new suffix pattern.
- Grok: Add entry to `MODEL_PRICING` dict in `parser/parsers/grok.py` (keyed by exact model id, e.g. `"grok-build"`). Set `long_ctx_threshold` / `input_usd_per_mtok_long` for tiered models. Only `input_usd_per_mtok` is used for cost (no output/cache tokens in the data).
