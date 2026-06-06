# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

CLI tool that aggregates Claude Code, Codex, and Grok Build CLI usage statistics across multiple machines. Reads local files written by Claude Code (`~/.claude/`), Codex (`~/.codex/`), and Grok Build (`~/.grok/`) — no API keys needed for basic stats. Optionally fetches rate limit data via Anthropic OAuth (auto-detected from macOS Keychain).

## Commands

```bash
# Setup
uv venv && source .venv/bin/activate
uv pip install -e .

# Run (all machines)
python adb.py

# Local only / specific remotes
python adb.py local
python adb.py local server1

# Sync remote machine data before running
./sync.sh
```

No tests, no linter, no build step. Python 3.10+, dependencies: `httpx`, `orjson`, `rich`.

## Architecture

**Data flow:** Parsers read raw files → produce `ToolStats` dataclass → `adb.py` merges multi-machine data → prints 6 terminal sections (RECENT, MODELS, COST+ACTIVITY side-by-side, MACHINES, PROJECTS) plus a header with rate limits.

### Key files

- `adb.py` — Entry point. Loads local + remote data, merges `ToolStats` from multiple machines, formats and prints all output sections using `rich` (panels, tables, bar charts, sparklines). All formatting helpers (`fmt_tokens`, `fmt_cost`, `fmt_duration`) live here.
- `parser/types.py` — Shared dataclasses: `ToolStats` (the unified stats container), `TokenBreakdown`, `CostBreakdown`, `DayActivity`, `ProjectInfo`, `RateLimitInfo`.
- `parser/parsers/claude.py` — Parses `~/.claude/stats-cache.json`, `~/.claude/history.jsonl`, and session JSONL files under `~/.claude/projects/`. Handles cost computation using hardcoded model pricing (`PRICE` dict). Fetches rate limits from Anthropic OAuth API via macOS Keychain credentials.
- `parser/parsers/codex.py` — Parses `~/.codex/sessions/*.jsonl`. Uses delta-based token accounting from cumulative snapshots. Normalizes model names (strips `-codex`, `-latest`, date suffixes). Has its own `MODEL_PRICING` dict. `parse()` accepts `sessions_dirs` (a list of bases) so a remote host can be read as its rsync mirror **plus** its `~/.codex/sessions/.remote-<host>` recall-sync staging dir together; overlapping sessions are collapsed by `session_meta.id`, mirroring the Claude `projects_base` list pattern.
- `parser/parsers/grok.py` — Parses `~/.grok/sessions/<enc-cwd>/<uuid>/` dirs. Grok Build CLI does NOT persist a token breakdown — the only token figure is a per-session context-window snapshot (`signals.json → contextTokensUsed`, falling back to `max(updates.jsonl _meta.totalTokens)` when signals is absent). That snapshot is treated as input tokens (no output/cache split exists). Has its own `MODEL_PRICING` dict.
- `sync.sh` — rsync script that pulls Claude/Codex/Grok data from remote hosts listed in `remotes.conf` into `.cache/remotes/<host>/`. **All three mirrors (Claude, Codex, Grok) are append-only (no `--delete`)** so sessions rotated/deleted off a remote stay counted in the cumulative total (a destructive mirror made the lifetime total *decrease* over time). Double-counting is handled at the parser level instead of by pruning: Codex by `_dedup_files_by_session` (session_meta.id), Claude by global msg.id dedup in `_aggregate_loose` (and per-host relative-path dedup for the session *count*); Grok needs none — each session is a self-contained `<enc-cwd>/<uuid>` directory with no cross-file overlap. The Claude mirror previously kept `--delete` on the premise that the `.remote-<host>` recall-sync staging dir preserved rotated sessions — but recall-sync was removed 2026-04-26 (staging frozen at 2026-04-24), so that mirror was the actual source of the observed Claude-side decrease and is now append-only too. The parser still reads the frozen staging dir as a second source for pre-2026-04-24 sessions already rotated off the remotes.

### Multi-machine merge

`load_all()` in `adb.py` collects `ToolStats` from local + each remote host, then `_merge_two()` combines them pairwise. Per-machine breakdown is preserved in `MachineData` for the MACHINES section.

### Cost computation

- **Claude:** Cost = tokens × per-model price from `PRICE` dict. By default cache read is at 0.1× input price and cache write is TTL-aware: 5-minute buckets at 1.25× input, 1-hour buckets at 2× input. The split is read per-message from `usage.cache_creation.ephemeral_{5m,1h}_input_tokens`; when only the summed `cacheCreationInputTokens` is available (stats-cache.json path), tokens are treated as 1-hour since that's what Claude Code currently uses. Non-Anthropic models routed through Claude Code (e.g. DeepSeek via `ANTHROPIC_BASE_URL`) override cache pricing via `CACHE_OVERRIDES` — absolute `(cache_read, cw_5m, cw_1h)` $/MTok per model. Per-project costs are computed by parsing individual session JSONL files.
- **Codex:** Token snapshots are cumulative counters; the parser computes deltas between consecutive snapshots, handling counter resets. Model names are normalized before pricing lookup.
- **Grok:** No input/output/cache breakdown exists in the data, so cost is a *notional* estimate: per-session `contextTokensUsed` × the model's full input rate (the most defensible single number). `grok-build` is tiered by context size (≤200K → $1/M, >200K → $2/M), applied per session. `grok-composer-2.5-fast` has no public xAI per-token rate — it's priced with the Cursor Composer 2.5 "fast" tier ($3/$15) as a proxy. Output and cache are always 0 (not recorded), so Grok cost is a lower bound for multi-call agentic sessions.
- **Recent section:** Estimates cost by multiplying output tokens by a global cost-per-output-token ratio (total_cost / total_output_tokens), not by re-pricing each model.

### Performance

Parsing is optimized with: `orjson` for fast JSON parsing, byte-level pre-filtering to skip irrelevant JSONL lines before parsing, incremental disk caches in `.cache/` keyed by file size/mtime (warm runs ~0.3s vs ~6s cold), and `ThreadPoolExecutor` to parse machines in parallel. Cache files are auto-generated; delete `.cache/*.json` to force a full re-parse.

### Adding new model pricing

- Claude: Add entry to `PRICE` dict in `parser/parsers/claude.py` (key format: `"model-name"`, value: `[input_per_mtok, output_per_mtok]`). Update `_pkey()` if the model name pattern differs. If the model has non-Anthropic cache pricing (cache_read ≠ 0.1× input, or no cache-write premium), add an entry to `CACHE_OVERRIDES` with absolute `(cache_read, cw_5m, cw_1h)` $/MTok prices.
- Codex: Add entry to `MODEL_PRICING` dict in `parser/parsers/codex.py`. Update `_normalize_model()` if the model has a new suffix pattern.
- Grok: Add entry to `MODEL_PRICING` dict in `parser/parsers/grok.py` (keyed by exact model id, e.g. `"grok-build"`). Set `long_ctx_threshold` / `input_usd_per_mtok_long` for tiered models. Only `input_usd_per_mtok` is used for cost (no output/cache tokens in the data).
