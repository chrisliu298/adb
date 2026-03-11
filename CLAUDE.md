# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

CLI tool that aggregates Claude Code and Codex usage statistics across multiple machines. Reads local files written by Claude Code (`~/.claude/`) and Codex (`~/.codex/`) — no API keys needed for basic stats. Optionally fetches rate limit data via Anthropic OAuth (auto-detected from macOS Keychain).

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
- `parser/parsers/codex.py` — Parses `~/.codex/sessions/*.jsonl`. Uses delta-based token accounting from cumulative snapshots. Normalizes model names (strips `-codex`, `-latest`, date suffixes). Has its own `MODEL_PRICING` dict.
- `sync.sh` — rsync script that pulls Claude/Codex data from remote hosts listed in `remotes.conf` into `.cache/remotes/<host>/`.

### Multi-machine merge

`load_all()` in `adb.py` collects `ToolStats` from local + each remote host, then `_merge_two()` combines them pairwise. Per-machine breakdown is preserved in `MachineData` for the MACHINES section.

### Cost computation

- **Claude:** Cost = tokens × per-model price from `PRICE` dict. Cache read at 0.1× input price, cache write at 1.25× (configurable via `CLAUDE_CACHE_WRITE_MULTIPLIER` env var). Per-project costs are computed by parsing individual session JSONL files.
- **Codex:** Token snapshots are cumulative counters; the parser computes deltas between consecutive snapshots, handling counter resets. Model names are normalized before pricing lookup.
- **Recent section:** Estimates cost by multiplying output tokens by a global cost-per-output-token ratio (total_cost / total_output_tokens), not by re-pricing each model.

### Performance

Parsing is optimized with: `orjson` for fast JSON parsing, byte-level pre-filtering to skip irrelevant JSONL lines before parsing, incremental disk caches in `.cache/` keyed by file size/mtime (warm runs ~0.3s vs ~6s cold), and `ThreadPoolExecutor` to parse machines in parallel. Cache files are auto-generated; delete `.cache/*.json` to force a full re-parse.

### Adding new model pricing

- Claude: Add entry to `PRICE` dict in `parser/parsers/claude.py` (key format: `"model-name"`, value: `[input_per_mtok, output_per_mtok]`). Update `_pkey()` if the model name pattern differs.
- Codex: Add entry to `MODEL_PRICING` dict in `parser/parsers/codex.py`. Update `_normalize_model()` if the model has a new suffix pattern.
