# adb

**a**gent **d**ash**b**oard — CLI tool that aggregates Claude Code and Codex usage statistics across multiple machines.

```text
╭─  adb  ──────────────────────────────────────────────────────────────────────╮
│ $5,302 · Claude $4,262 (Max 20x) · Codex $1,040 (Pro)                        │
│ 2,377 sessions · 8.13B tokens · 68 days · $77.97/day · 4,231 msgs/day · 35   │
│ sess/day · 68-day streak                                                     │
│ Claude  5-Hour ██░░░░░░░░  17% (1h 40m) · 7-Day ██████░░░░  57% (2d 6h)      │
│ Codex   5-Hour ░░░░░░░░░░   3% (now)    · 7-Day ░░░░░░░░░░   4% (6d 7h)      │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─  Recent  ───────────────────────────────────────────────────────────────────╮
│                    Today          This Week        This Month      All Time  │
│                   Mar 10          Mar 09-10           Mar 01-   2026-01-02~  │
│ ──────────────────────────────────────────────────────────────────────────── │
│  Messages            400              1,943             3,460       287,714  │
│  Sessions             37                185               313         2,377  │
│  Tool Calls          611              4,181             8,548        85,402  │
│  Tokens           ~57.8M            ~376.9M           ~908.6M         8.13B  │
│  Est. Cost       ~$37.70   ~$245.80 (↑819%)   ~$592.50 (↓56%)     $5,301.78  │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─  Models  ───────────────────────────────────────────────────────────────────╮
│  Model                Total    Input   Output    Cache         Cost       %  │
│ ──────────────────────────────────────────────────────────────────────────── │
│  claude-opus-4-6      2.67B     1.4M     2.5M    2.66B    $2,234.64   42.1%  │
│  claude-opus-4-5-…    2.33B     2.1M     1.8M    2.32B    $1,999.05   37.7%  │
│  gpt-5.3-codex        2.02B   124.2M     9.8M    1.88B      $683.35   12.9%  │
│  ...                                                                         │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─  Cost  ─────────────────────────────╮╭─  Activity  ─────────────────────────╮
│ Cache Rd  $2,842 ██████░░░░░░ 53.6%  ││ Daily Avg 4,231 msgs · 35.0 sess     │
│ Cache Wr  $1,769 ████░░░░░░░░ 33.4%  ││ Weekday   5,015 msgs/day (45d)       │
│ Input       $348 ░░░░░░░░░░░░  6.6%  ││ Weekend   3,266 msgs/day (19d)       │
│ Output      $342 ░░░░░░░░░░░░  6.5%  ││ Busiest   2026-02-04 (26,589)        │
│                                      ││ Peak Hour 20:00-21:00                │
│ Cache Hit   97.4% saved ~$25,579     ││ Tok/Hour  ▄▁▁▁▁▁▁▁▁▁▃▄▅▆▅▃▂▃▃▆█▄▇▆   │
│ Cost/Day    $77.97                   ││ Streak    68 days                    │
│ Cost/Sess   $2.23                    ││ Longest   212h 23m (155 msgs)        │
│ Cost/Msg    $0.02                    ││ Avg Sess  121.0 msgs · 19.3 turns    │
│ Cost/1K Tok $0.0007                  ││ Last 14d  ▅▃▂▁▁▁▁▁▁▄▆▂█▂             │
╰──────────────────────────────────────╯╰──────────────────────────────────────╯
╭─  Machines  ─────────────────────────────────────────────────────────────────╮
│  Machine           Cost        %    Sessions    $/Sess    Messages   Tokens  │
│ ──────────────────────────────────────────────────────────────────────────── │
│  local        $2,719.93    51.3%       1,388     $1.96     116,983    4.20B  │
│  server1      $2,506.14    47.3%         870     $2.88     167,426    3.85B  │
│  server2         $75.71     1.4%         119     $0.64       3,305    77.3M  │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─  Projects (Top 10)  ────────────────────────────────────────────────────────╮
│  Project                           Cost   Output Tokens    Duration   $/Hr   │
│ ──────────────────────────────────────────────────────────────────────────── │
│  project-a                      $572.98            4.1M   1205h 22m   $0.48  │
│  project-b                      $269.95            1.8M    231h 49m   $1.16  │
│  dotfiles                       $244.56            2.0M     54h 49m   $4.46  │
│  ...                                                                         │
╰──────────────────────────────────────────────────────────────────────────────╯
```

## 1. Installation

**Python 3.10+ with uv.**

```bash
git clone https://github.com/chrisliu298/adb.git
cd adb
uv venv && source .venv/bin/activate
uv pip install -e .
```

## 2. Usage

**Run to print terminal stats.**

```bash
adb                              # all machines (local + remotes)
adb local                        # local machine only
adb local server1                # local + specific remotes
```

## 3. Multi-Machine Setup

**Aggregate stats from remote machines via rsync over SSH.**

```text
┌──────────┐     rsync      ┌──────────────────────────────┐
│ server1  │ ──────────────→│                              │
└──────────┘                │  local machine               │
┌──────────┐     rsync      │  .cache/remotes/<host>/      │
│ server2  │ ──────────────→│  ├── claude/                 │
└──────────┘                │  │   ├── stats-cache.json    │
                            │  │   ├── history.jsonl       │
                            │  │   ├── .claude.json        │
                            │  │   └── projects/           │
                            │  └── codex/                  │
                            │      └── sessions/           │
                            └──────────────────────────────┘
```

1. Copy `remotes.conf.example` to `remotes.conf` and add your SSH hostnames (one per line):
   ```bash
   cp remotes.conf.example remotes.conf
   # edit remotes.conf with your hosts
   ```
2. Run `./sync.sh` to pull usage data from all remotes
3. Run `python adb.py` — remote data is automatically included

## 4. Output Sections

**6 sections, ordered from most time-sensitive to most stable.**

| Section | What it shows |
|---------|---------------|
| RECENT | Today / This Week / This Month / All Time: messages, sessions, tool calls, tokens, cost with deltas vs prior period |
| MODELS | Per-model token breakdown (input/output/cache) and cost |
| COST | Cache read/write/input/output bar chart, cache hit rate, cost per day/session/message/token |
| ACTIVITY | Daily/weekday/weekend averages, busiest day, peak hour, tokens-by-hour sparkline, streak, longest session, 14-day sparkline |
| MACHINES | Per-machine cost, sessions, $/session, messages, tokens (when multiple machines) |
| PROJECTS | Top 10 projects by cost with output tokens, duration, $/hour |

Rate limit utilization (Claude and Codex) is shown in the header when available.

## 5. Data Sources

**Reads local files written by Claude Code and Codex — no API keys needed for basic stats.**

- `~/.claude/stats-cache.json` — daily activity, model usage, session counts
- `~/.claude/history.jsonl` — session timestamps (streak fallback)
- `~/.claude/projects/*/` — session conversation logs (per-project cost computation)
- `~/.codex/sessions/` — Codex session files
- Anthropic OAuth API — rate limit utilization (optional, auto-detected from macOS Keychain)

## 6. Project Structure

```text
adb/
├── adb.py                      # Entry point, output formatting, multi-machine merge
├── sync.sh                     # rsync script for pulling remote machine data
├── remotes.conf                # list of SSH hostnames to sync from
├── parser/
│   ├── parsers/
│   │   ├── claude.py           # Claude Code stats + session log parsing
│   │   └── codex.py            # Codex session parsing
│   └── types.py                # Shared dataclasses (ToolStats, TokenBreakdown, etc.)
└── pyproject.toml
```
