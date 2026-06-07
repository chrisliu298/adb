#!/usr/bin/env bash
# Sync Claude Code and Codex usage data from remote machines.
# Usage: ./sync.sh
#
# Configure remote hosts in remotes.conf (one hostname per line).
# Data is stored in .cache/remotes/<host>/.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CACHE_DIR="$SCRIPT_DIR/.cache/remotes"
CONF="$SCRIPT_DIR/remotes.conf"

if [[ ! -f "$CONF" ]]; then
    echo "No remotes.conf found. Create one with hostnames (one per line)."
    exit 1
fi

# Bound SSH connect time (default is minutes on flaky networks) and never
# prompt for a password — fail fast if auth doesn't work non-interactively.
RSH='ssh -o ConnectTimeout=5 -o BatchMode=yes'

sync_host() {
    local host="$1"
    local dest="$CACHE_DIR/$host"
    echo "Syncing $host..."
    mkdir -p "$dest/claude" "$dest/codex" "$dest/grok"

    rsync -az --timeout=10 -e "$RSH" \
        "$host:.claude/stats-cache.json" \
        "$dest/claude/stats-cache.json" 2>/dev/null || true

    rsync -az --timeout=10 -e "$RSH" \
        "$host:.claude/history.jsonl" \
        "$dest/claude/history.jsonl" 2>/dev/null || true

    rsync -az --timeout=10 -e "$RSH" \
        "$host:.claude.json" \
        "$dest/claude/.claude.json" 2>/dev/null || true

    # NO --delete: this mirror is append-only so Claude sessions rotated off the
    # remote stay counted (the total is cumulative/lifetime). This block used to
    # --delete on the premise that the .remote-<host> recall-sync staging dir
    # preserved rotated sessions — but recall-sync was removed 2026-04-26, so that
    # staging is frozen (newest file 2026-04-24) and no longer backs up sessions
    # created after that date. Pruning them here dropped them from every source
    # adb can read, making the lifetime total decrease daily. Append-only keeps
    # the mirror itself the preservation layer; the parser's global msg.id dedup
    # (parser/parsers/claude.py _aggregate_loose) collapses overlap with staging.
    rsync -az --timeout=30 -e "$RSH" \
        --include='*/' --include='*.jsonl' --exclude='*' \
        "$host:.claude/projects/" \
        "$dest/claude/projects/" 2>/dev/null || true

    # NO --delete: this mirror is append-only so Codex sessions rotated off the
    # remote stay counted (the total is cumulative/lifetime). Stale-rollout
    # double-counting is handled at the parser level by session_meta.id dedup
    # (parser/parsers/codex.py _dedup_files_by_session), so pruning isn't needed.
    # The shadow homes below merge into this same dir; dedup collapses overlaps.
    rsync -az --timeout=10 -e "$RSH" \
        "$host:.codex/sessions/" \
        "$dest/codex/sessions/" 2>/dev/null || true

    # Some tools run Codex against a shadow CODEX_HOME (e.g. task-synth uses
    # ~/.task-synth-codex, taskforge uses ~/.codex-taskforge) so their rollouts
    # never land in ~/.codex/sessions. Merge those into the same sessions tree.
    # No --delete (append-only); the parser's session-id dedup collapses overlap.
    for ch in .task-synth-codex .codex-taskforge; do
        rsync -az --timeout=10 -e "$RSH" \
            "$host:$ch/sessions/" \
            "$dest/codex/sessions/" 2>/dev/null || true
    done

    # Grok Build CLI: one self-contained dir per session. NO --delete — this
    # mirror is append-only so sessions rotated off the remote stay counted in
    # the cumulative/lifetime total. Grok has no .remote-<host> staging layer,
    # so the append-only mirror is its only preservation path.
    rsync -az --timeout=10 -e "$RSH" \
        "$host:.grok/sessions/" \
        "$dest/grok/sessions/" 2>/dev/null || true

    # task-synth (l40s) saves a copy of each run's Codex rollout into its task
    # dirs as codex_session.jsonl. Most overlap the shadow home above, but some
    # don't, so pull them into a dedicated subtree; the parser's session-id dedup
    # collapses the overlap. The absolute path is l40s-specific (a no-op
    # elsewhere); heavy non-task dirs are excluded so rsync doesn't walk them.
    rsync -az --timeout=60 --prune-empty-dirs -e "$RSH" \
        --exclude='.venv' --exclude='.playwright-cli' --exclude='.git' \
        --exclude='external' --exclude='data' --exclude='.relay' \
        --include='*/' --include='codex_session.jsonl' --exclude='*' \
        "$host:/mnt/data1/chrisliu/code/task-synth/" \
        "$dest/codex/sessions/task-synth/" 2>/dev/null || true

    echo "  $host done."
}

pids=()
while IFS= read -r host || [[ -n "$host" ]]; do
    [[ -z "$host" || "$host" == \#* ]] && continue
    sync_host "$host" &
    pids+=($!)
done < "$CONF"

for pid in "${pids[@]}"; do
    wait "$pid" || true
done

echo "Sync complete. Data in $CACHE_DIR"

# Ingest every synced session into the in-repo append-only source of truth
# (data/), which is what adb actually reads. Never-shrink + never-delete, so a
# session later lost from a live source (30-day cleanup, rotation) stays counted.
# Idempotent; non-fatal if it fails — the sync itself has already succeeded.
INGEST="$SCRIPT_DIR/ingest.sh"
if [[ -x "$INGEST" ]]; then
    echo "Ingesting sessions into the durable store (data/)..."
    "$INGEST" || echo "  Ingest step failed — run ./ingest.sh manually."
fi
