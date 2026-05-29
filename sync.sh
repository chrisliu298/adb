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
    mkdir -p "$dest/claude" "$dest/codex"

    rsync -az --timeout=10 -e "$RSH" \
        "$host:.claude/stats-cache.json" \
        "$dest/claude/stats-cache.json" 2>/dev/null || true

    rsync -az --timeout=10 -e "$RSH" \
        "$host:.claude/history.jsonl" \
        "$dest/claude/history.jsonl" 2>/dev/null || true

    rsync -az --timeout=10 -e "$RSH" \
        "$host:.claude.json" \
        "$dest/claude/.claude.json" 2>/dev/null || true

    rsync -az --timeout=30 -e "$RSH" \
        --include='*/' --include='*.jsonl' --exclude='*' \
        "$host:.claude/projects/" \
        "$dest/claude/projects/" 2>/dev/null || true

    rsync -az --timeout=10 -e "$RSH" \
        "$host:.codex/sessions/" \
        "$dest/codex/sessions/" 2>/dev/null || true

    # Some tools run Codex against a shadow CODEX_HOME (e.g. task-synth uses
    # ~/.task-synth-codex, taskforge uses ~/.codex-taskforge) so their rollouts
    # never land in ~/.codex/sessions. Merge those into the same sessions tree —
    # rollouts are self-contained and there's no --delete, so merging is safe.
    for ch in .task-synth-codex .codex-taskforge; do
        rsync -az --timeout=10 -e "$RSH" \
            "$host:$ch/sessions/" \
            "$dest/codex/sessions/" 2>/dev/null || true
    done

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
