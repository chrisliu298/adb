#!/usr/bin/env bash
# Sync Claude Code and Codex usage data from remote machines.
# Usage: ./sync.sh
#
# Configure remote hosts in remotes.conf (one hostname per line).
# Data is stored in .cache/remotes/<host>/.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CACHE_DIR="$SCRIPT_DIR/.cache/remotes"
CONF="$SCRIPT_DIR/remotes.conf"

if [[ ! -f "$CONF" ]]; then
    echo "No remotes.conf found. Create one with hostnames (one per line)."
    exit 1
fi

while IFS= read -r host || [[ -n "$host" ]]; do
    # Skip empty lines and comments
    [[ -z "$host" || "$host" == \#* ]] && continue

    echo "Syncing $host..."
    dest="$CACHE_DIR/$host"
    mkdir -p "$dest/claude" "$dest/codex"

    # Claude Code: stats-cache, history, projects metadata
    rsync -az --timeout=10 \
        "$host:.claude/stats-cache.json" \
        "$dest/claude/stats-cache.json" 2>/dev/null || true

    rsync -az --timeout=10 \
        "$host:.claude/history.jsonl" \
        "$dest/claude/history.jsonl" 2>/dev/null || true

    # .claude.json lives in $HOME, contains project cost data
    rsync -az --timeout=10 \
        "$host:.claude.json" \
        "$dest/claude/.claude.json" 2>/dev/null || true

    # Claude Code: project session logs (JSONL only, skip tool-results)
    rsync -az --timeout=30 \
        --include='*/' --include='*.jsonl' --exclude='*' \
        "$host:.claude/projects/" \
        "$dest/claude/projects/" 2>/dev/null || true

    # Codex: session files
    rsync -az --timeout=10 \
        "$host:.codex/sessions/" \
        "$dest/codex/sessions/" 2>/dev/null || true

    echo "  Done."
done < "$CONF"

echo "Sync complete. Data in $CACHE_DIR"
