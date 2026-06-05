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

    # --delete prunes ONLY this local mirror ($dest), never the remote source:
    # rsync --delete removes extraneous files on the RECEIVER, so original data on
    # $host is never touched. It drops cached *.jsonl that were pruned/rotated off
    # the remote so they aren't counted forever. Single source, so safe; the
    # --exclude='*' rule protects non-jsonl receiver files. The local recall-sync
    # staging dir (.remote-<host>) is a separate tree, untouched.
    rsync -az --timeout=30 --delete -e "$RSH" \
        --include='*/' --include='*.jsonl' --exclude='*' \
        "$host:.claude/projects/" \
        "$dest/claude/projects/" 2>/dev/null || true

    # --delete here too (Codex has no cross-file dedup, so stale rollouts would
    # double-count permanently). Same guarantee: it only prunes the mirror $dest,
    # never $host. The shadow homes below merge into this same dir, so this MUST
    # run first: --delete mirrors the remote's ~/.codex/sessions (clearing last
    # run's stale primary AND shadow files from the mirror), then the loop re-adds
    # the current shadow rollouts. Order matters — do not move the shadow loop above.
    rsync -az --timeout=10 --delete -e "$RSH" \
        "$host:.codex/sessions/" \
        "$dest/codex/sessions/" 2>/dev/null || true

    # Some tools run Codex against a shadow CODEX_HOME (e.g. task-synth uses
    # ~/.task-synth-codex, taskforge uses ~/.codex-taskforge) so their rollouts
    # never land in ~/.codex/sessions. Merge those into the same sessions tree.
    # No --delete on these: each would delete the others' files (they share the
    # dest dir); the primary --delete above already cleared stale shadow rollouts.
    for ch in .task-synth-codex .codex-taskforge; do
        rsync -az --timeout=10 -e "$RSH" \
            "$host:$ch/sessions/" \
            "$dest/codex/sessions/" 2>/dev/null || true
    done

    # Grok Build CLI: one self-contained dir per session, no cross-file dedup
    # needed. --delete prunes ONLY this mirror ($dest), never $host — same
    # guarantee as the blocks above. Drops sessions rotated off the remote so
    # they aren't counted forever.
    rsync -az --timeout=10 --delete -e "$RSH" \
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
