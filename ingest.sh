#!/usr/bin/env bash
# Ingest every session adb counts — Claude / Codex / Grok, local + each remote
# (rsync mirror + .remote-<host> staging) + the one-time Time Machine recovery
# archive — into the in-repo append-only source of truth at data/.
#
# NEVER-SHRINK: append-only session JSONL (Claude/Codex) is copied with
# `rsync --append-verify`, which only ever EXTENDS an existing file and verifies
# the shared prefix — a shorter/rotated/corrupt source can never truncate the
# stored copy. The store is never pruned (no --delete), so a session deleted from
# a live source (30-day cleanup, rotation) stays counted forever. adb reads ONLY
# from data/, so it is immune to the live homes/mirrors losing data.
# Idempotent; sync.sh calls it automatically after pulling remotes.
set -u

REPO="$(cd "$(dirname "$0")" && pwd)"
DATA="$REPO/data"
CACHE="$REPO/.cache/remotes"
RECOVERY="$HOME/.claude-session-recovery-20260606"   # frozen TM recovery (extra source)
HOSTS="$(grep -vE '^[[:space:]]*(#|$)' "$REPO/remotes.conf" 2>/dev/null | tr '\n' ' ')"
[ -z "${HOSTS// /}" ] && HOSTS="l40s macmini"

# Append-only JSONL trees (Claude/Codex): --append-verify never shortens a file.
addj() {  # addj <src-dir> <dst-dir> [extra rsync args...]
    local src="$1" dst="$2"; shift 2
    [ -d "$src" ] || return 0; mkdir -p "$dst"
    rsync -a --append-verify "$@" "$src"/ "$dst"/ 2>/dev/null || true
}
# Non-append small files (Grok signals.json): plain copy, never deletes. Grok
# tokens are a tiny notional figure; a rewritten signals.json is harmless.
addp() {  # addp <src-dir> <dst-dir>
    local src="$1" dst="$2"
    [ -d "$src" ] || return 0; mkdir -p "$dst"
    rsync -a "$src"/ "$dst"/ 2>/dev/null || true
}
addmeta() {  # addmeta <dst-dir> <file>...  (stats-cache/history; rewritten, not append-only)
    local dst="$1"; shift; mkdir -p "$dst"
    for f in "$@"; do [ -f "$f" ] && cp -p "$f" "$dst/" 2>/dev/null; done
}

echo "Ingesting into $DATA ..."

# ---- Claude ----  (add the longest source — the live mirror — first)
addj "$HOME/.claude/projects"  "$DATA/claude/local"  --exclude='.remote-*'
addmeta "$DATA/claude/local/.meta" "$HOME/.claude/stats-cache.json" "$HOME/.claude/history.jsonl"
LAM="$HOME/Library/Application Support/Claude/local-agent-mode-sessions"
if [ -d "$LAM" ]; then
    while IFS= read -r d; do addj "$d" "$DATA/claude/local-agent-mode"; done \
        < <(find "$LAM" -type d -path '*/.claude/projects' 2>/dev/null)
fi
for h in $HOSTS; do
    addj "$CACHE/$h/claude/projects"            "$DATA/claude/$h"
    addj "$RECOVERY/mirror/$h/claude/projects"  "$DATA/claude/$h"
    addmeta "$DATA/claude/$h/.meta" "$CACHE/$h/claude/stats-cache.json" "$CACHE/$h/claude/history.jsonl"
done
# Note: the ~/.{claude,codex}/.../.remote-<host> recall-sync staging dirs (frozen
# 2026-04-24, recall-sync removed) were folded into data/ and then deleted as
# redundant, so they are no longer ingest sources.

# ---- Codex ----
addj "$HOME/.codex/sessions"  "$DATA/codex/local"  --exclude='.remote-*'
for h in $HOSTS; do
    addj "$CACHE/$h/codex/sessions"         "$DATA/codex/$h"
done

# ---- Grok ----  (signals.json is rewritten, not append-only -> plain copy)
addp "$HOME/.grok/sessions"  "$DATA/grok/local"
for h in $HOSTS; do
    addp "$CACHE/$h/grok/sessions"  "$DATA/grok/$h"
done

echo "Done. data/ session-file counts:"
for agent in claude codex grok; do
    for d in "$DATA/$agent"/*/; do
        [ -d "$d" ] || continue
        printf "  %-22s %s\n" "$agent/$(basename "$d")" "$(find "$d" -name '*.jsonl' 2>/dev/null | wc -l | tr -d ' ')"
    done
done
