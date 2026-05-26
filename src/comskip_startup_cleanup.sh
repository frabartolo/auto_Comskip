# comskip_startup_cleanup.sh – von auto_process*.sh eingebunden (nicht direkt starten)
#
# Räumt vor jedem Lauf lokale Temp-Reste und verwaiste Locks auf (Multi-Worker-sicher).
# Deaktivieren: COMSKIP_SKIP_CLEANUP=1

run_comskip_startup_cleanup() {
    local log_fn="${1:-}"

    [ "${COMSKIP_SKIP_CLEANUP:-0}" = "1" ] && return 0

    local cleanup_script
    cleanup_script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/cleanup_comskip.sh"
    [ -x "$cleanup_script" ] || return 0

    _cleanup_log() {
        if [ -n "$log_fn" ] && type "$log_fn" >/dev/null 2>&1; then
            "$log_fn" "$1"
        else
            echo "$1"
        fi
    }

    _cleanup_log "Start-Cleanup (abgebrochene Jobs)..."
    while IFS= read -r line; do
        [ -n "$line" ] && _cleanup_log "  [cleanup] $line"
    done < <("$cleanup_script" --own-locks 2>&1 || true)
}
