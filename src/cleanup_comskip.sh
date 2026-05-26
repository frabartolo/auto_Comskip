#!/bin/bash
#
# cleanup_comskip.sh – Räumt lokale Reste abgebrochener Jobs auf und entfernt
# veraltete Locks auf dem gemeinsamen Quell-Server (Multi-Worker-sicher).
#
# Lokale Temp-Dateien: nur auf DIESEM Rechner.
# Locks (Datei + Global): nur wenn älter als LOCK_TIMEOUT (wie try_claim_file)
# oder (--own-locks) Lock gehört zu diesem Host und der Worker-PID läuft nicht mehr.
#
# Aufruf:
#   ./cleanup_comskip.sh              # Standard: lokal + veraltete Locks
#   ./cleanup_comskip.sh --dry-run    # Nur anzeigen
#   ./cleanup_comskip.sh --local-only # Keine Remote-/Mount-Locks
#   ./cleanup_comskip.sh --own-locks  # Zusätzlich: eigene Locks mit toter PID
#

set -u

CRED_FILE="${CRED_FILE:-$HOME/.smbcredentials}"
SOURCE_SSH_HOST="${SOURCE_SSH_HOST:-cold-lairs}"
SOURCE_REMOTE_PATH="${SOURCE_REMOTE_PATH:-/var/opt/shares/Videos}"
SOURCE_MOUNT_DIR="${SOURCE_MOUNT_DIR:-$HOME/mount/cold-lairs-videos}"
TARGET_MOUNT_DIR="${TARGET_MOUNT_DIR:-$HOME/mount/khanhiwara-videos}"

TEMP_BASE="${TEMP_BASE:-/tmp/comskip_work}"
FAILED_UPLOAD_DIR="${FAILED_UPLOAD_DIR:-$HOME/comskip_failed_uploads}"
LOCK_TIMEOUT_MINUTES="${LOCK_TIMEOUT_MINUTES:-120}"
GLOBAL_LOCK_TIMEOUT_MINUTES="${GLOBAL_LOCK_TIMEOUT_MINUTES:-30}"
LOCAL_MIN_AGE_MINUTES="${LOCAL_MIN_AGE_MINUTES:-60}"
GLOBAL_LOCK_NAME="${GLOBAL_LOCK_NAME:-network_global}"

THIS_HOST="$(hostname)"
DRY_RUN=0
DO_LOCAL=1
DO_LOCKS=1
DO_OWN_ORPHAN_LOCKS=0

usage() {
    sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
    echo ""
    echo "Optionen:"
    echo "  --dry-run       Nur anzeigen, nichts löschen"
    echo "  --local-only    Nur lokale Temp-Dateien (keine Locks auf Quell-Server)"
    echo "  --own-locks     Eigene Locks (Hostname-$(hostname)-*) löschen, wenn PID tot"
    echo "  --locks-only    Nur Locks, keine lokalen Temp-Dateien"
    echo "  -h, --help      Diese Hilfe"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --local-only) DO_LOCKS=0 ;;
        --locks-only) DO_LOCAL=0 ;;
        --own-locks) DO_OWN_ORPHAN_LOCKS=1 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unbekannte Option: $1" >&2; usage; exit 2 ;;
    esac
    shift
done

REMOVED=0
SKIPPED_ACTIVE=0
USE_MOUNTS=0
LOCK_BASE=""
SSH_USER=""
SSH_PASS=""

log_msg() {
    echo "$1"
}

do_remove() {
    local target="$1"
    local reason="$2"
    if [ ! -e "$target" ]; then
        return 0
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
        log_msg "  [dry-run] würde entfernen: $target ($reason)"
        return 0
    fi
    if rm -rf "$target" 2>/dev/null; then
        log_msg "  ✓ entfernt: $target ($reason)"
        REMOVED=$((REMOVED + 1))
    else
        log_msg "  ✗ konnte nicht entfernen: $target"
    fi
}

pid_alive() {
    local pid="$1"
    [[ "$pid" =~ ^[0-9]+$ ]] || return 1
    kill -0 "$pid" 2>/dev/null
}

processing_running_locally() {
    pgrep -f '[a]uto_process\.sh|[a]uto_process_rsync|[a]uto_process_rsync_gpu|[c]ut_with_edl\.py' >/dev/null 2>&1
}

# --- Lokale Temp-Dateien (nur dieser Rechner) ---
cleanup_local() {
    log_msg ""
    log_msg "=== Lokale Temp-Dateien ($THIS_HOST) ==="

    # rsync: /tmp/comskip_work/<pid>/ – nur wenn PID nicht mehr läuft
    if [ -d "$TEMP_BASE" ]; then
        for work_dir in "$TEMP_BASE"/*; do
            [ -d "$work_dir" ] || continue
            base=$(basename "$work_dir")
            if [[ "$base" =~ ^[0-9]+$ ]]; then
                if pid_alive "$base"; then
                    SKIPPED_ACTIVE=$((SKIPPED_ACTIVE + 1))
                    continue
                fi
                do_remove "$work_dir" "rsync WORK_DIR, PID $base tot"
            fi
        done

        # auto_process: flache Dateien in TEMP_BASE – nur wenn kein Worker läuft
        if processing_running_locally; then
            log_msg "  -> Worker läuft lokal: überspringe flache Dateien in $TEMP_BASE"
        else
            while IFS= read -r f; do
                [ -n "$f" ] && do_remove "$f" "alt, kein lokaler Worker"
            done < <(find "$TEMP_BASE" -mindepth 1 -maxdepth 1 ! -name '[0-9]*' -mmin "+$LOCAL_MIN_AGE_MINUTES" 2>/dev/null)
        fi
    fi

    # auto_process: /tmp/comskip_preprocess_<pid>.ts
    while IFS= read -r f; do
        [ -n "$f" ] && do_remove "$f" "Vorab-Reparatur-Temp"
    done < <(find /tmp -maxdepth 1 -name 'comskip_preprocess_*' -mmin "+$LOCAL_MIN_AGE_MINUTES" 2>/dev/null)

    # cut_with_edl.py
    while IFS= read -r f; do
        [ -n "$f" ] && do_remove "$f" "FFmpeg/Python-Temp"
    done < <(find /tmp -maxdepth 1 \( -name 'repaired_*' -o -name 'ffmpeg_segments_*' \) -mmin "+$LOCAL_MIN_AGE_MINUTES" 2>/dev/null)

    # Leere verwaiste comskip_work-Basis
    if [ -d "$TEMP_BASE" ] && [ -z "$(ls -A "$TEMP_BASE" 2>/dev/null)" ]; then
        do_remove "$TEMP_BASE" "leeres Temp-Verzeichnis"
    fi
}

# --- Lock-Auswertung ---
# info: worker:epoch[:file]
lock_age_minutes() {
    local info="$1"
    local lock_time now
    lock_time=$(echo "$info" | cut -d: -f2)
    if ! [[ "$lock_time" =~ ^[0-9]+$ ]]; then
        echo 999
        return 0
    fi
    now=$(date +%s)
    echo $(( (now - lock_time) / 60 ))
}

lock_worker_id() {
    echo "$1" | cut -d: -f1
}

lock_file_path() {
    echo "$1" | cut -d: -f3-
}

should_remove_lock() {
    local info="$1"
    local lock_name="$2"
    local age_min
    age_min=$(lock_age_minutes "$info")
    local worker pid lock_host

    worker=$(lock_worker_id "$info")
    pid="${worker##*-}"
    lock_host="${worker%-*}"

    # Globaler Netzwerk-Lock
    if [ "$lock_name" = "${GLOBAL_LOCK_NAME}.lck" ]; then
        if [ "$age_min" -ge "$GLOBAL_LOCK_TIMEOUT_MINUTES" ]; then
            echo "globaler Lock veraltet (${age_min}min)"
            return 0
        fi
        return 1
    fi

    # Veraltet (wie try_claim_file auf allen Workern)
    if [ "$age_min" -ge "$LOCK_TIMEOUT_MINUTES" ]; then
        echo "Lock veraltet (${age_min}min >= ${LOCK_TIMEOUT_MINUTES}min)"
        return 0
    fi

    # Optional: eigener Host, PID auf DIESEM Rechner tot (nur wenn Cleanup hier läuft)
    if [ "$DO_OWN_ORPHAN_LOCKS" -eq 1 ] && [ "$lock_host" = "$THIS_HOST" ]; then
        if ! pid_alive "$pid"; then
            echo "eigener Worker $worker, PID $pid nicht aktiv"
            return 0
        fi
    fi

    return 1
}

remove_lock_dir() {
    local lock_dir="$1"
    local reason="$2"

    if [ "$DRY_RUN" -eq 1 ]; then
        log_msg "  [dry-run] würde Lock entfernen: $lock_dir ($reason)"
        return 0
    fi
    if [ "$USE_MOUNTS" -eq 1 ]; then
        do_remove "$lock_dir" "$reason"
        return 0
    fi
    if sshpass -p "$SSH_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no -n \
        "$SSH_USER@$SOURCE_SSH_HOST" "rm -rf '$lock_dir'" 2>/dev/null; then
        log_msg "  ✓ Lock entfernt (remote): $lock_dir ($reason)"
        REMOVED=$((REMOVED + 1))
    else
        log_msg "  ✗ Lock nicht entfernbar (remote): $lock_dir"
    fi
}

process_lock_dir() {
    local lock_dir="$1"
    local name reason info file_hint

    name=$(basename "$lock_dir")
    [ -f "$lock_dir/info" ] || return 0
    info=$(tr -d '\r' < "$lock_dir/info" | head -n 1)
    [ -n "$info" ] || return 0

    if reason=$(should_remove_lock "$info" "$name"); then
        file_hint=$(lock_file_path "$info")
        [ -n "$file_hint" ] && reason="$reason, Datei: $file_hint"
        remove_lock_dir "$lock_dir" "$reason"
    else
        SKIPPED_ACTIVE=$((SKIPPED_ACTIVE + 1))
    fi
}

cleanup_locks_on_path() {
    local lock_base="$1"
    local label="$2"

    log_msg ""
    log_msg "=== Locks auf $label ==="
    log_msg "  Pfad: $lock_base"
    log_msg "  Regel: veraltet >= ${LOCK_TIMEOUT_MINUTES}min (Datei-Locks), Global >= ${GLOBAL_LOCK_TIMEOUT_MINUTES}min"
    [ "$DO_OWN_ORPHAN_LOCKS" -eq 1 ] && log_msg "  Zusätzlich: eigene Locks ($THIS_HOST-*) mit toter PID"

    [ -d "$lock_base" ] || {
        log_msg "  (Verzeichnis nicht vorhanden oder nicht erreichbar)"
        return 0
    }

    shopt -s nullglob
    for lock_dir in "$lock_base"/*.lck; do
        [ -d "$lock_dir" ] || continue
        process_lock_dir "$lock_dir"
    done
    shopt -u nullglob
}

load_ssh_credentials() {
    SSH_USER=""
    SSH_PASS=""
    [ -f "$CRED_FILE" ] || return 1
    SSH_USER=$(grep "username" "$CRED_FILE" 2>/dev/null | cut -d'=' -f2 | xargs)
    SSH_PASS=$(grep "password" "$CRED_FILE" 2>/dev/null | cut -d'=' -f2 | xargs)
    [ -n "$SSH_USER" ] && [ -n "$SSH_PASS" ]
}

cleanup_locks_ssh() {
    local lock_base="$1"
    log_msg ""
    log_msg "=== Locks per SSH ($SOURCE_SSH_HOST) ==="
    log_msg "  Pfad: $lock_base"

    if ! load_ssh_credentials; then
        log_msg "  ✗ $CRED_FILE fehlt oder unvollständig – überspringe Remote-Locks"
        return 0
    fi

    while IFS='|' read -r lock_dir info; do
        [ -n "$lock_dir" ] || continue
        [ -n "$info" ] || continue
        name=$(basename "$lock_dir")
        if reason=$(should_remove_lock "$info" "$name"); then
            file_hint=$(lock_file_path "$info")
            [ -n "$file_hint" ] && reason="$reason, Datei: $file_hint"
            remove_lock_dir "$lock_dir" "$reason"
        else
            SKIPPED_ACTIVE=$((SKIPPED_ACTIVE + 1))
        fi
    done < <(
        sshpass -p "$SSH_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no -n \
            "$SSH_USER@$SOURCE_SSH_HOST" \
            "for d in $lock_base/*.lck; do [ -f \"\$d/info\" ] && printf '%s|%s\n' \"\$d\" \"\$(tr -d '\r' < \"\$d/info\" | head -n 1)\"; done" \
            2>/dev/null
    )
}

init_lock_paths() {
    USE_MOUNTS=0
    LOCK_BASE=""
    if [ -d "$TARGET_MOUNT_DIR" ] && [ -w "$TARGET_MOUNT_DIR" ] 2>/dev/null && \
       [ -d "$SOURCE_MOUNT_DIR" ] && [ -w "$SOURCE_MOUNT_DIR" ] 2>/dev/null; then
        USE_MOUNTS=1
        LOCK_BASE="$SOURCE_MOUNT_DIR/.comskip_locks"
    fi
}

# --- Main ---
log_msg "Comskip-Cleanup auf $THIS_HOST"
[ "$DRY_RUN" -eq 1 ] && log_msg "(Dry-Run – nichts wird gelöscht)"

if [ "$DO_LOCAL" -eq 1 ]; then
    cleanup_local
fi

if [ "$DO_LOCKS" -eq 1 ]; then
    init_lock_paths
    if [ "$USE_MOUNTS" -eq 1 ]; then
        cleanup_locks_on_path "$LOCK_BASE" "Quell-Mount"
    else
        cleanup_locks_ssh "$SOURCE_REMOTE_PATH/.comskip_locks"
    fi
fi

log_msg ""
log_msg "=== Zusammenfassung ==="
log_msg "  Entfernt:        $REMOVED"
log_msg "  Aktiv belassen:  $SKIPPED_ACTIVE (frische Locks / laufende PIDs)"
log_msg "  Failed-Uploads:  $FAILED_UPLOAD_DIR (nicht automatisch gelöscht)"
log_msg ""
log_msg "Hinweis: Locks anderer Rechner werden nur nach Timeout (${LOCK_TIMEOUT_MINUTES}min)"
log_msg "oder mit --own-locks entfernt, wenn die PID auf DIESEM Host nicht mehr läuft."

exit 0
