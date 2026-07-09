#!/bin/bash
#
# Recodiert Blacklist-Dateien OHNE Comskip (EDL=none, voller Film mit Werbung).
# Für lange Läufe in screen/tmux geeignet.
#
# Aufruf:
#   ./encode_blacklisted.sh              # alle Einträge in corrupted_files.blacklist
#   ./encode_blacklisted.sh --dry-run    # nur anzeigen
#   ./encode_blacklisted.sh --list datei # nur diese Basenames (eine pro Zeile)
#
# Screen-Beispiel:
#   screen -S encode-bl
#   cd ~/auto_Comskip/src && ./encode_blacklisted.sh
#   # Detach: Ctrl+A, dann D

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE_MOUNT_DIR="${SOURCE_MOUNT_DIR:-$HOME/mount/cold-lairs-videos}"
TARGET_MOUNT_DIR="${TARGET_MOUNT_DIR:-$HOME/mount/khanhiwara-videos}"
BLACKLIST_FILE="${BLACKLIST_FILE:-$TARGET_MOUNT_DIR/corrupted_files.blacklist}"
PYTHON_SCRIPT="${PYTHON_SCRIPT:-$SCRIPT_DIR/cut_with_edl.py}"
RUN_LOG="${ENCODE_BL_LOG:-$TARGET_MOUNT_DIR/encode_blacklisted.log}"

DRY_RUN=0
LIST_FILE=""

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --list) LIST_FILE="${2:?--list braucht eine Datei}"; shift ;;
        -h|--help)
            sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "Unbekannte Option: $1" >&2; exit 2 ;;
    esac
    shift
done

log_msg() {
    echo "$1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$RUN_LOG"
}

remove_from_blacklist() {
    local fname="$1"
    local tmp
    tmp=$(mktemp)
    grep -vxF "$fname" "$BLACKLIST_FILE" > "$tmp" || true
    mv "$tmp" "$BLACKLIST_FILE"
}

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "FEHLER: $PYTHON_SCRIPT nicht gefunden" >&2
    exit 1
fi
if [ ! -d "$SOURCE_MOUNT_DIR" ]; then
    echo "FEHLER: Quell-Mount fehlt: $SOURCE_MOUNT_DIR" >&2
    exit 1
fi
if [ ! -d "$TARGET_MOUNT_DIR" ]; then
    echo "FEHLER: Ziel-Mount fehlt: $TARGET_MOUNT_DIR" >&2
    exit 1
fi

if [ -n "$LIST_FILE" ]; then
    [ -f "$LIST_FILE" ] || { echo "FEHLER: $LIST_FILE nicht gefunden" >&2; exit 1; }
    mapfile -t ENTRIES < <(grep -v '^[[:space:]]*$' "$LIST_FILE")
elif [ -f "$BLACKLIST_FILE" ]; then
    mapfile -t ENTRIES < <(grep -v '^[[:space:]]*$' "$BLACKLIST_FILE")
else
    echo "Keine Blacklist: $BLACKLIST_FILE"
    exit 0
fi

if [ "${#ENTRIES[@]}" -eq 0 ]; then
    echo "Keine Einträge zu verarbeiten."
    exit 0
fi

log_msg "=========================================="
log_msg "encode_blacklisted: Start (${#ENTRIES[@]} Einträge, dry-run=$DRY_RUN)"
log_msg "  Quelle: $SOURCE_MOUNT_DIR"
log_msg "  Ziel:   $TARGET_MOUNT_DIR"
log_msg "  Log:    $RUN_LOG"
log_msg "=========================================="

OK=0
SKIP=0
FAIL=0
N=0

for fname in "${ENTRIES[@]}"; do
    N=$((N + 1))
    base="${fname%.*}"

    src=$(find "$SOURCE_MOUNT_DIR" -type f -name "$fname" 2>/dev/null | head -n 1)
    if [ -z "$src" ] || [ ! -f "$src" ]; then
        log_msg "[$N/${#ENTRIES[@]}] ✗ Quelle nicht gefunden: $fname"
        FAIL=$((FAIL + 1))
        continue
    fi

    rel="${src#$SOURCE_MOUNT_DIR/}"
    rel="${rel#/}"
    rel_dir=$(dirname "$rel")
    [ "$rel_dir" = "." ] && rel_dir=""
    if [ -n "$rel_dir" ]; then
        out="$TARGET_MOUNT_DIR/$rel_dir/$base.mkv"
    else
        out="$TARGET_MOUNT_DIR/$base.mkv"
    fi

    if [ -f "$out" ]; then
        log_msg "[$N/${#ENTRIES[@]}] Überspringe (Ziel existiert): $out"
        if [ "$DRY_RUN" -eq 0 ] && [ -f "$BLACKLIST_FILE" ] && grep -qxF "$fname" "$BLACKLIST_FILE" 2>/dev/null; then
            remove_from_blacklist "$fname"
            log_msg "  -> Blacklist-Eintrag entfernt (bereits recodiert)"
        fi
        SKIP=$((SKIP + 1))
        continue
    fi

    clean_base=$(echo "$base" | sed 's/__.*//')
    if [ -n "$rel_dir" ]; then
        out_clean="$TARGET_MOUNT_DIR/$rel_dir/$clean_base.mkv"
    else
        out_clean="$TARGET_MOUNT_DIR/$clean_base.mkv"
    fi
    if [ "$base" != "$clean_base" ] && [ -f "$out_clean" ]; then
        log_msg "[$N/${#ENTRIES[@]}] Überspringe (Ziel als $clean_base.mkv): $fname"
        SKIP=$((SKIP + 1))
        continue
    fi

    if [ "$DRY_RUN" -eq 1 ]; then
        log_msg "[$N/${#ENTRIES[@]}] [dry-run] würde encodieren: $fname -> $out"
        continue
    fi

    mkdir -p "$(dirname "$out")"
    file_base="${src%.*}"
    srt_arg="none"
    meta_arg="none"
    [ -f "${file_base}.srt" ] && srt_arg="${file_base}.srt"
    [ -f "${file_base}.txt" ] && meta_arg="${file_base}.txt"
    [ "$meta_arg" = "none" ] && [ -f "${file_base}.xml" ] && meta_arg="${file_base}.xml"

    log_msg "[$N/${#ENTRIES[@]}] Verarbeite (ohne Comskip): $fname"

    if python3 "$PYTHON_SCRIPT" "$src" none "$out" "$srt_arg" "$meta_arg" none < /dev/null; then
        if [ -f "$out" ]; then
            log_msg "  ✓ Erfolgreich: $out"
            if [ -f "$BLACKLIST_FILE" ] && grep -qxF "$fname" "$BLACKLIST_FILE" 2>/dev/null; then
                remove_from_blacklist "$fname"
                log_msg "  -> aus Blacklist entfernt"
            fi
            for ext in srt txt xml; do
                side="${file_base}.${ext}"
                [ -f "$side" ] && cp -f "$side" "$(dirname "$out")/$base.${ext}" 2>/dev/null || true
            done
            OK=$((OK + 1))
        else
            log_msg "  ✗ Python OK, aber Ausgabedatei fehlt: $out"
            FAIL=$((FAIL + 1))
        fi
    else
        ec=$?
        log_msg "  ✗ Fehler (Exit: $ec): $fname"
        FAIL=$((FAIL + 1))
    fi
done

log_msg "=========================================="
log_msg "encode_blacklisted: Ende – OK: $OK, übersprungen: $SKIP, Fehler: $FAIL"
log_msg "=========================================="

exit 0
