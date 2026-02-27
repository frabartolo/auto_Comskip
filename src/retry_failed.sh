#!/bin/bash
#
# Analysiert die Protokolldatei (process_summary.log) auf Fehler und schickt
# die betroffenen Dateien erneut durch den Comskip-/FFmpeg-Workflow.
#
# Voraussetzung: Mount muss bereits aktiv sein (z.B. vorher auto_process.sh
# ausgeführt oder MOUNT_DIR manuell gemountet).
#
# Aufruf: ./retry_failed.sh [--dry-run]
#   --dry-run  Nur anzeigen, welche Dateien wiederholt würden, ohne zu verarbeiten.
#

set -e
set -u

# --- KONFIGURATION (muss zu auto_process.sh passen) ---
SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "$0")" && pwd)}"
MOUNT_DIR="${MOUNT_DIR:-$HOME/mount/cold-lairs-videos}"
TARGET_BASE="${TARGET_BASE:-/srv/data/Videos}"
TEMP_DIR="${TEMP_DIR:-/tmp/comskip_work}"
MAIN_LOG="${MAIN_LOG:-/srv/data/Videos/process_summary.log}"
PYTHON_SCRIPT="${SCRIPT_DIR}/cut_with_edl.py"
COMSKIP_INI="${SCRIPT_DIR}/comskip.ini"
BLACKLIST_FILE="/srv/data/Videos/corrupted_files.blacklist"

DRY_RUN=0
if [ "${1:-}" = "--dry-run" ]; then
    DRY_RUN=1
    shift
fi

log_message() {
    local msg="$1"
    echo "$msg"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $msg" >> "$MAIN_LOG"
}

# --- Protokolldatei prüfen ---
if [ ! -f "$MAIN_LOG" ]; then
    echo "FEHLER: Protokolldatei nicht gefunden: $MAIN_LOG"
    exit 1
fi

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "FEHLER: Python-Skript nicht gefunden: $PYTHON_SCRIPT"
    exit 1
fi

# --- Log analysieren: fehlgeschlagene Dateien sammeln ---
# Suchmuster: Nach "Verarbeite: DATEINAME" schauen, ob im gleichen Block
# "Speicherzugriffsfehler", "✗ Keine Ausgabe" oder "✗ Python Exit:" vorkommt.
FAILED_FILES=()
CURRENT_FILE=""
CURRENT_FAILED=0

while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" == *"Verarbeite: "* ]]; then
        # Neuer Block: vorherigen als fehlgeschlagen merken
        if [ -n "$CURRENT_FILE" ] && [ "$CURRENT_FAILED" -eq 1 ]; then
            FAILED_FILES+=("$CURRENT_FILE")
        fi
        # Dateiname = alles nach "Verarbeite: " (ggf. mit [timestamp] davor)
        CURRENT_FILE=$(echo "${line#*Verarbeite: }" | xargs)
        CURRENT_FAILED=0
    fi

    if [ -n "$CURRENT_FILE" ]; then
        if [[ "$line" == *"Speicherzugriffsfehler"* ]] \
           || [[ "$line" == *"✗ Keine Ausgabe"* ]] \
           || [[ "$line" == *"✗ Python Exit:"* ]]; then
            CURRENT_FAILED=1
        fi
        if [[ "$line" == *"✓ Video erfolgreich verarbeitet"* ]]; then
            CURRENT_FAILED=0
        fi
    fi
done < "$MAIN_LOG"

# Letzten Block auswerten
if [ -n "$CURRENT_FILE" ] && [ "$CURRENT_FAILED" -eq 1 ]; then
    FAILED_FILES+=("$CURRENT_FILE")
fi

# Duplikate entfernen (gleiche Datei evtl. mehrfach im Log)
FAILED_FILES=($(printf '%s\n' "${FAILED_FILES[@]}" | sort -u))

if [ ${#FAILED_FILES[@]} -eq 0 ]; then
    echo "Keine fehlgeschlagenen Dateien in der Protokolldatei gefunden."
    exit 0
fi

echo "Gefundene fehlgeschlagene Dateien: ${#FAILED_FILES[@]}"
for f in "${FAILED_FILES[@]}"; do
    echo "  - $f"
done
echo ""

if [ "$DRY_RUN" -eq 1 ]; then
    echo "Dry-run: Es wird nichts verarbeitet."
    exit 0
fi

# --- Mount prüfen ---
if ! mountpoint -q "$MOUNT_DIR" 2>/dev/null; then
    echo "WARNUNG: $MOUNT_DIR ist nicht gemountet. Bitte zuerst auto_process.sh ausführen oder Mount manuell setzen."
    read -p "Trotzdem fortfahren? (j/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[jJ]$ ]]; then
        exit 1
    fi
fi

mkdir -p "$TEMP_DIR"
PROCESSED=0
FAILED=0

for BASENAME in "${FAILED_FILES[@]}"; do
    # Prüfe ob in Blacklist
    if [ -f "$BLACKLIST_FILE" ] && grep -qxF "$BASENAME" "$BLACKLIST_FILE"; then
        log_message "Retry: Überspringe (Blacklist): $BASENAME"
        ((FAILED++)) || true
        continue
    fi
    
    # Vollständigen Pfad der Quelldatei suchen (unter MOUNT_DIR)
    FILE=$(find "$MOUNT_DIR" -type f -name "$BASENAME" 2>/dev/null | head -n 1)
    if [ -z "$FILE" ] || [ ! -f "$FILE" ]; then
        log_message "Retry: Quelle nicht gefunden, überspringe: $BASENAME"
        ((FAILED++)) || true
        continue
    fi

    FILE_BASE="${FILE%.*}"
    FILENAME=$(basename "$FILE_BASE")
    EXTENSION="${FILE##*.}"
    REL_DIR=$(dirname "${FILE#$MOUNT_DIR/}")
    REL_DIR="${REL_DIR#/}"
    TARGET_DIR="$TARGET_BASE/$REL_DIR"
    TARGET_FILE="$TARGET_DIR/$FILENAME.mkv"

    log_message "------------------------------------------"
    log_message "Retry: Verarbeite: $FILENAME.$EXTENSION"

    mkdir -p "$TARGET_DIR"

    SRT_FILE="${FILE_BASE}.srt"
    TXT_FILE="${FILE_BASE}.txt"
    XML_FILE="${FILE_BASE}.xml"
    SRT_ARG="none"
    METADATA_ARG="none"

    if [ -f "$SRT_FILE" ]; then
        SRT_ARG="$SRT_FILE"
        log_message "  -> Untertitel: $(basename "$SRT_FILE")"
    fi
    if [ -f "$TXT_FILE" ]; then
        METADATA_ARG="$TXT_FILE"
    elif [ -f "$XML_FILE" ]; then
        METADATA_ARG="$XML_FILE"
    fi

    # Comskip
    log_message "Retry: Comskip..."
    rm -rf "$TEMP_DIR"/*
    if [ -f "$COMSKIP_INI" ]; then
        comskip --ini="$COMSKIP_INI" --output="$TEMP_DIR" --quiet -- "$FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || true
    else
        comskip --output="$TEMP_DIR" --quiet -- "$FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || true
    fi

    EDL_FILE=$(find "$TEMP_DIR" -name "*.edl" 2>/dev/null | head -n 1)
    if [ -n "$EDL_FILE" ] && [ -f "$EDL_FILE" ]; then
        EDL_ARG="$EDL_FILE"
    else
        EDL_ARG="none"
    fi

    # FFmpeg (Python)
    log_message "Retry: FFmpeg..."
    if python3 "$PYTHON_SCRIPT" "$FILE" "$EDL_ARG" "$TARGET_FILE" "$SRT_ARG" "$METADATA_ARG" "$MAIN_LOG" < /dev/null; then
        if [ -f "$TARGET_FILE" ]; then
            log_message "  ✓ Video erfolgreich verarbeitet (Retry)"
            if [ -f "$SRT_FILE" ]; then
                cp "$SRT_FILE" "$TARGET_DIR/$FILENAME.srt"
                log_message "  ✓ Untertitel kopiert"
            fi
            if [ -f "$TXT_FILE" ]; then
                cp "$TXT_FILE" "$TARGET_DIR/$FILENAME.txt"
            fi
            if [ -f "$XML_FILE" ]; then
                cp "$XML_FILE" "$TARGET_DIR/$FILENAME.xml"
            fi
            ((PROCESSED++)) || true
        else
            log_message "  ✗ Keine Ausgabe"
            ((FAILED++)) || true
        fi
    else
        log_message "  ✗ Python Exit: $?"
        ((FAILED++)) || true
    fi

    rm -rf "$TEMP_DIR"/*
done

log_message "=========================================="
log_message "Retry abgeschlossen: $PROCESSED erfolgreich, $FAILED erneut fehlgeschlagen"
log_message "=========================================="
echo ""
echo "Retry abgeschlossen. Erfolgreich: $PROCESSED, erneut fehlgeschlagen: $FAILED"
echo "Log: $MAIN_LOG"

exit 0
