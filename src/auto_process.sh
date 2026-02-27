#!/bin/bash

set -e
set -u

# --- KONFIGURATION ---
CRED_FILE="$HOME/.smbcredentials"

# === QUELL-SERVER (Manuell gemountet) ===
SOURCE_MOUNT_DIR="$HOME/mount/cold-lairs-videos"
SOURCE_SSH_HOST="cold-lairs"
SOURCE_REMOTE_PATH="/var/opt/shares/Videos"

# === ZIEL-SERVER (Manuell gemountet) ===
TARGET_MOUNT_DIR="$HOME/mount/khanhiwara-videos"
TARGET_SSH_HOST="khanhiwara"
TARGET_REMOTE_PATH="/srv/data/Videos"

# Arbeitspfade
TARGET_BASE="$TARGET_MOUNT_DIR"
TEMP_DIR="/tmp/comskip_work"
MAIN_LOG="$TARGET_MOUNT_DIR/process_summary.log"
PYTHON_SCRIPT="./cut_with_edl.py"
COMSKIP_INI="./comskip.ini"
BLACKLIST_FILE="$TARGET_MOUNT_DIR/corrupted_files.blacklist"

# Multi-Maschinen Koordination
LOCK_DIR="$SOURCE_MOUNT_DIR/.comskip_locks"
WORKER_ID="$(hostname)-$$"
LOCK_TIMEOUT_MINUTES=120
LOCK_REFRESH_INTERVAL=300

PROCESSED=0
FAILED=0
SKIPPED=0
RENAMED=0

log_message() {
    local msg="$1"
    echo "$msg"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $msg" >> "$MAIN_LOG"
}

# --- BLACKLIST-FUNKTIONEN ---
is_blacklisted() {
    local filename="$1"
    [ ! -f "$BLACKLIST_FILE" ] && return 1
    grep -qxF "$filename" "$BLACKLIST_FILE" 2>/dev/null
}

# --- LOCK-FUNKTIONEN ---
generate_lock_key() {
    local input="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        echo "$input" | sha256sum | cut -d' ' -f1
    elif command -v md5sum >/dev/null 2>&1; then
        echo "$input" | md5sum | cut -d' ' -f1
    else
        echo "$input" | base64 | tr -d '=' | tr '+/' '-_'
    fi
}

try_claim_file() {
    local file_key="$1"
    local lock_dir="$LOCK_DIR/${file_key}.lck"

    mkdir -p "$LOCK_DIR" 2>/dev/null || {
        log_message "  -> WARNUNG: Kann Lock-Verzeichnis nicht erstellen"
        return 0
    }

    local retries=3
    while [ $retries -gt 0 ]; do
        if mkdir "$lock_dir" 2>/dev/null; then
            printf '%s:%s\n' "$WORKER_ID" "$(date +%s)" > "$lock_dir/info" 2>/dev/null || true
            return 0
        fi
        
        if [ -f "$lock_dir/info" ]; then
            local lock_info worker_name lock_time age_minutes now
            lock_info=$(cat "$lock_dir/info" 2>/dev/null || echo "unknown:0")
            worker_name=$(echo "$lock_info" | cut -d: -f1)
            lock_time=$(echo "$lock_info" | cut -d: -f2)
            now=$(date +%s)
            age_minutes=$(( (now - lock_time) / 60 ))
            
            if [ "$age_minutes" -ge "$LOCK_TIMEOUT_MINUTES" ]; then
                log_message "  -> Staler Lock von $worker_name (${age_minutes}min), übernehme..."
                if rm -rf "$lock_dir" 2>/dev/null && mkdir "$lock_dir" 2>/dev/null; then
                    printf '%s:%s\n' "$WORKER_ID" "$now" > "$lock_dir/info" 2>/dev/null || true
                    return 0
                fi
                retries=$((retries - 1))
                sleep 1
                continue
            fi
            
            log_message "  -> Bereits in Bearbeitung von: $worker_name (${age_minutes}min)"
            return 1
        else
            rm -rf "$lock_dir" 2>/dev/null || true
            retries=$((retries - 1))
            sleep 1
            continue
        fi
    done

    return 1
}

refresh_lock() {
    local file_key="$1"
    local lock_dir="$LOCK_DIR/${file_key}.lck"
    
    if [ -d "$lock_dir" ] && [ -f "$lock_dir/info" ]; then
        if grep -q "^${WORKER_ID}:" "$lock_dir/info" 2>/dev/null; then
            printf '%s:%s\n' "$WORKER_ID" "$(date +%s)" > "$lock_dir/info" 2>/dev/null || true
        fi
    fi
}

release_file() {
    local file_key="$1"
    local lock_dir="$LOCK_DIR/${file_key}.lck"
    
    if [ -d "$lock_dir" ] && [ -f "$lock_dir/info" ]; then
        if grep -q "^${WORKER_ID}:" "$lock_dir/info" 2>/dev/null; then
            rm -rf "$lock_dir" 2>/dev/null || true
        fi
    fi
}

cleanup_locks() {
    [ -d "$LOCK_DIR" ] || return 0
    log_message "Räume Locks auf..."
    find "$LOCK_DIR" -name "*.lck" -type d 2>/dev/null | while IFS= read -r lock; do
        if [ -f "$lock/info" ] && grep -q "^${WORKER_ID}:" "$lock/info" 2>/dev/null; then
            rm -rf "$lock" 2>/dev/null || true
        fi
    done
}
trap cleanup_locks EXIT INT TERM

# --- MOUNT-PRÜFUNG ---
check_and_mount() {
    local mount_dir="$1"
    local ssh_user="$2"
    local ssh_pass="$3"
    local ssh_host="$4"
    local remote_path="$5"
    local mount_name="$6"
    
    mkdir -p "$mount_dir"
    
    # Prüfe ob bereits gemountet
    if mountpoint -q "$mount_dir"; then
        if ls "$mount_dir" > /dev/null 2>&1; then
            log_message "$mount_name bereits gemountet und funktionsfähig"
            return 0
        else
            log_message "WARNUNG: Stale mount bei $mount_name erkannt"
            fusermount -u "$mount_dir" 2>/dev/null || sudo umount -l "$mount_dir" 2>/dev/null || true
            sleep 1
        fi
    fi
    
    # Mounte neu
    log_message "Mounte $mount_name..."
    
    if ! sshpass -p "$ssh_pass" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$ssh_user@$ssh_host" "echo SSH_OK" > /dev/null 2>&1; then
        echo "FEHLER: SSH-Verbindung zu $ssh_host fehlgeschlagen!"
        return 1
    fi
    
    if sshpass -p "$ssh_pass" sshfs -o allow_other,default_permissions,reconnect,ServerAliveInterval=15,ServerAliveCountMax=3 \
        "$ssh_user@$ssh_host:$remote_path" "$mount_dir" 2>/dev/null; then
        log_message "  -> $mount_name erfolgreich gemountet (mit allow_other)"
        return 0
    elif sshpass -p "$ssh_pass" sshfs -o reconnect,ServerAliveInterval=15,ServerAliveCountMax=3 \
        "$ssh_user@$ssh_host:$remote_path" "$mount_dir" 2>/dev/null; then
        log_message "  -> $mount_name erfolgreich gemountet (ohne allow_other)"
        return 0
    else
        echo "FEHLER: SSHFS-Mount von $mount_name fehlgeschlagen!"
        return 1
    fi
}

# --- ZUGANGSDATEN AUSLESEN ---
if [ ! -f "$CRED_FILE" ]; then
    echo "FEHLER: Datei $CRED_FILE nicht gefunden!"
    exit 1
fi

SSH_USER=$(grep "username" "$CRED_FILE" | cut -d'=' -f2 | xargs)
SSH_PASS=$(grep "password" "$CRED_FILE" | cut -d'=' -f2 | xargs)

if [ -z "$SSH_USER" ] || [ -z "$SSH_PASS" ]; then
    echo "FEHLER: Username oder Passwort konnte nicht gelesen werden!"
    exit 1
fi

# --- BEIDE MOUNTS PRÜFEN/ERSTELLEN ---
log_message "=========================================="
log_message "Mount-Prüfung"
log_message "=========================================="

if ! check_and_mount "$SOURCE_MOUNT_DIR" "$SSH_USER" "$SSH_PASS" "$SOURCE_SSH_HOST" "$SOURCE_REMOTE_PATH" "Quell-Server (cold-lairs)"; then
    exit 1
fi

if ! check_and_mount "$TARGET_MOUNT_DIR" "$SSH_USER" "$SSH_PASS" "$TARGET_SSH_HOST" "$TARGET_REMOTE_PATH" "Ziel-Server (khanhiwara)"; then
    exit 1
fi

# --- SYSTEM-INFO ---
log_message "=========================================="
log_message "System-Info"
log_message "=========================================="
log_message "Worker-ID: $WORKER_ID"
log_message "Quell-Mount: $SOURCE_MOUNT_DIR"
log_message "Ziel-Mount: $TARGET_MOUNT_DIR"

TOTAL_RAM_GB=$(( $(awk '/^MemTotal:/ {print $2}' /proc/meminfo) / 1024 / 1024 ))
AVAILABLE_RAM_GB=$(( $(awk '/^MemAvailable:/ {print $2}' /proc/meminfo) / 1024 / 1024 ))

log_message "RAM: ${AVAILABLE_RAM_GB}GB verfügbar von ${TOTAL_RAM_GB}GB gesamt"

VIDEO_FILES=$(find "$SOURCE_MOUNT_DIR" -type f \
    \( -iname "*.mp4" -o -iname "*.mkv" -o -iname "*.ts" -o -iname "*.mpeg" -o -iname "*.mpg" \
    -o -iname "*.m4v" -o -iname "*.mov" -o -iname "*.webm" -o -iname "*.avi" -o -iname "*.divx" \) 2>/dev/null | wc -l)
log_message "Video-Dateien gefunden: $VIDEO_FILES"

if [ "$VIDEO_FILES" -eq 0 ]; then
    echo "FEHLER: Keine Video-Dateien gefunden!"
    exit 1
fi

# --- VERARBEITUNG ---
mkdir -p "$TEMP_DIR"

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "FEHLER: Python-Skript nicht gefunden: $PYTHON_SCRIPT"
    exit 1
fi

log_message "=========================================="
log_message "Start Verarbeitung: $(date)"
log_message "=========================================="

LAST_LOCK_REFRESH=0

find "$SOURCE_MOUNT_DIR" -type f \
    \( -iname "*.mp4" -o -iname "*.m4v" -o -iname "*.mkv" -o -iname "*.ts" \
    -o -iname "*.mpeg" -o -iname "*.mpg" -o -iname "*.mov" -o -iname "*.webm" \
    -o -iname "*.asf" -o -iname "*.wmv" -o -iname "*.avi" -o -iname "*.divx" \) 2>/dev/null | \
while IFS= read -r FILE; do

    FILE_BASE="${FILE%.*}"
    FILENAME=$(basename "$FILE_BASE")
    EXTENSION="${FILE##*.}"
    REL_DIR=$(dirname "${FILE#$SOURCE_MOUNT_DIR/}")
    TARGET_DIR="$TARGET_BASE/$REL_DIR"

    # Blacklist-Prüfung
    if is_blacklisted "$FILENAME.$EXTENSION"; then
        echo "Überspringe (Blacklist): $FILENAME.$EXTENSION"
        ((SKIPPED++)) || true
        continue
    fi

    # Duplikaterkennung
    CLEAN_NAME=$(echo "$FILENAME" | sed 's/__.*//')

    TARGET_FILE_ORIGINAL="$TARGET_DIR/$FILENAME.mkv"
    TARGET_FILE_CLEAN="$TARGET_DIR/$CLEAN_NAME.mkv"

    # Skip wenn bereits vorhanden (jetzt zentral auf khanhiwara)
    if [ -f "$TARGET_FILE_ORIGINAL" ]; then
        echo "Überspringe (bereits vorhanden): $FILENAME.mkv"
        ((SKIPPED++)) || true
        continue
    fi

    if [ "$FILENAME" != "$CLEAN_NAME" ] && [ -f "$TARGET_FILE_CLEAN" ]; then
        echo "Überspringe (bereits vorhanden als $CLEAN_NAME.mkv)"
        ((SKIPPED++)) || true
        continue
    fi

    TARGET_FILE="$TARGET_FILE_ORIGINAL"

    # Multi-Maschinen Lock
    LOCK_KEY=$(generate_lock_key "${FILE#$SOURCE_MOUNT_DIR/}")
    if ! try_claim_file "$LOCK_KEY"; then
        echo "Überspringe (in Bearbeitung): $FILENAME.$EXTENSION"
        ((SKIPPED++)) || true
        continue
    fi

    log_message "------------------------------------------"
    log_message "Verarbeite: $FILENAME.$EXTENSION"

    # RAM-Check
    FILE_SIZE_MB=$(du -m "$FILE" | cut -f1)
    AVAILABLE_RAM_MB=$(( $(awk '/^MemAvailable:/ {print $2}' /proc/meminfo) / 1024 ))

    if [ "$FILE_SIZE_MB" -gt "$((AVAILABLE_RAM_MB - 500))" ]; then
        log_message "  ⚠ Datei zu groß: ${FILE_SIZE_MB}MB (verfügbar: ${AVAILABLE_RAM_MB}MB)"
        log_message "  ✗ Überspringe"
        release_file "$LOCK_KEY"
        ((FAILED++)) || true
        continue
    fi

    mkdir -p "$TARGET_DIR"

    # Sidecar-Dateien
    SRT_FILE="${FILE_BASE}.srt"
    TXT_FILE="${FILE_BASE}.txt"
    XML_FILE="${FILE_BASE}.xml"

    SRT_ARG="none"
    METADATA_ARG="none"

    [ -f "$SRT_FILE" ] && SRT_ARG="$SRT_FILE" && log_message "  -> Untertitel gefunden"
    [ -f "$TXT_FILE" ] && METADATA_ARG="$TXT_FILE" && log_message "  -> TXT-Metadaten gefunden"
    [ -z "$METADATA_ARG" -o "$METADATA_ARG" = "none" ] && [ -f "$XML_FILE" ] && METADATA_ARG="$XML_FILE" && log_message "  -> XML-Metadaten gefunden"

    # Comskip
    log_message "Schritt 1: Comskip..."
    rm -rf "$TEMP_DIR"/*
    refresh_lock "$LOCK_KEY"

    # Pre-Check: Prüfe ob Datei von ffprobe lesbar ist
    WORKING_FILE="$FILE"
    TEMP_REPAIRED=""
    EDL_ARG=""
    
    if ! ffprobe -v error "$FILE" >/dev/null 2>&1; then
        log_message "  ⚠ Datei hat Fehler, versuche Reparatur vor Comskip..."
        TEMP_REPAIRED="/tmp/comskip_preprocess_$$.ts"
        if ffmpeg -nostdin -v error -err_detect ignore_err -i "$FILE" -c copy -y "$TEMP_REPAIRED" >/dev/null 2>&1; then
            if [ -f "$TEMP_REPAIRED" ] && [ -s "$TEMP_REPAIRED" ]; then
                log_message "  ✓ Datei repariert, nutze reparierte Version für Comskip"
                WORKING_FILE="$TEMP_REPAIRED"
            else
                log_message "  ✗ Reparatur fehlgeschlagen, überspringe Comskip"
                rm -f "$TEMP_REPAIRED" 2>/dev/null
                EDL_ARG="none"
                WORKING_FILE="$FILE"
            fi
        else
            log_message "  ✗ Reparatur fehlgeschlagen, überspringe Comskip"
            EDL_ARG="none"
        fi
    fi

    if [ -z "$EDL_ARG" ]; then
        if [ -f "$COMSKIP_INI" ]; then
            comskip --ini="$COMSKIP_INI" --output="$TEMP_DIR" --quiet -- "$WORKING_FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || {
                COMSKIP_EXIT=$?
                if [ $COMSKIP_EXIT -eq 139 ] || [ $COMSKIP_EXIT -eq 134 ]; then
                    log_message "  ⚠ Comskip Segfault (Exit $COMSKIP_EXIT), überspringe Werbeerkennung"
                else
                    log_message "  ⚠ Comskip Exit $COMSKIP_EXIT"
                fi
            }
        else
            comskip --output="$TEMP_DIR" --quiet -- "$WORKING_FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || {
                COMSKIP_EXIT=$?
                if [ $COMSKIP_EXIT -eq 139 ] || [ $COMSKIP_EXIT -eq 134 ]; then
                    log_message "  ⚠ Comskip Segfault (Exit $COMSKIP_EXIT), überspringe Werbeerkennung"
                else
                    log_message "  ⚠ Comskip Exit $COMSKIP_EXIT"
                fi
            }
        fi

        EDL_FILE=$(find "$TEMP_DIR" -name "*.edl" 2>/dev/null | head -n 1)
        EDL_ARG="${EDL_FILE:-none}"
    fi
    
    # Cleanup temp repaired file
    [ -n "$TEMP_REPAIRED" ] && rm -f "$TEMP_REPAIRED" 2>/dev/null

    # FFmpeg mit Lock-Refresh
    log_message "Schritt 2: FFmpeg..."
    refresh_lock "$LOCK_KEY"

    # Background Lock-Refresh
    (
        while kill -0 $$ 2>/dev/null; do
            NOW=$(date +%s)
            if [ $((NOW - LAST_LOCK_REFRESH)) -ge $LOCK_REFRESH_INTERVAL ]; then
                refresh_lock "$LOCK_KEY"
                LAST_LOCK_REFRESH=$NOW
            fi
            sleep 60
        done
    ) &
    REFRESH_PID=$!

    python3 "$PYTHON_SCRIPT" "$FILE" "$EDL_ARG" "$TARGET_FILE" "$SRT_ARG" "$METADATA_ARG" "$MAIN_LOG" < /dev/null
    PYTHON_EXIT=$?

    kill $REFRESH_PID 2>/dev/null || true
    wait $REFRESH_PID 2>/dev/null || true

    if [ $PYTHON_EXIT -eq 0 ] && [ -f "$TARGET_FILE" ]; then
        log_message "  ✓ Video verarbeitet"

        # Kopiere Sidecar-Dateien
        [ -f "$SRT_FILE" ] && cp "$SRT_FILE" "$TARGET_DIR/$FILENAME.srt" && log_message "  ✓ Untertitel kopiert"
        [ -f "$TXT_FILE" ] && cp "$TXT_FILE" "$TARGET_DIR/$FILENAME.txt" && log_message "  ✓ TXT kopiert"
        [ -f "$XML_FILE" ] && cp "$XML_FILE" "$TARGET_DIR/$FILENAME.xml" && log_message "  ✓ XML kopiert"

        ((PROCESSED++)) || true
    elif [ $PYTHON_EXIT -eq 9 ]; then
        log_message "  ✗ Datei ist auf Blacklist (permanent defekt)"
        ((FAILED++)) || true
    else
        log_message "  ✗ Fehler (Exit: $PYTHON_EXIT)"
        [ "$PYTHON_EXIT" -eq 6 ] && log_message "    (FFmpeg-Fehler oder OOM)"
        ((FAILED++)) || true
    fi

    release_file "$LOCK_KEY"
    rm -rf "$TEMP_DIR"/*

done

# --- DATEIEN UMBENENNEN ---
log_message "=========================================="
log_message "Umbenennung..."
log_message "=========================================="

find "$TARGET_BASE" -type f -name "*__*.mkv" 2>/dev/null | while IFS= read -r FILE; do
    DIRNAME=$(dirname "$FILE")
    BASENAME=$(basename "$FILE" .mkv)
    NEW_BASENAME=$(echo "$BASENAME" | sed 's/__.*//')

    OLD_MKV="$DIRNAME/$BASENAME.mkv"
    NEW_MKV="$DIRNAME/$NEW_BASENAME.mkv"

    [ -f "$NEW_MKV" ] && [ "$OLD_MKV" != "$NEW_MKV" ] && continue

    log_message "Umbenennen: $BASENAME -> $NEW_BASENAME"

    [ "$OLD_MKV" != "$NEW_MKV" ] && mv "$OLD_MKV" "$NEW_MKV" && ((RENAMED++)) || true

    for EXT in txt xml srt; do
        OLD_FILE="$DIRNAME/$BASENAME.$EXT"
        NEW_FILE="$DIRNAME/$NEW_BASENAME.$EXT"
        [ -f "$OLD_FILE" ] && [ "$OLD_FILE" != "$NEW_FILE" ] && [ ! -f "$NEW_FILE" ] && mv "$OLD_FILE" "$NEW_FILE"
    done
done

# --- ZUSAMMENFASSUNG ---
log_message "=========================================="
log_message "Ende: $(date)"
log_message "=========================================="
log_message "STATISTIK ($WORKER_ID):"
log_message "  Erfolgreich: $PROCESSED"
log_message "  Übersprungen: $SKIPPED"
log_message "  Fehlgeschlagen: $FAILED"
log_message "  Umbenannt: $RENAMED"
log_message "=========================================="

echo ""
echo "Fertig!"
echo "Erfolgreich: $PROCESSED | Übersprungen: $SKIPPED | Fehler: $FAILED | Umbenannt: $RENAMED"
echo "Log: $MAIN_LOG"

exit 0
