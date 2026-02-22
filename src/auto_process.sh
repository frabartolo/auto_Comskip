#!/bin/bash

set -e
set -u

# --- KONFIGURATION ---
CRED_FILE="/home/stefan/.smbcredentials"
SSH_HOST="cold-lairs"
REMOTE_PATH="/var/opt/shares/Videos"
MOUNT_DIR="$HOME/mount/cold-lairs-videos"
TARGET_BASE="/srv/data/Videos"
TEMP_DIR="/tmp/comskip_work"
MAIN_LOG="/srv/data/Videos/process_summary.log"
PYTHON_SCRIPT="./cut_with_edl.py"
COMSKIP_INI="./comskip.ini"

# --- MULTI-MASCHINEN KOORDINATION ---
# Lock-Verzeichnis auf dem Remote-Share (sichtbar für alle Rechner)
LOCK_DIR="$MOUNT_DIR/.comskip_locks"
# Eindeutige Worker-ID: Hostname + PID
WORKER_ID="$(hostname)-$$"
# Staler Lock nach X Minuten übernehmen (z. B. bei Absturz)
LOCK_TIMEOUT_MINUTES=60
# Heartbeat-Intervall in Sekunden (muss deutlich kürzer als LOCK_TIMEOUT_MINUTES*60 sein)
HEARTBEAT_INTERVAL_SECONDS=60

PROCESSED=0
FAILED=0
SKIPPED=0
RENAMED=0

log_message() {
    local msg="$1"
    echo "$msg"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $msg" >> "$MAIN_LOG"
}

# --- MULTI-MASCHINEN LOCK-FUNKTIONEN ---

# Erzeugt einen kollisionssicheren Schlüssel aus dem Dateipfad.
# Versucht sha256sum → md5sum → cksum als Fallback.
make_lock_key() {
    local path="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        printf '%s' "$path" | sha256sum | cut -d' ' -f1
    elif command -v md5sum >/dev/null 2>&1; then
        printf '%s' "$path" | md5sum | cut -d' ' -f1
    else
        printf '%s' "$path" | cksum | awk '{print $1 "_" $2}'
    fi
}

# Versucht, eine Datei exklusiv zu beanspruchen.
# Gibt 0 zurück wenn erfolgreich, 1 wenn bereits von anderem Rechner belegt.
try_claim_file() {
    local file_key="$1"
    local lock_dir="$LOCK_DIR/${file_key}.lck"

    # Lock-Verzeichnis anlegen (Fehler ignorieren, z. B. Read-only FS)
    mkdir -p "$LOCK_DIR" 2>/dev/null || return 0

    # Atomares Anlegen per mkdir
    if mkdir "$lock_dir" 2>/dev/null; then
        printf '%s:%s\n' "$WORKER_ID" "$(date +%s)" > "$lock_dir/info"
        return 0
    fi

    # Lock existiert – prüfe ob er abgelaufen ist
    if [ -f "$lock_dir/info" ]; then
        local lock_info worker_name lock_time age_minutes
        lock_info=$(cat "$lock_dir/info" 2>/dev/null || echo "unknown:0")
        worker_name=$(echo "$lock_info" | cut -d: -f1)
        lock_time=$(echo "$lock_info" | cut -d: -f2)
        age_minutes=$(( ( $(date +%s) - lock_time ) / 60 ))
        if [ "$age_minutes" -ge "$LOCK_TIMEOUT_MINUTES" ]; then
            log_message "  -> Staler Lock von $worker_name (${age_minutes}min), übernehme..."
            rm -rf "$lock_dir"
            if mkdir "$lock_dir" 2>/dev/null; then
                printf '%s:%s\n' "$WORKER_ID" "$(date +%s)" > "$lock_dir/info"
                # Race-Condition-Schutz: kurz warten, dann Eigentümerschaft bestätigen
                sleep 1
                if grep -q "^${WORKER_ID}:" "$lock_dir/info" 2>/dev/null; then
                    return 0
                fi
            fi
        fi
        log_message "  -> Bereits in Bearbeitung von: $worker_name (${age_minutes}min)"
    fi

    return 1
}

# Gibt den Lock für eine Datei frei.
release_file() {
    local file_key="$1"
    rm -rf "$LOCK_DIR/${file_key}.lck" 2>/dev/null || true
}

# Startet einen Hintergrundprozess, der den Lock-Zeitstempel regelmäßig erneuert,
# damit lange Jobs (>LOCK_TIMEOUT_MINUTES) nicht fälschlich als abgelaufen gelten.
start_heartbeat() {
    local file_key="$1"
    local lock_dir="$LOCK_DIR/${file_key}.lck"
    (
        while true; do
            sleep "$HEARTBEAT_INTERVAL_SECONDS"
            if grep -q "^${WORKER_ID}:" "$lock_dir/info" 2>/dev/null; then
                printf '%s:%s\n' "$WORKER_ID" "$(date +%s)" > "$lock_dir/info"
            else
                break
            fi
        done
    ) &
    echo $!
}

# Stoppt den Heartbeat-Prozess.
stop_heartbeat() {
    local pid="$1"
    [ -n "$pid" ] && kill "$pid" 2>/dev/null || true
}

# Räumt beim Beenden alle Locks auf, die von diesem Prozess angelegt wurden.
cleanup_locks() {
    [ -d "$LOCK_DIR" ] || return 0
    find "$LOCK_DIR" -name "*.lck" -type d 2>/dev/null | while IFS= read -r lock; do
        if [ -f "$lock/info" ] && grep -q "^${WORKER_ID}:" "$lock/info" 2>/dev/null; then
            rm -rf "$lock"
        fi
    done
}
trap cleanup_locks EXIT

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

# --- MOUNT LOGIK ---
mkdir -p "$MOUNT_DIR"

unmount_safely() {
    if mountpoint -q "$MOUNT_DIR"; then
        log_message "Unmounte $MOUNT_DIR..."
        fusermount -u "$MOUNT_DIR" 2>/dev/null || \
        sudo umount -l "$MOUNT_DIR" 2>/dev/null || \
        true
        sleep 1
    fi
}

if mountpoint -q "$MOUNT_DIR"; then
    if ! ls "$MOUNT_DIR" > /dev/null 2>&1; then
        log_message "WARNUNG: Stale mount erkannt"
        unmount_safely
    fi
fi

if ! mountpoint -q "$MOUNT_DIR"; then
    log_message "Mounte $MOUNT_DIR..."
    
    if ! sshpass -p "$SSH_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$SSH_USER@$SSH_HOST" "echo SSH_OK" > /dev/null 2>&1; then
        echo "FEHLER: SSH-Verbindung fehlgeschlagen!"
        exit 1
    fi
    
    if sshpass -p "$SSH_PASS" sshfs -o allow_other,default_permissions,reconnect,ServerAliveInterval=15,ServerAliveCountMax=3 \
        "$SSH_USER@$SSH_HOST:$REMOTE_PATH" "$MOUNT_DIR" 2>/dev/null; then
        log_message "  -> Mount erfolgreich (mit allow_other)"
    elif sshpass -p "$SSH_PASS" sshfs -o reconnect,ServerAliveInterval=15,ServerAliveCountMax=3 \
        "$SSH_USER@$SSH_HOST:$REMOTE_PATH" "$MOUNT_DIR" 2>/dev/null; then
        log_message "  -> Mount erfolgreich (ohne allow_other)"
    else
        echo "FEHLER: SSHFS-Mount fehlgeschlagen!"
        exit 1
    fi
    
    sleep 2
fi

# --- SYSTEM-INFO ---
log_message "=========================================="
log_message "System-Info"
log_message "=========================================="
TOTAL_RAM_GB=$(free -g | awk '/^Mem:/{print $2}')
AVAILABLE_RAM_GB=$(free -g | awk '/^Mem:/{print $7}')
log_message "RAM: ${AVAILABLE_RAM_GB}GB verfügbar von ${TOTAL_RAM_GB}GB gesamt"

VIDEO_FILES=$(find "$MOUNT_DIR" -type f \
    \( -iname "*.mp4" -o -iname "*.mkv" -o -iname "*.ts" -o -iname "*.mpeg" -o -iname "*.mpg" \
    -o -iname "*.m4v" -o -iname "*.mov" -o -iname "*.webm" -o -iname "*.avi" -o -iname "*.divx" \) 2>/dev/null | wc -l)
log_message "Video-Dateien gefunden: $VIDEO_FILES"

if [ "$VIDEO_FILES" -eq 0 ]; then
    echo "FEHLER: Keine Video-Dateien gefunden!"
    exit 1
fi

# --- VERARBEITUNG ---
mkdir -p "$TEMP_DIR"
mkdir -p "$TARGET_BASE"

if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "FEHLER: Python-Skript nicht gefunden: $PYTHON_SCRIPT"
    exit 1
fi

log_message "=========================================="
log_message "Start Verarbeitung: $(date)"
log_message "=========================================="

find "$MOUNT_DIR" -type f \
    \( -iname "*.mp4" -o -iname "*.m4v" -o -iname "*.mkv" -o -iname "*.ts" \
    -o -iname "*.mpeg" -o -iname "*.mpg" -o -iname "*.mov" -o -iname "*.webm" \
    -o -iname "*.asf" -o -iname "*.wmv" -o -iname "*.avi" -o -iname "*.divx" \) 2>/dev/null | \
while IFS= read -r FILE; do
    
    FILE_BASE="${FILE%.*}"
    FILENAME=$(basename "$FILE_BASE")
    EXTENSION="${FILE##*.}"
    REL_DIR=$(dirname "${FILE#$MOUNT_DIR/}")
    TARGET_DIR="$TARGET_BASE/$REL_DIR"
    
    # VERBESSERTE DUPLIKATERKENNUNG
    # Prüfe auf bereits verarbeitete Datei mit und ohne Zeitstempel
    CLEAN_NAME=$(echo "$FILENAME" | sed 's/__.*//')
    
    TARGET_FILE_ORIGINAL="$TARGET_DIR/$FILENAME.mkv"
    TARGET_FILE_CLEAN="$TARGET_DIR/$CLEAN_NAME.mkv"
    
    # Skip wenn eine der Varianten existiert
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
    
    # Verwende den Original-Namen für die Ausgabe
    TARGET_FILE="$TARGET_FILE_ORIGINAL"

    # Multi-Maschinen Lock: Datei exklusiv für diesen Rechner beanspruchen
    LOCK_KEY=$(make_lock_key "${FILE#$MOUNT_DIR/}")
    if ! try_claim_file "$LOCK_KEY"; then
        echo "Überspringe (in Bearbeitung auf anderem Rechner): $FILENAME.$EXTENSION"
        ((SKIPPED++)) || true
        continue
    fi
    HEARTBEAT_PID=$(start_heartbeat "$LOCK_KEY")

    log_message "------------------------------------------"
    log_message "Verarbeite: $FILENAME.$EXTENSION"
    
    # RAM-Check
    FILE_SIZE_MB=$(du -m "$FILE" | cut -f1)
    AVAILABLE_RAM_MB=$(free -m | awk '/^Mem:/{print $7}')
    
    if [ "$FILE_SIZE_MB" -gt "$((AVAILABLE_RAM_MB - 500))" ]; then
        log_message "  ⚠ WARNUNG: Datei zu groß für verfügbaren RAM"
        log_message "    Datei: ${FILE_SIZE_MB}MB, Verfügbar: ${AVAILABLE_RAM_MB}MB"
        log_message "    Tipp: Füge mehr RAM oder Swap hinzu"
        log_message "  ✗ Überspringe"
        stop_heartbeat "$HEARTBEAT_PID"
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
    
    if [ -f "$SRT_FILE" ]; then
        SRT_ARG="$SRT_FILE"
        log_message "  -> Untertitel: $(basename "$SRT_FILE")"
    fi
    
    if [ -f "$TXT_FILE" ]; then
        METADATA_ARG="$TXT_FILE"
        log_message "  -> Metadaten: $(basename "$TXT_FILE")"
    elif [ -f "$XML_FILE" ]; then
        METADATA_ARG="$XML_FILE"
        log_message "  -> Metadaten: $(basename "$XML_FILE")"
    fi
    
    # Comskip
    log_message "Comskip..."
    rm -rf "$TEMP_DIR"/*
    
    if [ -f "$COMSKIP_INI" ]; then
        comskip --ini="$COMSKIP_INI" --output="$TEMP_DIR" --quiet -- "$FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || true
    else
        comskip --output="$TEMP_DIR" --quiet -- "$FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || true
    fi
    
    EDL_FILE=$(find "$TEMP_DIR" -name "*.edl" 2>/dev/null | head -n 1)
    EDL_ARG="${EDL_FILE:-none}"
    
    # FFmpeg
    log_message "FFmpeg..."
    
    if python3 "$PYTHON_SCRIPT" "$FILE" "$EDL_ARG" "$TARGET_FILE" "$SRT_ARG" "$METADATA_ARG" "$MAIN_LOG" < /dev/null; then
        if [ -f "$TARGET_FILE" ]; then
            log_message "  ✓ Video verarbeitet"
            
            # Kopiere Sidecar-Dateien
            if [ -f "$SRT_FILE" ]; then
                cp "$SRT_FILE" "$TARGET_DIR/$FILENAME.srt"
                log_message "  ✓ Untertitel kopiert"
            fi
            
            if [ -f "$TXT_FILE" ]; then
                cp "$TXT_FILE" "$TARGET_DIR/$FILENAME.txt"
                log_message "  ✓ TXT kopiert"
            fi
            
            if [ -f "$XML_FILE" ]; then
                cp "$XML_FILE" "$TARGET_DIR/$FILENAME.xml"
                log_message "  ✓ XML kopiert"
            fi
            
            ((PROCESSED++)) || true
        else
            log_message "  ✗ Keine Ausgabe"
            ((FAILED++)) || true
        fi
    else
        EXIT_CODE=$?
        log_message "  ✗ Python Exit: $EXIT_CODE"
        if [ "$EXIT_CODE" -eq 6 ]; then
            log_message "    (FFmpeg Exit -9 = OOM Killer! Mehr RAM/Swap benötigt)"
        fi
        ((FAILED++)) || true
    fi
    
    stop_heartbeat "$HEARTBEAT_PID"
    release_file "$LOCK_KEY"

    rm -rf "$TEMP_DIR"/*
    
done

# --- DATEIEN UMBENENNEN ---
log_message "=========================================="
log_message "Benenne Dateien um..."
log_message "=========================================="

find "$TARGET_BASE" -type f -name "*__*.mkv" | while IFS= read -r FILE; do
    DIRNAME=$(dirname "$FILE")
    BASENAME=$(basename "$FILE" .mkv)
    NEW_BASENAME=$(echo "$BASENAME" | sed 's/__.*//')
    
    OLD_MKV="$DIRNAME/$BASENAME.mkv"
    NEW_MKV="$DIRNAME/$NEW_BASENAME.mkv"
    
    if [ -f "$NEW_MKV" ] && [ "$OLD_MKV" != "$NEW_MKV" ]; then
        log_message "Überspringe: $NEW_BASENAME.mkv existiert bereits"
        continue
    fi
    
    log_message "Umbenennen: $BASENAME -> $NEW_BASENAME"
    
    if [ "$OLD_MKV" != "$NEW_MKV" ]; then
        mv "$OLD_MKV" "$NEW_MKV"
        ((RENAMED++)) || true
    fi
    
    # Sidecar-Dateien
    for EXT in txt xml srt; do
        OLD_FILE="$DIRNAME/$BASENAME.$EXT"
        NEW_FILE="$DIRNAME/$NEW_BASENAME.$EXT"
        if [ -f "$OLD_FILE" ] && [ "$OLD_FILE" != "$NEW_FILE" ] && [ ! -f "$NEW_FILE" ]; then
            mv "$OLD_FILE" "$NEW_FILE"
            log_message "  -> $EXT umbenannt"
        fi
    done
done

log_message "Umbenannt: $RENAMED Dateien"

# --- ZUSAMMENFASSUNG ---
log_message "=========================================="
log_message "Ende: $(date)"
log_message "=========================================="
log_message "STATISTIK:"
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
