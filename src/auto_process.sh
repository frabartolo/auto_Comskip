#!/bin/bash

set -e
set -u

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

PROCESSED=0
FAILED=0
SKIPPED=0
RENAMED=0

log_message() {
    local msg="$1"
    echo "$msg"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $msg" >> "$MAIN_LOG"
}

# --- ZUGANGSDATEN AUSLESEN ---
if [ ! -f "$CRED_FILE" ]; then
COMSKIP_INI="./comskip.ini"

PROCESSED=0
FAILED=0
SKIPPED=0
RENAMED=0

log_message() {
    local msg="$1"
    echo "$msg"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $msg" >> "$MAIN_LOG"
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
    
    if ! mountpoint -q "$MOUNT_DIR"; then
        echo "FEHLER: Mount-Punkt ist nicht aktiv!"
        exit 1
    fi
    
    if ! ls "$MOUNT_DIR" > /dev/null 2>&1; then
        echo "FEHLER: Mount ist tot!"
        unmount_safely
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
    
    \( -iname "*.mp4" -o -iname "*.m4v" -o -iname "*.mkv" -o -iname "*.ts" \
    -o -iname "*.mpeg" -o -iname "*.mpg" -o -iname "*.mov" -o -iname "*.webm" \
    -o -iname "*.asf" -o -iname "*.wmv" -o -iname "*.avi" -o -iname "*.divx" \) 2>/dev/null | \
while IFS= read -r FILE; do
    
    FILE_BASE="${FILE%.*}"
    FILENAME=$(basename "$FILE_BASE")
    EXTENSION="${FILE##*.}"
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
