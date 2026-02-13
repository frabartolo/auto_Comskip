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

# --- MOUNT LOGIK MIT STALE-MOUNT-ERKENNUNG ---
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
    log_message "Info: $MOUNT_DIR ist gemountet."
    
    if ls "$MOUNT_DIR" > /dev/null 2>&1; then
        FILE_COUNT=$(find "$MOUNT_DIR" -maxdepth 1 -type f 2>/dev/null | wc -l)
        log_message "Mount ist OK"
        
        if [ "$FILE_COUNT" -eq 0 ] && [ $(find "$MOUNT_DIR" -maxdepth 1 2>/dev/null | wc -l) -le 1 ]; then
            log_message "WARNUNG: Mount ist leer! Versuche neu zu mounten..."
            unmount_safely
        fi
    else
        log_message "WARNUNG: Stale mount erkannt"
        unmount_safely
    fi
fi

if ! mountpoint -q "$MOUNT_DIR"; then
    log_message "Versuche $MOUNT_DIR zu mounten..."
    
    log_message "Teste SSH-Verbindung zu $SSH_HOST..."
    if sshpass -p "$SSH_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$SSH_USER@$SSH_HOST" "echo SSH_OK" > /dev/null 2>&1; then
        log_message "  -> SSH-Verbindung erfolgreich"
    else
        echo "FEHLER: SSH-Verbindung zu $SSH_HOST fehlgeschlagen!"
        exit 1
    fi
    
    log_message "Mounte mit SSHFS..."
    
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
        exit 1
    fi
fi

# --- FINALE MOUNT-PRÜFUNG ---
log_message "=========================================="
log_message "Prüfe Mount-Inhalt..."
log_message "=========================================="

TOTAL_FILES=$(find "$MOUNT_DIR" -type f 2>/dev/null | wc -l)
VIDEO_FILES=$(find "$MOUNT_DIR" -type f \
    \( -iname "*.mp4" -o -iname "*.mkv" -o -iname "*.ts" -o -iname "*.mpeg" -o -iname "*.mpg" \
    -o -iname "*.m4v" -o -iname "*.mov" -o -iname "*.webm" -o -iname "*.avi" -o -iname "*.divx" \) 2>/dev/null | wc -l)

log_message "Dateien gesamt: $TOTAL_FILES"
log_message "Video-Dateien: $VIDEO_FILES"

if [ "$VIDEO_FILES" -eq 0 ]; then
    echo ""
    echo "FEHLER: Keine Video-Dateien im Mount gefunden!"
    echo ""
    echo "Mount-Verzeichnis-Inhalt:"
    ls -lah "$MOUNT_DIR" | head -20
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
    TARGET_FILE="$TARGET_DIR/$FILENAME.mkv"
    
    if [ -f "$TARGET_FILE" ]; then
        echo "Überspringe: $FILENAME.$EXTENSION"
        ((SKIPPED++)) || true
        continue
    fi
    
    log_message "------------------------------------------"
    log_message "Verarbeite: $FILENAME.$EXTENSION"
    
    mkdir -p "$TARGET_DIR"
    
    # Sidecar-Dateien identifizieren
    SRT_FILE="${FILE_BASE}.srt"
    TXT_FILE="${FILE_BASE}.txt"
    XML_FILE="${FILE_BASE}.xml"
    
    # Für FFmpeg: setze auf "none" wenn nicht vorhanden
    SRT_ARG="none"
    METADATA_ARG="none"
    
    if [ -f "$SRT_FILE" ]; then
        SRT_ARG="$SRT_FILE"
        log_message "  -> Untertitel gefunden: $(basename "$SRT_FILE")"
    fi
    
    if [ -f "$TXT_FILE" ]; then
        METADATA_ARG="$TXT_FILE"
        log_message "  -> Metadaten gefunden: $(basename "$TXT_FILE")"
    elif [ -f "$XML_FILE" ]; then
        METADATA_ARG="$XML_FILE"
        log_message "  -> Metadaten gefunden: $(basename "$XML_FILE")"
    fi
    
    # Comskip ausführen
    log_message "Comskip..."
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
    
    # FFmpeg-Verarbeitung
    log_message "FFmpeg..."
    
    if python3 "$PYTHON_SCRIPT" "$FILE" "$EDL_ARG" "$TARGET_FILE" "$SRT_ARG" "$METADATA_ARG" "$MAIN_LOG" < /dev/null; then
        if [ -f "$TARGET_FILE" ]; then
            log_message "  ✓ Video erfolgreich verarbeitet"
            
            # WICHTIG: Kopiere Sidecar-Dateien zum Ziel
            if [ -f "$SRT_FILE" ]; then
                cp "$SRT_FILE" "$TARGET_DIR/$FILENAME.srt"
                log_message "  ✓ Untertitel kopiert"
            fi
            
            if [ -f "$TXT_FILE" ]; then
                cp "$TXT_FILE" "$TARGET_DIR/$FILENAME.txt"
                log_message "  ✓ TXT-Metadaten kopiert"
            fi
            
            if [ -f "$XML_FILE" ]; then
                cp "$XML_FILE" "$TARGET_DIR/$FILENAME.xml"
                log_message "  ✓ XML-Metadaten kopiert"
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

# --- DATEIEN UMBENENNEN (MIT SIDECAR-DATEIEN) ---
log_message "=========================================="
log_message "Benenne Dateien um (entferne Zeitstempel)..."
log_message "=========================================="

find "$TARGET_BASE" -type f -name "*__*.mkv" | while IFS= read -r FILE; do
    DIRNAME=$(dirname "$FILE")
    BASENAME=$(basename "$FILE" .mkv)
    
    # Extrahiere Basisname (alles vor dem ersten __)
    NEW_BASENAME=$(echo "$BASENAME" | sed 's/__.*//')
    
    # Ziel-Dateien
    OLD_MKV="$DIRNAME/$BASENAME.mkv"
    NEW_MKV="$DIRNAME/$NEW_BASENAME.mkv"
    
    # Prüfe ob Ziel-MKV bereits existiert
    if [ -f "$NEW_MKV" ] && [ "$OLD_MKV" != "$NEW_MKV" ]; then
        log_message "Überspringe: $NEW_BASENAME.mkv existiert bereits"
        continue
    fi
    
    log_message "Umbenennen: $BASENAME -> $NEW_BASENAME"
    
    # Benenne .mkv um
    if [ "$OLD_MKV" != "$NEW_MKV" ]; then
        mv "$OLD_MKV" "$NEW_MKV"
        ((RENAMED++)) || true
    fi
    
    # Benenne zugehörige .txt Datei um (falls vorhanden)
    OLD_TXT="$DIRNAME/$BASENAME.txt"
    NEW_TXT="$DIRNAME/$NEW_BASENAME.txt"
    if [ -f "$OLD_TXT" ] && [ "$OLD_TXT" != "$NEW_TXT" ]; then
        if [ ! -f "$NEW_TXT" ]; then
            log_message "  -> Benenne auch um: $BASENAME.txt"
            mv "$OLD_TXT" "$NEW_TXT"
        fi
    fi
    
    # Benenne zugehörige .xml Datei um (falls vorhanden)
    OLD_XML="$DIRNAME/$BASENAME.xml"
    NEW_XML="$DIRNAME/$NEW_BASENAME.xml"
    if [ -f "$OLD_XML" ] && [ "$OLD_XML" != "$NEW_XML" ]; then
        if [ ! -f "$NEW_XML" ]; then
            log_message "  -> Benenne auch um: $BASENAME.xml"
            mv "$OLD_XML" "$NEW_XML"
        fi
    fi
    
    # Benenne zugehörige .srt Datei um (falls vorhanden)
    OLD_SRT="$DIRNAME/$BASENAME.srt"
    NEW_SRT="$DIRNAME/$NEW_BASENAME.srt"
    if [ -f "$OLD_SRT" ] && [ "$OLD_SRT" != "$NEW_SRT" ]; then
        if [ ! -f "$NEW_SRT" ]; then
            log_message "  -> Benenne auch um: $BASENAME.srt"
            mv "$OLD_SRT" "$NEW_SRT"
        fi
    fi
done

log_message "Umbenannt: $RENAMED Video-Dateien (plus zugehörige Sidecar-Dateien)"

# --- ZUSAMMENFASSUNG ---
log_message "=========================================="
log_message "Ende: $(date)"
log_message "=========================================="
log_message "STATISTIK:"
log_message "  Erfolgreich verarbeitet: $PROCESSED"
log_message "  Übersprungen (existiert): $SKIPPED"
log_message "  Fehlgeschlagen: $FAILED"
log_message "  Umbenannt: $RENAMED"
log_message "=========================================="

echo ""
echo "Verarbeitung abgeschlossen!"
echo "Erfolgreich verarbeitet: $PROCESSED"
echo "Übersprungen: $SKIPPED"
echo "Fehlgeschlagen: $FAILED"
echo "Umbenannt: $RENAMED"
echo ""
echo "Vollständiges Log siehe: $MAIN_LOG"

exit 0