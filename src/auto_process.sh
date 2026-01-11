#!/bin/bash

# --- KONFIGURATION ---
CRED_FILE="/home/stefan/.smbcredentials"
SSH_HOST="cold-lairs"
REMOTE_PATH="/var/opt/shares/Videos"
MOUNT_DIR="$HOME/mount/cold-lairs-videos"

TARGET_BASE="/srv/data/Videos"
TEMP_DIR="/tmp/comskip_work"
MAIN_LOG="/srv/data/Videos/process_summary.log"
PYTHON_SCRIPT="./cut_with_edl.py"

# --- ZUGANGSDATEN AUS DATEI AUSLESEN ---
if [ -f "$CRED_FILE" ]; then
    # Extrahiert Username und Passwort (löscht Leerzeichen um das '=')
    SSH_USER=$(grep "username" "$CRED_FILE" | cut -d'=' -f2 | xargs)
    SSH_PASS=$(grep "password" "$CRED_FILE" | cut -d'=' -f2 | xargs)
else
    echo "FEHLER: Datei $CRED_FILE nicht gefunden!"
    exit 1
fi

# --- MOUNT LOGIK ---
mkdir -p "$MOUNT_DIR"

# Prüfen, ob das Verzeichnis bereits gemountet ist
if mountpoint -q "$MOUNT_DIR"; then
    echo "Info: $MOUNT_DIR ist bereits gemountet."
else
    echo "Versuche $MOUNT_DIR zu mounten (Daten aus credentials-Datei)..."
    sshpass -p "$SSH_PASS" sshfs -o allow_other,default_permissions "$SSH_USER@$SSH_HOST:$REMOTE_PATH" "$MOUNT_DIR"

    if [ $? -eq 0 ]; then
        echo "Mount erfolgreich."
    else
        echo "FEHLER: Mounten fehlgeschlagen. Prüfe $CRED_FILE und die Netzwerkverbindung."
        exit 1
    fi
fi

# --- AB HIER FOLGT DER BEKANNTE VERARBEITUNGSTEIL ---
mkdir -p "$TEMP_DIR"
echo "--- Start Durchlauf: $(date) ---" >> "$MAIN_LOG"

find "$MOUNT_DIR" -type f \( -iname "*.mp4" -o -iname "*.mkv" -o -iname "*.ts" -o -iname "*.divx" -o -iname ".m4v"\) | while read -r FILE; do
    FILE_BASE="${FILE%.*}"
    FILENAME=$(basename "$FILE_BASE")
    REL_DIR=$(dirname "${FILE#$MOUNT_DIR/}")
    TARGET_DIR="$TARGET_BASE/$REL_DIR"
    TARGET_FILE="$TARGET_DIR/$FILENAME.mkv"

    if [ -f "$TARGET_FILE" ]; then continue; fi

    echo "Verarbeite: $FILENAME"
    mkdir -p "$TARGET_DIR"
    echo "[START] $FILENAME - $(date)" >> "$MAIN_LOG"

    SRT_FILE="${FILE_BASE}.srt"; [ ! -f "$SRT_FILE" ] && SRT_FILE="none"
    TXT_FILE="${FILE_BASE}.txt"; [ ! -f "$TXT_FILE" ] && TXT_FILE="none"

    comskip --ini=comskip.ini --output="$TEMP_DIR" --quiet -- "$FILE" < /dev/null >> "$MAIN_LOG" 2>&1
    EDL_FILE=$(find "$TEMP_DIR" -name "*.edl" | head -n 1)

    # Use EDL if found, otherwise pass "none" so Python re-encodes
    EDL_ARG="${EDL_FILE:-.}"
    [ ! -f "$EDL_ARG" ] && EDL_ARG="none"
    
    echo "Schritt 2: Schneiden und Metadaten einbetten..."
    # Hier wichtig: < /dev/null am Ende des Python-Aufrufs!
    python3 "$PYTHON_SCRIPT" "$FILE" "$EDL_ARG" "$TARGET_FILE" "$SRT_FILE" "$TXT_FILE" "$MAIN_LOG" < /dev/null
    
    if [ -f "$TARGET_FILE" ]; then
        echo "[OK] $FILENAME" >> "$MAIN_LOG"
    else
        echo "[FEHLER] $FILENAME (Schnitt fehlgeschlagen)" >> "$MAIN_LOG"
    fi

    rm -rf "$TEMP_DIR"/*
done
echo "--- Ende Durchlauf: $(date) ---" >> "$MAIN_LOG"

# Function to rename files to their base name
rename_files() {
    for FILE in "$SOURCE_DIR"/*; do
        BASENAME=$(basename "$FILE" | sed 's/__.*//')
        mv "$FILE" "$SOURCE_DIR/$BASENAME"
    done
}

# Call the rename function
rename_files
