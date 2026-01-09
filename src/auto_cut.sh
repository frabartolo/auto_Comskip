#!/bin/bash

SOURCE_DIR="$HOME/mount/cold-lairs-videos/neu/Anne_Will_20230917"
TARGET_BASE="/srv/data/Videos"
TEMP_DIR="/tmp/comskip_work"
MAIN_LOG="$TARGET_BASE/process_summary.log"

mkdir -p "$TEMP_DIR"
echo "--- Start Durchlauf: $(date) ---" >> "$MAIN_LOG"

find "$SOURCE_DIR" -type f \( -iname "*.mp4" -o -iname "*.mkv" -o -iname "*.ts" -o -iname "*.divx" -o -iname "*.xml" \) | while read -r FILE; do
    # Process XML files if they exist
    if [[ "${FILE,,}" == *.xml ]]; then
        # Handle XML file processing here
        echo "Processing XML file: $FILE"
    fi

    FILE_BASE="${FILE%.*}"
    FILENAME_NO_EXT=$(basename "$FILE_BASE")
    REL_DIR=$(dirname "${FILE#$SOURCE_DIR/}")

    # Ziel als MKV für bessere Metadaten-Unterstützung
    TARGET_FILE="$TARGET_BASE/$REL_DIR/$FILENAME_NO_EXT.mkv"
    mkdir -p "$(dirname "$TARGET_FILE")"

    if [ -f "$TARGET_FILE" ]; then continue; fi

    # Suche nach Zusatzdateien
    SRT_FILE="${FILE_BASE}.srt"
    TXT_FILE="${FILE_BASE}.txt"

    [ ! -f "$SRT_FILE" ] && SRT_FILE="none"
    [ ! -f "$TXT_FILE" ] && TXT_FILE="none"

    echo "Verarbeite: $FILENAME_NO_EXT"
    echo "[START] $FILENAME_NO_EXT - $(date)" >> "$MAIN_LOG"

    # Comskip Analyse
    comskip --output="$TEMP_DIR" "$FILE" >> "$MAIN_LOG" 2>&1
    EDL_FILE="$TEMP_DIR/$FILENAME_NO_EXT.edl"

    if [ -f "$EDL_FILE" ]; then
        # Aufruf des Python-Skripts mit optionalen Dateien
        ./cut_with_edl.py "$FILE" "$EDL_FILE" "$TARGET_FILE" "$SRT_FILE" "$TXT_FILE" "$MAIN_LOG"
    fi

    rm -f "$TEMP_DIR"/*
done
echo "--- Ende Durchlauf: $(date) ---" >> "$MAIN_LOG"
