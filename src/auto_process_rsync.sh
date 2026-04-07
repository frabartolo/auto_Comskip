#!/bin/bash
#
# auto_process_rsync.sh - Comskip-Verarbeitung OHNE sshfs
#
# Workflow:
# 1. Datei per rsync vom Quell-Server lokal holen
# 2. Mit ffprobe/ffmpeg auf Korruptheit prüfen und ggf. reparieren
# 3. Comskip für Werbeerkennung
# 4. FFmpeg-Recodierung (cut_with_edl.py)
# 5. Ergebnis per rsync auf Ziel-Server kopieren
# 6. Blacklist/Log auf Ziel-Server aktualisieren
# 7. Lokale Temp-Dateien löschen
#
# WICHTIG: Globaler Netzwerk-Lock NUR während rsync (in/out) - die Rechenarbeit
# (Comskip, FFmpeg) läuft parallel auf allen Rechnern. Serialisiert wird nur
# der Netzwerkzugriff (verhindert Proxmox e1000e-Hang bei gleichzeitigem Transfer).
#

set -e
set -u

# --- KONFIGURATION ---
CRED_FILE="$HOME/.smbcredentials"

# === QUELL-SERVER ===
SOURCE_SSH_HOST="${SOURCE_SSH_HOST:-cold-lairs}"
SOURCE_REMOTE_PATH="/var/opt/shares/Videos"
SOURCE_MOUNT_DIR="${SOURCE_MOUNT_DIR:-$HOME/mount/cold-lairs-videos}"

# === ZIEL-SERVER ===
TARGET_SSH_HOST="${TARGET_SSH_HOST:-khanhiwara}"
TARGET_REMOTE_PATH="/srv/data/Videos"
TARGET_MOUNT_DIR="${TARGET_MOUNT_DIR:-$HOME/mount/khanhiwara-videos}"

# Arbeitspfade
TEMP_BASE="/tmp/comskip_work"
WORK_DIR="$TEMP_BASE/$$"
TEMP_DIR="$WORK_DIR/stage"
FAILED_UPLOAD_DIR="${FAILED_UPLOAD_DIR:-$HOME/comskip_failed_uploads}"
PYTHON_SCRIPT="./cut_with_edl.py"
COMSKIP_INI="./comskip.ini"

# Log/Blacklist/Locks: Nutze Mounts wenn vorhanden (alle Worker teilen sich die Dateien)
if [ -d "$TARGET_MOUNT_DIR" ] && [ -w "$TARGET_MOUNT_DIR" ] 2>/dev/null && \
   [ -d "$SOURCE_MOUNT_DIR" ] && [ -w "$SOURCE_MOUNT_DIR" ] 2>/dev/null; then
    USE_MOUNTS=1
    MAIN_LOG="$TARGET_MOUNT_DIR/process_summary.log"
    BLACKLIST_FILE="$TARGET_MOUNT_DIR/corrupted_files.blacklist"
    LOCK_BASE="$SOURCE_MOUNT_DIR/.comskip_locks"
else
    USE_MOUNTS=0
    MAIN_LOG="$WORK_DIR/process_summary.log"
    BLACKLIST_FILE=""
    LOCK_BASE=""
    LOCK_BASE_REMOTE="$SOURCE_REMOTE_PATH/.comskip_locks"
fi
GLOBAL_LOCK_NAME="network_global"
LOCK_TIMEOUT_MINUTES=120
GLOBAL_LOCK_TIMEOUT_MINUTES=30

PROCESSED=0
FAILED=0
SKIPPED=0
RENAMED=0

WORKER_ID="$(hostname)-$$"

# --- HILFSFUNKTIONEN ---
# WICHTIG: </dev/null bei allen SSH-Befehlen, damit stdin (Dateiliste) nicht verbraucht wird!
ssh_cmd() {
    sshpass -p "$SSH_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no -n "$SSH_USER@$1" "$2"
}

rsync_from() {
    local host="$1"
    local remote_path="$2"
    local local_path="$3"
    rsync -avz --progress -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
        "$SSH_USER@$host:$remote_path" "$local_path" 2>/dev/null || return 1
}

rsync_to() {
    local local_path="$1"
    local host="$2"
    local remote_path="$3"
    rsync -avz --progress -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
        "$local_path" "$SSH_USER@$host:$remote_path" 2>/dev/null || return 1
}

log_message() {
    local msg="$1"
    echo "$msg"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $msg" >> "$MAIN_LOG"
}

append_log_to_remote() {
    [ "$USE_MOUNTS" -eq 1 ] && return 0
    [ ! -f "$MAIN_LOG" ] && return 0
    LOG_REMOTE="$TARGET_REMOTE_PATH/process_summary.log"
    ssh_cmd "$TARGET_SSH_HOST" "mkdir -p $(dirname "$LOG_REMOTE")" 2>/dev/null || true
    sshpass -p "$SSH_PASS" scp -o StrictHostKeyChecking=no -q "$MAIN_LOG" \
        "$SSH_USER@$TARGET_SSH_HOST:/tmp/comskip_log_$$.tmp" 2>/dev/null && \
    ssh_cmd "$TARGET_SSH_HOST" "cat /tmp/comskip_log_$$.tmp >> $LOG_REMOTE 2>/dev/null; rm -f /tmp/comskip_log_$$.tmp" 2>/dev/null || true
}

# --- BLACKLIST ---
fetch_blacklist() {
    if [ "$USE_MOUNTS" -eq 1 ]; then
        [ -f "$BLACKLIST_FILE" ] && cp -f "$BLACKLIST_FILE" "$WORK_DIR/blacklist.txt" || touch "$WORK_DIR/blacklist.txt"
    else
        ssh_cmd "$TARGET_SSH_HOST" "[ -f $TARGET_REMOTE_PATH/corrupted_files.blacklist ] && cat $TARGET_REMOTE_PATH/corrupted_files.blacklist || true" 2>/dev/null > "$WORK_DIR/blacklist.txt" || touch "$WORK_DIR/blacklist.txt"
    fi
}

is_blacklisted() {
    local filename="$1"
    [ ! -f "$WORK_DIR/blacklist.txt" ] && return 1
    grep -qxF "$filename" "$WORK_DIR/blacklist.txt" 2>/dev/null
}

add_to_blacklist_remote() {
    local filename="$1"
    log_message "  -> Füge zu Blacklist hinzu: $filename"
    printf '%s\n' "$filename" >> "$WORK_DIR/blacklist.txt"
    if [ "$USE_MOUNTS" -eq 1 ]; then
        printf '%s\n' "$filename" >> "$BLACKLIST_FILE" 2>/dev/null || log_message "  ⚠ Blacklist-Update fehlgeschlagen"
    else
        printf '%s\n' "$filename" | sshpass -p "$SSH_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$SSH_USER@$TARGET_SSH_HOST" "cat >> $TARGET_REMOTE_PATH/corrupted_files.blacklist" 2>/dev/null || log_message "  ⚠ Blacklist-Update fehlgeschlagen"
    fi
}

# --- LOCK-FUNKTIONEN ---
generate_lock_key() {
    local input="$1"
    if command -v sha256sum >/dev/null 2>&1; then
        echo "$input" | sha256sum | cut -d' ' -f1
    else
        echo "$input" | md5sum | cut -d' ' -f1
    fi
}

acquire_global_lock() {
    local lock_dir base
    if [ "$USE_MOUNTS" -eq 1 ]; then
        base="$LOCK_BASE"
        lock_dir="$base/${GLOBAL_LOCK_NAME}.lck"
    else
        base="$LOCK_BASE_REMOTE"
        lock_dir="$base/${GLOBAL_LOCK_NAME}.lck"
    fi
    local retries=60 interval=10

    while [ $retries -gt 0 ]; do
        if [ "$USE_MOUNTS" -eq 1 ]; then
            mkdir -p "$base" 2>/dev/null && mkdir "$lock_dir" 2>/dev/null && { echo "$WORKER_ID:$(date +%s)" > "$lock_dir/info" 2>/dev/null; return 0; }
            local info; info=$(cat "$lock_dir/info" 2>/dev/null || echo "unknown:0")
        else
            ssh_cmd "$SOURCE_SSH_HOST" "mkdir -p $base && mkdir $lock_dir 2>/dev/null" && { ssh_cmd "$SOURCE_SSH_HOST" "echo '$WORKER_ID:$(date +%s)' > $lock_dir/info" 2>/dev/null; return 0; }
            local info; info=$(ssh_cmd "$SOURCE_SSH_HOST" "cat $lock_dir/info 2>/dev/null || echo 'unknown:0'" 2>/dev/null) || info="unknown:0"
        fi
        local lock_time now age_minutes
        lock_time=$(echo "$info" | cut -d: -f2)
        now=$(date +%s)
        age_minutes=$(( (now - lock_time) / 60 ))
        if [ "$age_minutes" -ge "$GLOBAL_LOCK_TIMEOUT_MINUTES" ]; then
            log_message "  -> Staler Global-Lock (${age_minutes}min), übernehme..."
            [ "$USE_MOUNTS" -eq 1 ] && rm -rf "$lock_dir" 2>/dev/null || ssh_cmd "$SOURCE_SSH_HOST" "rm -rf $lock_dir" 2>/dev/null
            sleep 2
        else
            log_message "  -> Warte auf Global-Lock (${retries}s verbleibend)..."
            sleep $interval
        fi
        retries=$((retries - 1))
    done
    return 1
}

release_global_lock() {
    local lock_dir
    if [ "$USE_MOUNTS" -eq 1 ]; then
        lock_dir="$LOCK_BASE/${GLOBAL_LOCK_NAME}.lck"
        rm -rf "$lock_dir" 2>/dev/null || true
    else
        lock_dir="$LOCK_BASE_REMOTE/${GLOBAL_LOCK_NAME}.lck"
        ssh_cmd "$SOURCE_SSH_HOST" "rm -rf $lock_dir" 2>/dev/null || true
    fi
}

try_claim_file() {
    local file_key="$1" lock_dir base
    if [ "$USE_MOUNTS" -eq 1 ]; then
        base="$LOCK_BASE"
        lock_dir="$base/${file_key}.lck"
        mkdir -p "$base" 2>/dev/null || true
        if mkdir "$lock_dir" 2>/dev/null; then
            echo "$WORKER_ID:$(date +%s)" > "$lock_dir/info" 2>/dev/null
            return 0
        fi
        local info; info=$(cat "$lock_dir/info" 2>/dev/null) || info=""
    else
        base="$LOCK_BASE_REMOTE"
        lock_dir="$base/${file_key}.lck"
        if ssh_cmd "$SOURCE_SSH_HOST" "mkdir -p $base && mkdir $lock_dir 2>/dev/null"; then
            ssh_cmd "$SOURCE_SSH_HOST" "echo '$WORKER_ID:$(date +%s)' > $lock_dir/info" 2>/dev/null
            return 0
        fi
        local info; info=$(ssh_cmd "$SOURCE_SSH_HOST" "cat $lock_dir/info 2>/dev/null" 2>/dev/null) || info=""
    fi
    if [ -n "$info" ]; then
        local lock_time now age_minutes
        lock_time=$(echo "$info" | cut -d: -f2)
        now=$(date +%s)
        age_minutes=$(( (now - lock_time) / 60 ))
        if [ "$age_minutes" -ge "$LOCK_TIMEOUT_MINUTES" ]; then
            [ "$USE_MOUNTS" -eq 1 ] && rm -rf "$lock_dir" 2>/dev/null || ssh_cmd "$SOURCE_SSH_HOST" "rm -rf $lock_dir" 2>/dev/null
            sleep 1
            if [ "$USE_MOUNTS" -eq 1 ]; then
                mkdir "$lock_dir" 2>/dev/null && { echo "$WORKER_ID:$(date +%s)" > "$lock_dir/info"; return 0; }
            else
                ssh_cmd "$SOURCE_SSH_HOST" "mkdir $lock_dir 2>/dev/null" && ssh_cmd "$SOURCE_SSH_HOST" "echo '$WORKER_ID:$(date +%s)' > $lock_dir/info" 2>/dev/null && return 0
            fi
        fi
    fi
    return 1
}

release_file() {
    local file_key="$1" lock_dir
    if [ "$USE_MOUNTS" -eq 1 ]; then
        lock_dir="$LOCK_BASE/${file_key}.lck"
        rm -rf "$lock_dir" 2>/dev/null || true
    else
        lock_dir="$LOCK_BASE_REMOTE/${file_key}.lck"
        ssh_cmd "$SOURCE_SSH_HOST" "rm -rf $lock_dir" 2>/dev/null || true
    fi
}

# --- DATEILISTE ---
get_file_list() {
    if [ "$USE_MOUNTS" -eq 1 ]; then
        find "$SOURCE_MOUNT_DIR" -type f \( -iname '*.mp4' -o -iname '*.m4v' -o -iname '*.mkv' -o -iname '*.ts' -o -iname '*.mpeg' -o -iname '*.mpg' -o -iname '*.mov' -o -iname '*.webm' -o -iname '*.avi' -o -iname '*.divx' \) 2>/dev/null
    else
        ssh_cmd "$SOURCE_SSH_HOST" "find $SOURCE_REMOTE_PATH -type f \( -iname '*.mp4' -o -iname '*.m4v' -o -iname '*.mkv' -o -iname '*.ts' -o -iname '*.mpeg' -o -iname '*.mpg' -o -iname '*.mov' -o -iname '*.webm' -o -iname '*.avi' -o -iname '*.divx' \) 2>/dev/null"
    fi
}

# --- CREDENTIALS ---
if [ ! -f "$CRED_FILE" ]; then
    echo "FEHLER: $CRED_FILE nicht gefunden!"
    exit 1
fi

SSH_USER=$(grep "username" "$CRED_FILE" | cut -d'=' -f2 | xargs)
SSH_PASS=$(grep "password" "$CRED_FILE" | cut -d'=' -f2 | xargs)

[ -z "$SSH_USER" ] || [ -z "$SSH_PASS" ] && { echo "FEHLER: Username/Passwort fehlt!"; exit 1; }

# --- VORBEREITUNG ---
mkdir -p "$WORK_DIR" "$TEMP_DIR"
touch "$MAIN_LOG"

log_message "=========================================="
log_message "auto_process_rsync.sh - Start ($(date))"
log_message "=========================================="
log_message "Worker: $WORKER_ID"
log_message "Quell-Server: $SOURCE_SSH_HOST"
log_message "Ziel-Server: $TARGET_SSH_HOST"
[ "$USE_MOUNTS" -eq 1 ] && log_message "Modus: Mounts (Log/Blacklist/Locks auf $TARGET_MOUNT_DIR, $SOURCE_MOUNT_DIR)" || log_message "Modus: SSH-only (Log/Blacklist/Locks per SSH)"

# SSH-Verbindung prüfen (zeigt echten Fehler bei Misserfolg)
check_ssh() {
    local host="$1"
    local err
    err=$(sshpass -p "$SSH_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$SSH_USER@$host" "echo OK" 2>&1)
    local ret=$?
    if [ $ret -eq 0 ]; then return 0; fi
    log_message "FEHLER: Keine SSH-Verbindung zu $host (Exit $ret)"
    log_message "  -> SSH-Ausgabe: $err"
    echo "Tipp: Prüfe Host-Auflösung (ping $host), SSH-Port und ~/.smbcredentials (username/password)"
    return 1
}
if ! check_ssh "$SOURCE_SSH_HOST"; then exit 1; fi
if ! check_ssh "$TARGET_SSH_HOST"; then exit 1; fi

# Dateiliste holen (ohne Global-Lock - kleines Kommando)
log_message "Hole Dateiliste..."
FILE_LIST=$(mktemp)
get_file_list > "$FILE_LIST" 2>/dev/null || true

VIDEO_COUNT=$(wc -l < "$FILE_LIST")
log_message "Gefunden: $VIDEO_COUNT Video-Dateien"

if [ "$VIDEO_COUNT" -eq 0 ]; then
    log_message "Keine Dateien zu verarbeiten."
    rm -f "$FILE_LIST"
    append_log_to_remote
    rm -rf "$WORK_DIR"
    exit 0
fi

[ ! -f "$PYTHON_SCRIPT" ] && { log_message "FEHLER: $PYTHON_SCRIPT nicht gefunden"; exit 1; }

# --- EINMALIG: Blacklist + vorhandene MKV-Dateien laden ---
log_message "Lade Blacklist und Liste vorhandener Dateien..."
fetch_blacklist
if [ "$USE_MOUNTS" -eq 1 ]; then
    find "$TARGET_MOUNT_DIR" -type f -name '*.mkv' 2>/dev/null | sed "s|^$TARGET_MOUNT_DIR|$TARGET_REMOTE_PATH|" > "$WORK_DIR/existing_mkv.txt" || touch "$WORK_DIR/existing_mkv.txt"
else
    ssh_cmd "$TARGET_SSH_HOST" "find $TARGET_REMOTE_PATH -type f -name '*.mkv' 2>/dev/null" 2>/dev/null > "$WORK_DIR/existing_mkv.txt" || touch "$WORK_DIR/existing_mkv.txt"
fi

is_already_on_target() {
    local path="$1"
    [ ! -f "$WORK_DIR/existing_mkv.txt" ] && return 1
    grep -qxF "$path" "$WORK_DIR/existing_mkv.txt" 2>/dev/null
}

add_to_existing_list() {
    echo "$1" >> "$WORK_DIR/existing_mkv.txt"
}

# --- VERARBEITUNG ---
while IFS= read -r REMOTE_FILE; do
    [ -z "$REMOTE_FILE" ] && continue

    if [ "$USE_MOUNTS" -eq 1 ]; then
        REL_PATH="${REMOTE_FILE#$SOURCE_MOUNT_DIR/}"
        RSYNC_SOURCE_PATH="$SOURCE_SSH_HOST:$SOURCE_REMOTE_PATH/$REL_PATH"
    else
        REL_PATH="${REMOTE_FILE#$SOURCE_REMOTE_PATH/}"
        RSYNC_SOURCE_PATH="$SOURCE_SSH_HOST:$REMOTE_FILE"
    fi
    REL_DIR=$(dirname "$REL_PATH")
    FILENAME=$(basename "$REMOTE_FILE")
    FILE_BASE="${FILENAME%.*}"
    EXTENSION="${REMOTE_FILE##*.}"
    TARGET_REL_DIR="$TARGET_REMOTE_PATH/$REL_DIR"
    TARGET_FILENAME="${FILE_BASE}.mkv"
    TARGET_REMOTE_FILE="$TARGET_REL_DIR/$TARGET_FILENAME"

    # Blacklist (einmal geladen)
    if is_blacklisted "$FILENAME"; then
        echo "Überspringe (Blacklist): $FILENAME"
        ((SKIPPED++)) || true
        continue
    fi

    CLEAN_NAME=$(echo "$FILE_BASE" | sed 's/__.*//')
    TARGET_CLEAN="$TARGET_REL_DIR/$CLEAN_NAME.mkv"

    # Bereits vorhanden? (lokale Liste, kein SSH pro Datei)
    if is_already_on_target "$TARGET_REL_DIR/$TARGET_FILENAME"; then
        echo "Überspringe (bereits vorhanden): $TARGET_FILENAME"
        ((SKIPPED++)) || true
        continue
    fi
    if [ "$FILENAME" != "$CLEAN_NAME" ] && is_already_on_target "$TARGET_CLEAN"; then
        echo "Überspringe (bereits als $CLEAN_NAME.mkv)"
        ((SKIPPED++)) || true
        continue
    fi

    LOCK_KEY=$(generate_lock_key "$REL_PATH")
    if ! try_claim_file "$LOCK_KEY"; then
        echo "Überspringe (in Bearbeitung): $FILENAME"
        ((SKIPPED++)) || true
        continue
    fi

    log_message "------------------------------------------"
    log_message "Verarbeite: $FILENAME"

    LOCAL_INPUT="$TEMP_DIR/input_$FILENAME"
    LOCAL_OUTPUT="$TEMP_DIR/output_${FILE_BASE}.mkv"
    mkdir -p "$TEMP_DIR"

    # 1. rsync vom Quell-Server (NUR HIER: Globaler Netzwerk-Lock)
    if ! acquire_global_lock; then
        log_message "Kein Global-Lock erhalten - überspringe"
        release_file "$LOCK_KEY"
        ((SKIPPED++)) || true
        continue
    fi
    log_message "Schritt 1: Lade Datei (+ Sidecars) vom Quell-Server..."
    if ! rsync -avz -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
        "$SSH_USER@$RSYNC_SOURCE_PATH" "$LOCAL_INPUT" 2>/dev/null; then
        log_message "  ✗ rsync von Quell-Server fehlgeschlagen"
        release_file "$LOCK_KEY"
        release_global_lock
        ((FAILED++)) || true
        rm -f "$LOCAL_INPUT"
        continue
    fi
    REMOTE_BASE="$SOURCE_REMOTE_PATH/${REL_PATH%.*}"
    for ext in srt txt xml; do
        rsync -az -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
            "$SSH_USER@$SOURCE_SSH_HOST:${REMOTE_BASE}.${ext}" "$TEMP_DIR/" 2>/dev/null || true
    done
    release_global_lock

    # 2. ffprobe / Repair
    log_message "Schritt 2: Prüfe Datei..."
    WORKING_FILE="$LOCAL_INPUT"
    TEMP_REPAIRED=""
    if ! ffprobe -v error "$LOCAL_INPUT" >/dev/null 2>&1; then
        log_message "  ⚠ ffprobe meldet Fehler, Reparatur..."
        TEMP_REPAIRED="$TEMP_DIR/repaired_$$.ts"
        if ffmpeg -nostdin -v error -err_detect ignore_err -i "$LOCAL_INPUT" -c copy -y "$TEMP_REPAIRED" >/dev/null 2>&1 && [ -s "$TEMP_REPAIRED" ]; then
            WORKING_FILE="$TEMP_REPAIRED"
            log_message "  ✓ Reparatur OK, Comskip/FFmpeg nutzen diese Datei"
        else
            log_message "  ✗ Reparatur fehlgeschlagen"
            add_to_blacklist_remote "$FILENAME"
            release_file "$LOCK_KEY"
            ((FAILED++)) || true
            rm -f "$LOCAL_INPUT" "$TEMP_REPAIRED"
            continue
        fi
    else
        log_message "  ✓ ffprobe OK (keine Vorab-Reparatur)"
    fi

    # 3. Comskip (bei Fehler: Remux von Original, zweiter Lauf; erneuter Fehler → dirty/Blacklist)
    log_message "Schritt 3: Comskip..."
    rm -f "$TEMP_DIR"/*.edl
    COMSKIP_EXIT=0
    if [ -f "$COMSKIP_INI" ]; then
        comskip --ini="$COMSKIP_INI" --output="$TEMP_DIR" --quiet -- "$WORKING_FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || COMSKIP_EXIT=$?
    else
        comskip --output="$TEMP_DIR" --quiet -- "$WORKING_FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || COMSKIP_EXIT=$?
    fi

    if [ "$COMSKIP_EXIT" -ne 0 ]; then
        log_message "  ⚠ Comskip fehlgeschlagen (Exit $COMSKIP_EXIT), Reparatur und zweiter Versuch..."
        rm -f "$TEMP_DIR"/*.edl
        [ -n "$TEMP_REPAIRED" ] && [ -f "$TEMP_REPAIRED" ] && rm -f "$TEMP_REPAIRED"
        TEMP_REPAIRED="$TEMP_DIR/comskip_retry_$$.ts"
        if ffmpeg -nostdin -v error -err_detect ignore_err -i "$LOCAL_INPUT" -c copy -y "$TEMP_REPAIRED" >/dev/null 2>&1 && [ -s "$TEMP_REPAIRED" ]; then
            WORKING_FILE="$TEMP_REPAIRED"
            log_message "  ✓ Reparatur OK, zweiter Comskip-Lauf..."
            COMSKIP_EXIT=0
            if [ -f "$COMSKIP_INI" ]; then
                comskip --ini="$COMSKIP_INI" --output="$TEMP_DIR" --quiet -- "$WORKING_FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || COMSKIP_EXIT=$?
            else
                comskip --output="$TEMP_DIR" --quiet -- "$WORKING_FILE" < /dev/null >> "$MAIN_LOG" 2>&1 || COMSKIP_EXIT=$?
            fi
        else
            log_message "  ✗ Reparatur für Comskip-Retry fehlgeschlagen"
            COMSKIP_EXIT=1
        fi

        if [ "$COMSKIP_EXIT" -ne 0 ]; then
            log_message "  ✗ Comskip weiterhin fehlgeschlagen – Datei als dirty markiert"
            add_to_blacklist_remote "$FILENAME"
            release_file "$LOCK_KEY"
            ((FAILED++)) || true
            rm -f "$LOCAL_INPUT" "$TEMP_REPAIRED" "$TEMP_DIR"/*.edl "$TEMP_DIR"/*.srt "$TEMP_DIR"/*.txt "$TEMP_DIR"/*.xml 2>/dev/null || true
            continue
        fi
    fi

    EDL_FILE=$(find "$TEMP_DIR" -name "*.edl" 2>/dev/null | head -n 1)
    EDL_ARG="${EDL_FILE:-none}"

    # 4. cut_with_edl.py (WORKING_FILE = Original oder reparierte Datei – nicht vorher löschen!)
    log_message "Schritt 4: FFmpeg-Recodierung..."

    SRT_ARG="none"
    METADATA_ARG="none"
    [ -f "$TEMP_DIR/$(basename "${REMOTE_BASE}.srt")" ] 2>/dev/null && SRT_ARG="$TEMP_DIR/$(basename "${REMOTE_BASE}.srt")"
    [ -f "$TEMP_DIR/$(basename "${REMOTE_BASE}.txt")" ] 2>/dev/null && METADATA_ARG="$TEMP_DIR/$(basename "${REMOTE_BASE}.txt")"
    [ -z "$METADATA_ARG" ] || [ "$METADATA_ARG" = "none" ] && [ -f "$TEMP_DIR/$(basename "${REMOTE_BASE}.xml")" ] 2>/dev/null && METADATA_ARG="$TEMP_DIR/$(basename "${REMOTE_BASE}.xml")"

    cp -f "$WORK_DIR/blacklist.txt" "$WORK_DIR/corrupted_files.blacklist" 2>/dev/null || true

    if python3 "$PYTHON_SCRIPT" "$WORKING_FILE" "$EDL_ARG" "$LOCAL_OUTPUT" "$SRT_ARG" "$METADATA_ARG" "$MAIN_LOG" < /dev/null; then
        PYTHON_EXIT=0
    else
        PYTHON_EXIT=$?
    fi
    [ -n "$TEMP_REPAIRED" ] && rm -f "$TEMP_REPAIRED"

    if [ $PYTHON_EXIT -eq 0 ] && [ -f "$LOCAL_OUTPUT" ]; then
        # 5. rsync zum Ziel-Server (NUR HIER: Globaler Netzwerk-Lock)
        if ! acquire_global_lock; then
            log_message "  ✗ Kein Global-Lock zum Kopieren - Ergebnis bleibt lokal"
            ((FAILED++)) || true
        else
            log_message "Schritt 5: Kopiere auf Ziel-Server..."
            ssh_cmd "$TARGET_SSH_HOST" "mkdir -p $TARGET_REL_DIR" 2>/dev/null || true
            RSYNC_ERR=$(rsync -avz -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
                "$LOCAL_OUTPUT" "$SSH_USER@$TARGET_SSH_HOST:$TARGET_REL_DIR/$TARGET_FILENAME" 2>&1)
            if [ $? -eq 0 ]; then
                [ -n "$SRT_ARG" ] && [ "$SRT_ARG" != "none" ] && rsync -az -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
                    "$SRT_ARG" "$SSH_USER@$TARGET_SSH_HOST:$TARGET_REL_DIR/$FILE_BASE.srt" 2>/dev/null || true
                [ -n "$METADATA_ARG" ] && [ "$METADATA_ARG" != "none" ] && rsync -az -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
                    "$METADATA_ARG" "$SSH_USER@$TARGET_SSH_HOST:$TARGET_REL_DIR/$FILE_BASE.${METADATA_ARG##*.}" 2>/dev/null || true

                log_message "  ✓ Erfolgreich verarbeitet"
                add_to_existing_list "$TARGET_REL_DIR/$TARGET_FILENAME"
                ((PROCESSED++)) || true
            else
                log_message "  ✗ rsync zum Ziel FEHLGESCHLAGEN"
                log_message "  -> Fehler: $RSYNC_ERR"
                echo "FEHLER rsync → Ziel: $RSYNC_ERR"
                mkdir -p "$FAILED_UPLOAD_DIR"
                SAVE_SUBDIR="$FAILED_UPLOAD_DIR/$REL_DIR"
                mkdir -p "$SAVE_SUBDIR"
                SAVE_PATH="$SAVE_SUBDIR/$TARGET_FILENAME"
                cp -f "$LOCAL_OUTPUT" "$SAVE_PATH" && log_message "  -> Gesichert unter: $SAVE_PATH" || log_message "  -> WARNUNG: Sicherung fehlgeschlagen!"
                release_global_lock
                release_file "$LOCK_KEY"
                append_log_to_remote 2>/dev/null || true
                echo ""
                echo "ABBRUCH: rsync zum Ziel fehlgeschlagen. Fertige Datei gesichert unter: $SAVE_PATH"
                echo "Behebe das Problem (Platte voll? Rechte? Pfad?) und starte das Skript erneut."
                exit 1
            fi
            release_global_lock
        fi
    elif [ $PYTHON_EXIT -eq 9 ] || [ $PYTHON_EXIT -eq 6 ]; then
        add_to_blacklist_remote "$FILENAME"
        ((FAILED++)) || true
    else
        log_message "  ✗ Fehler (Exit: $PYTHON_EXIT)"
        ((FAILED++)) || true
    fi

    release_file "$LOCK_KEY"

    rm -f "$LOCAL_INPUT" "$LOCAL_OUTPUT" "$TEMP_DIR"/*.edl "$TEMP_DIR"/*.srt "$TEMP_DIR"/*.txt "$TEMP_DIR"/*.xml 2>/dev/null || true

done < "$FILE_LIST"

rm -f "$FILE_LIST"

# --- UMBENENNUNG (auf Ziel) ---
log_message "Prüfe Umbenennung..."
_rename_script="$(dirname "$0")/rename_duplicates_remote.sh"
if [ -f "$_rename_script" ]; then
  if [ "$USE_MOUNTS" -eq 1 ]; then
    bash "$_rename_script" "$TARGET_MOUNT_DIR" 2>/dev/null || true
  else
    sshpass -p "$SSH_PASS" scp -o StrictHostKeyChecking=no -q "$_rename_script" "$SSH_USER@$TARGET_SSH_HOST:/tmp/rename_$$.sh" 2>/dev/null && \
    sshpass -p "$SSH_PASS" ssh -o StrictHostKeyChecking=no "$SSH_USER@$TARGET_SSH_HOST" "bash /tmp/rename_$$.sh $TARGET_REMOTE_PATH; rm -f /tmp/rename_$$.sh" 2>/dev/null || true
  fi
fi

# --- LOG SYNC ---
append_log_to_remote

# --- CLEANUP ---
log_message "=========================================="
log_message "Ende: $(date)"
log_message "STATISTIK - $WORKER_ID: Erfolgreich: $PROCESSED, Übersprungen: $SKIPPED, Fehler: $FAILED"
log_message "=========================================="
append_log_to_remote

rm -rf "$WORK_DIR"

echo ""
echo "Fertig! Erfolgreich: $PROCESSED, Übersprungen: $SKIPPED, Fehler: $FAILED"

exit 0
