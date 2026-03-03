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
SOURCE_SSH_HOST="cold-lairs"
SOURCE_REMOTE_PATH="/var/opt/shares/Videos"

# === ZIEL-SERVER ===
TARGET_SSH_HOST="khanhiwara"
TARGET_REMOTE_PATH="/srv/data/Videos"

# Arbeitspfade (alles lokal)
TEMP_BASE="/tmp/comskip_work"
WORK_DIR="$TEMP_BASE/$$"
TEMP_DIR="$WORK_DIR/stage"
MAIN_LOG_LOCAL="$WORK_DIR/process_summary.log"
PYTHON_SCRIPT="./cut_with_edl.py"
COMSKIP_INI="./comskip.ini"

# Remote-Pfade für Log/Blacklist
BLACKLIST_REMOTE="$TARGET_REMOTE_PATH/corrupted_files.blacklist"
LOG_REMOTE="$TARGET_REMOTE_PATH/process_summary.log"

# Lock-Pfad auf Quell-Server
LOCK_BASE_REMOTE="$SOURCE_REMOTE_PATH/.comskip_locks"
GLOBAL_LOCK_NAME="network_global"
LOCK_TIMEOUT_MINUTES=120
GLOBAL_LOCK_TIMEOUT_MINUTES=30

PROCESSED=0
FAILED=0
SKIPPED=0
RENAMED=0

WORKER_ID="$(hostname)-$$"

# --- HILFSFUNKTIONEN ---
ssh_cmd() {
    sshpass -p "$SSH_PASS" ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no "$SSH_USER@$1" "$2"
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
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $msg" >> "$MAIN_LOG_LOCAL"
}

append_log_to_remote() {
    [ -f "$MAIN_LOG_LOCAL" ] || return 0
    ssh_cmd "$TARGET_SSH_HOST" "mkdir -p $(dirname "$LOG_REMOTE")" 2>/dev/null || true
    sshpass -p "$SSH_PASS" scp -o StrictHostKeyChecking=no -q "$MAIN_LOG_LOCAL" \
        "$SSH_USER@$TARGET_SSH_HOST:/tmp/comskip_log_$$.tmp" 2>/dev/null && \
    ssh_cmd "$TARGET_SSH_HOST" "cat /tmp/comskip_log_$$.tmp >> $LOG_REMOTE 2>/dev/null; rm -f /tmp/comskip_log_$$.tmp" 2>/dev/null || true
}

# --- BLACKLIST ---
fetch_blacklist() {
    ssh_cmd "$TARGET_SSH_HOST" "[ -f $BLACKLIST_REMOTE ] && cat $BLACKLIST_REMOTE || true" 2>/dev/null > "$WORK_DIR/blacklist.txt" || touch "$WORK_DIR/blacklist.txt"
}

is_blacklisted() {
    local filename="$1"
    [ ! -f "$WORK_DIR/blacklist.txt" ] && return 1
    grep -qxF "$filename" "$WORK_DIR/blacklist.txt" 2>/dev/null
}

add_to_blacklist_remote() {
    local filename="$1"
    log_message "  -> Füge zu Blacklist hinzu: $filename"
    printf '%s\n' "$filename" | sshpass -p "$SSH_PASS" ssh -o StrictHostKeyChecking=no "$SSH_USER@$TARGET_SSH_HOST" "cat >> $BLACKLIST_REMOTE" 2>/dev/null || log_message "  ⚠ Blacklist-Update fehlgeschlagen"
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
    local lock_dir="$LOCK_BASE_REMOTE/${GLOBAL_LOCK_NAME}.lck"
    local retries=60
    local interval=10

    while [ $retries -gt 0 ]; do
        if ssh_cmd "$SOURCE_SSH_HOST" "mkdir -p $LOCK_BASE_REMOTE && mkdir $lock_dir 2>/dev/null"; then
            ssh_cmd "$SOURCE_SSH_HOST" "echo '$WORKER_ID:$(date +%s)' > $lock_dir/info" 2>/dev/null || true
            return 0
        fi

        local info
        info=$(ssh_cmd "$SOURCE_SSH_HOST" "cat $lock_dir/info 2>/dev/null || echo 'unknown:0'") || info="unknown:0"
        local lock_time
        lock_time=$(echo "$info" | cut -d: -f2)
        local now
        now=$(date +%s)
        local age_minutes=$(( (now - lock_time) / 60 ))

        if [ "$age_minutes" -ge "$GLOBAL_LOCK_TIMEOUT_MINUTES" ]; then
            log_message "  -> Staler Global-Lock (${age_minutes}min), übernehme..."
            ssh_cmd "$SOURCE_SSH_HOST" "rm -rf $lock_dir" 2>/dev/null || true
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
    local lock_dir="$LOCK_BASE_REMOTE/${GLOBAL_LOCK_NAME}.lck"
    ssh_cmd "$SOURCE_SSH_HOST" "rm -rf $lock_dir" 2>/dev/null || true
}

try_claim_file() {
    local file_key="$1"
    local lock_dir="$LOCK_BASE_REMOTE/${file_key}.lck"

    if ssh_cmd "$SOURCE_SSH_HOST" "mkdir -p $LOCK_BASE_REMOTE && mkdir $lock_dir 2>/dev/null"; then
        ssh_cmd "$SOURCE_SSH_HOST" "echo '$WORKER_ID:$(date +%s)' > $lock_dir/info" 2>/dev/null || true
        return 0
    fi

    local info
    info=$(ssh_cmd "$SOURCE_SSH_HOST" "cat $lock_dir/info 2>/dev/null" 2>/dev/null) || info=""
    if [ -n "$info" ]; then
        local lock_time
        lock_time=$(echo "$info" | cut -d: -f2)
        local now
        now=$(date +%s)
        local age_minutes=$(( (now - lock_time) / 60 ))
        if [ "$age_minutes" -ge "$LOCK_TIMEOUT_MINUTES" ]; then
            ssh_cmd "$SOURCE_SSH_HOST" "rm -rf $lock_dir" 2>/dev/null || true
            sleep 1
            if ssh_cmd "$SOURCE_SSH_HOST" "mkdir $lock_dir 2>/dev/null"; then
                ssh_cmd "$SOURCE_SSH_HOST" "echo '$WORKER_ID:$(date +%s)' > $lock_dir/info" 2>/dev/null || true
                return 0
            fi
        fi
    fi
    return 1
}

release_file() {
    local file_key="$1"
    local lock_dir="$LOCK_BASE_REMOTE/${file_key}.lck"
    ssh_cmd "$SOURCE_SSH_HOST" "rm -rf $lock_dir" 2>/dev/null || true
}

# --- DATEILISTE VOM QUELL-SERVER ---
get_file_list() {
    ssh_cmd "$SOURCE_SSH_HOST" "find $SOURCE_REMOTE_PATH -type f \( -iname '*.mp4' -o -iname '*.m4v' -o -iname '*.mkv' -o -iname '*.ts' -o -iname '*.mpeg' -o -iname '*.mpg' -o -iname '*.mov' -o -iname '*.webm' -o -iname '*.avi' -o -iname '*.divx' \) 2>/dev/null"
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
touch "$MAIN_LOG_LOCAL"

log_message "=========================================="
log_message "auto_process_rsync.sh - Start ($(date))"
log_message "=========================================="
log_message "Worker: $WORKER_ID"
log_message "Quell-Server: $SOURCE_SSH_HOST"
log_message "Ziel-Server: $TARGET_SSH_HOST"

# SSH-Verbindung prüfen
if ! ssh_cmd "$SOURCE_SSH_HOST" "echo OK" >/dev/null 2>&1; then
    log_message "FEHLER: Keine SSH-Verbindung zu $SOURCE_SSH_HOST"
    exit 1
fi
if ! ssh_cmd "$TARGET_SSH_HOST" "echo OK" >/dev/null 2>&1; then
    log_message "FEHLER: Keine SSH-Verbindung zu $TARGET_SSH_HOST"
    exit 1
fi

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

# --- VERARBEITUNG ---
while IFS= read -r REMOTE_FILE; do
    [ -z "$REMOTE_FILE" ] && continue

    REL_PATH="${REMOTE_FILE#$SOURCE_REMOTE_PATH/}"
    REL_DIR=$(dirname "$REL_PATH")
    FILENAME=$(basename "$REMOTE_FILE")
    FILE_BASE="${FILENAME%.*}"
    EXTENSION="${REMOTE_FILE##*.}"
    TARGET_REL_DIR="$TARGET_REMOTE_PATH/$REL_DIR"
    TARGET_FILENAME="${FILE_BASE}.mkv"
    TARGET_REMOTE_FILE="$TARGET_REL_DIR/$TARGET_FILENAME"

    # Blacklist
    fetch_blacklist
    if is_blacklisted "$FILENAME"; then
        echo "Überspringe (Blacklist): $FILENAME"
        ((SKIPPED++)) || true
        continue
    fi

    CLEAN_NAME=$(echo "$FILE_BASE" | sed 's/__.*//')
    TARGET_CLEAN="$TARGET_REL_DIR/$CLEAN_NAME.mkv"

    # Bereits vorhanden?
    if ssh_cmd "$TARGET_SSH_HOST" "[ -f \"$TARGET_REL_DIR/$TARGET_FILENAME\" ]" 2>/dev/null; then
        echo "Überspringe (bereits vorhanden): $TARGET_FILENAME"
        ((SKIPPED++)) || true
        continue
    fi
    if [ "$FILENAME" != "$CLEAN_NAME" ] && ssh_cmd "$TARGET_SSH_HOST" "[ -f \"$TARGET_CLEAN\" ]" 2>/dev/null; then
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
        "$SSH_USER@$SOURCE_SSH_HOST:$REMOTE_FILE" "$LOCAL_INPUT" 2>/dev/null; then
        log_message "  ✗ rsync von Quell-Server fehlgeschlagen"
        release_file "$LOCK_KEY"
        release_global_lock
        ((FAILED++)) || true
        rm -f "$LOCAL_INPUT"
        continue
    fi
    REMOTE_BASE="${REMOTE_FILE%.*}"
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
        log_message "  ⚠ Datei fehlerhaft, repariere..."
        TEMP_REPAIRED="$TEMP_DIR/repaired_$$.ts"
        if ffmpeg -nostdin -v error -err_detect ignore_err -i "$LOCAL_INPUT" -c copy -y "$TEMP_REPAIRED" >/dev/null 2>&1 && [ -s "$TEMP_REPAIRED" ]; then
            WORKING_FILE="$TEMP_REPAIRED"
        else
            log_message "  ✗ Reparatur fehlgeschlagen"
            add_to_blacklist_remote "$FILENAME"
            release_file "$LOCK_KEY"
            ((FAILED++)) || true
            rm -f "$LOCAL_INPUT" "$TEMP_REPAIRED"
            continue
        fi
    fi

    # 3. Comskip
    log_message "Schritt 3: Comskip..."
    rm -f "$TEMP_DIR"/*.edl
    if [ -f "$COMSKIP_INI" ]; then
        comskip --ini="$COMSKIP_INI" --output="$TEMP_DIR" --quiet -- "$WORKING_FILE" < /dev/null >> "$MAIN_LOG_LOCAL" 2>&1 || true
    else
        comskip --output="$TEMP_DIR" --quiet -- "$WORKING_FILE" < /dev/null >> "$MAIN_LOG_LOCAL" 2>&1 || true
    fi

    EDL_FILE=$(find "$TEMP_DIR" -name "*.edl" 2>/dev/null | head -n 1)
    EDL_ARG="${EDL_FILE:-none}"

    [ -n "$TEMP_REPAIRED" ] && rm -f "$TEMP_REPAIRED"

    # 4. cut_with_edl.py
    log_message "Schritt 4: FFmpeg-Recodierung..."

    SRT_ARG="none"
    METADATA_ARG="none"
    [ -f "$TEMP_DIR/$(basename "${REMOTE_BASE}.srt")" ] 2>/dev/null && SRT_ARG="$TEMP_DIR/$(basename "${REMOTE_BASE}.srt")"
    [ -f "$TEMP_DIR/$(basename "${REMOTE_BASE}.txt")" ] 2>/dev/null && METADATA_ARG="$TEMP_DIR/$(basename "${REMOTE_BASE}.txt")"
    [ -z "$METADATA_ARG" ] || [ "$METADATA_ARG" = "none" ] && [ -f "$TEMP_DIR/$(basename "${REMOTE_BASE}.xml")" ] 2>/dev/null && METADATA_ARG="$TEMP_DIR/$(basename "${REMOTE_BASE}.xml")"

    cp -f "$WORK_DIR/blacklist.txt" "$WORK_DIR/corrupted_files.blacklist" 2>/dev/null || true

    if python3 "$PYTHON_SCRIPT" "$WORKING_FILE" "$EDL_ARG" "$LOCAL_OUTPUT" "$SRT_ARG" "$METADATA_ARG" "$MAIN_LOG_LOCAL" < /dev/null; then
        PYTHON_EXIT=0
    else
        PYTHON_EXIT=$?
    fi

    if [ $PYTHON_EXIT -eq 0 ] && [ -f "$LOCAL_OUTPUT" ]; then
        # 5. rsync zum Ziel-Server (NUR HIER: Globaler Netzwerk-Lock)
        if ! acquire_global_lock; then
            log_message "  ✗ Kein Global-Lock zum Kopieren - Ergebnis bleibt lokal"
            ((FAILED++)) || true
        else
            log_message "Schritt 5: Kopiere auf Ziel-Server..."
            ssh_cmd "$TARGET_SSH_HOST" "mkdir -p $TARGET_REL_DIR" 2>/dev/null || true
            if rsync -avz -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
                "$LOCAL_OUTPUT" "$SSH_USER@$TARGET_SSH_HOST:$TARGET_REL_DIR/$TARGET_FILENAME" 2>/dev/null; then

                [ -n "$SRT_ARG" ] && [ "$SRT_ARG" != "none" ] && rsync -az -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
                    "$SRT_ARG" "$SSH_USER@$TARGET_SSH_HOST:$TARGET_REL_DIR/$FILE_BASE.srt" 2>/dev/null || true
                [ -n "$METADATA_ARG" ] && [ "$METADATA_ARG" != "none" ] && rsync -az -e "sshpass -p $SSH_PASS ssh -o StrictHostKeyChecking=no" \
                    "$METADATA_ARG" "$SSH_USER@$TARGET_SSH_HOST:$TARGET_REL_DIR/$FILE_BASE.${METADATA_ARG##*.}" 2>/dev/null || true

                log_message "  ✓ Erfolgreich verarbeitet"
                ((PROCESSED++)) || true
            else
                log_message "  ✗ rsync zum Ziel fehlgeschlagen"
                ((FAILED++)) || true
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

    rm -f "$LOCAL_INPUT" "$LOCAL_OUTPUT" "$TEMP_DIR"/*.edl "$TEMP_DIR"/*.srt "$TEMP_DIR"/*.txt "$TEMP_DIR"/*.xml" 2>/dev/null || true

done < "$FILE_LIST"

rm -f "$FILE_LIST"

# --- UMBENENNUNG (auf Ziel-Server) ---
log_message "Prüfe Umbenennung..."
ssh_cmd "$TARGET_SSH_HOST" "
  find $TARGET_REMOTE_PATH -type f -name '*__*.mkv' 2>/dev/null | while read -r f; do
    d=\$(dirname \"\$f\")
    b=\$(basename \"\$f\" .mkv)
    n=\$(echo \"\$b\" | sed 's/__.*//')
    [ -f \"\$d/\$n.mkv\" ] && continue
    [ \"\$b\" != \"\$n\" ] && mv \"\$f\" \"\$d/\$n.mkv\"
    for ext in txt xml srt; do
      [ -f \"\$d/\$b.\$ext\" ] && [ ! -f \"\$d/\$n.\$ext\" ] && mv \"\$d/\$b.\$ext\" \"\$d/\$n.\$ext\"
    done
  done
" 2>/dev/null || true

# --- LOG SYNC ---
append_log_to_remote

# --- CLEANUP ---
log_message "=========================================="
log_message "Ende: $(date)"
log_message "STATISTIK ($WORKER_ID): Erfolgreich: $PROCESSED | Übersprungen: $SKIPPED | Fehler: $FAILED"
log_message "=========================================="
append_log_to_remote

rm -rf "$WORK_DIR"

echo ""
echo "Fertig! Erfolgreich: $PROCESSED | Übersprungen: $SKIPPED | Fehler: $FAILED"

exit 0
