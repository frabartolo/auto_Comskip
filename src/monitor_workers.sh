#!/bin/bash
#
# Überwacht alle Worker, die an der Video-Verarbeitung arbeiten.
# Zeigt Status, Fortschritt und aktuelle Fehler an.
#
# Aufruf: ./monitor_workers.sh [--watch]
#   --watch  Kontinuierliche Überwachung (aktualisiert alle 10 Sekunden)
#

set -u

# --- KONFIGURATION ---
SOURCE_MOUNT="${SOURCE_MOUNT:-$HOME/mount/cold-lairs-videos}"
TARGET_MOUNT="${TARGET_MOUNT:-$HOME/mount/khanhiwara-videos}"
MAIN_LOG="$TARGET_MOUNT/process_summary.log"
LOCK_DIR="$SOURCE_MOUNT/.comskip_locks"

WATCH_MODE=0
if [ "${1:-}" = "--watch" ]; then
    WATCH_MODE=1
fi

# Farben für Ausgabe
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${CYAN}╔════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║           Video-Verarbeitung Monitoring Dashboard                  ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_separator() {
    echo -e "${CYAN}────────────────────────────────────────────────────────────────────${NC}"
}

# --- GESAMTFORTSCHRITT ---
calculate_progress() {
    if [ ! -f "$MAIN_LOG" ]; then
        echo "Log nicht gefunden"
        return
    fi
    
    # Gesamtzahl der Videos
    TOTAL_FILES=$(grep "Video-Dateien gefunden:" "$MAIN_LOG" | tail -1 | grep -oP '\d+' | head -1)
    
    if [ -z "$TOTAL_FILES" ] || [ "$TOTAL_FILES" -eq 0 ]; then
        echo "Keine Daten"
        return
    fi
    
    # Erfolgreich verarbeitete Videos (eindeutig nach Dateiname)
    PROCESSED=$(grep "✓ Video verarbeitet" "$MAIN_LOG" | wc -l)
    
    # Fehlgeschlagene Videos
    FAILED=$(grep -E "✗ Fehler \(Exit:|✗ Datei ist auf Blacklist|✗ Überspringe|✗ Keine Ausgabe" "$MAIN_LOG" | wc -l)
    
    # Fortschritt berechnen
    PROGRESS_PCT=$(( PROCESSED * 100 / TOTAL_FILES ))
    REMAINING=$(( TOTAL_FILES - PROCESSED - FAILED ))
    
    echo -e "${BLUE}Gesamtfortschritt:${NC}"
    echo -e "  Gesamt:        ${TOTAL_FILES} Videos"
    echo -e "  Verarbeitet:   ${GREEN}${PROCESSED}${NC} (${PROGRESS_PCT}%)"
    echo -e "  Fehlgeschlagen: ${RED}${FAILED}${NC}"
    echo -e "  Verbleibend:   ${YELLOW}${REMAINING}${NC}"
    echo ""
    
    # Fortschrittsbalken
    BAR_WIDTH=50
    FILLED=$(( PROGRESS_PCT * BAR_WIDTH / 100 ))
    EMPTY=$(( BAR_WIDTH - FILLED ))
    
    printf "  ["
    printf "${GREEN}%${FILLED}s${NC}" | tr ' ' '█'
    printf "%${EMPTY}s" | tr ' ' '░'
    printf "] ${PROGRESS_PCT}%%\n"
    echo ""
}

# --- AKTIVE WORKER ---
list_active_workers() {
    if [ ! -d "$LOCK_DIR" ]; then
        echo -e "${YELLOW}Keine aktiven Worker (Lock-Verzeichnis nicht gefunden)${NC}"
        return
    fi
    
    ACTIVE_LOCKS=$(find "$LOCK_DIR" -name "*.lck" -type d 2>/dev/null | wc -l)
    
    if [ "$ACTIVE_LOCKS" -eq 0 ]; then
        echo -e "${YELLOW}Keine aktiven Worker${NC}"
        return
    fi
    
    echo -e "${BLUE}Aktive Worker: ${GREEN}${ACTIVE_LOCKS}${NC}"
    echo ""
    
    # Sammel Worker-Daten
    declare -A WORKER_FILES
    declare -A WORKER_TIMES
    
    find "$LOCK_DIR" -name "*.lck" -type d 2>/dev/null | while IFS= read -r lock; do
        if [ -f "$lock/info" ]; then
            LOCK_INFO=$(cat "$lock/info" 2>/dev/null || echo "unknown:0")
            WORKER_NAME=$(echo "$LOCK_INFO" | cut -d: -f1)
            LOCK_TIME=$(echo "$LOCK_INFO" | cut -d: -f2)
            NOW=$(date +%s)
            AGE_MINUTES=$(( (NOW - LOCK_TIME) / 60 ))
            
            # Extrahiere Dateinamen aus Lock-Pfad (Hash zurück zur Datei ist schwierig)
            LOCK_BASENAME=$(basename "$lock" .lck)
            
            # Finde letzte Verarbeitung dieses Workers im Log
            CURRENT_FILE=$(grep -F "$WORKER_NAME" "$MAIN_LOG" | grep "Verarbeite:" | tail -1 | sed 's/.*Verarbeite: //' || echo "?")
            
            echo -e "  ${CYAN}${WORKER_NAME}${NC}"
            echo -e "    Datei:      ${CURRENT_FILE}"
            echo -e "    Seit:       ${AGE_MINUTES} Minuten"
            
            # Warnung bei sehr langen Locks
            if [ "$AGE_MINUTES" -gt 120 ]; then
                echo -e "    ${RED}⚠ Lock sehr alt! Möglicherweise hängend.${NC}"
            fi
            echo ""
        fi
    done
}

# --- AKTUELLE FEHLER ---
show_recent_errors() {
    if [ ! -f "$MAIN_LOG" ]; then
        return
    fi
    
    # Letzte 5 Fehler
    ERRORS=$(grep -E "✗ Fehler \(Exit:|Segfault|✗ Datei ist auf Blacklist" "$MAIN_LOG" | tail -5)
    
    if [ -z "$ERRORS" ]; then
        echo -e "${GREEN}Keine aktuellen Fehler${NC}"
        return
    fi
    
    echo -e "${RED}Letzte Fehler:${NC}"
    echo ""
    
    echo "$ERRORS" | while IFS= read -r line; do
        # Extrahiere Dateinamen aus vorheriger Zeile
        LINE_NUM=$(grep -n "$line" "$MAIN_LOG" | tail -1 | cut -d: -f1)
        if [ -n "$LINE_NUM" ]; then
            FILE_LINE=$(sed -n "$((LINE_NUM - 1))p" "$MAIN_LOG" | grep "Verarbeite:" || echo "")
            if [ -n "$FILE_LINE" ]; then
                FILENAME=$(echo "$FILE_LINE" | sed 's/.*Verarbeite: //')
                echo -e "  ${YELLOW}${FILENAME}${NC}"
            fi
        fi
        echo -e "    ${RED}→${NC} $(echo "$line" | sed 's/.*\] //')"
    done
    echo ""
}

# --- WORKER-STATISTIKEN ---
show_worker_stats() {
    if [ ! -f "$MAIN_LOG" ]; then
        return
    fi
    
    echo -e "${BLUE}Worker-Statistiken (aus STATISTIK-Einträgen):${NC}"
    echo ""
    
    # Finde alle STATISTIK-Blöcke
    grep -A 4 "STATISTIK (" "$MAIN_LOG" | grep -E "STATISTIK \(|Erfolgreich:|Übersprungen:|Fehlgeschlagen:" | \
    while IFS= read -r line; do
        if [[ "$line" == *"STATISTIK ("* ]]; then
            WORKER=$(echo "$line" | sed 's/.*STATISTIK (\(.*\)):/\1/')
            echo -e "  ${CYAN}${WORKER}${NC}"
        elif [[ "$line" == *"Erfolgreich:"* ]]; then
            SUCCESS=$(echo "$line" | grep -oP '\d+')
            echo -n -e "    Erfolgreich: ${GREEN}${SUCCESS}${NC}"
        elif [[ "$line" == *"Übersprungen:"* ]]; then
            SKIPPED=$(echo "$line" | grep -oP '\d+')
            echo -n -e " | Übersprungen: ${YELLOW}${SKIPPED}${NC}"
        elif [[ "$line" == *"Fehlgeschlagen:"* ]]; then
            FAILED=$(echo "$line" | grep -oP '\d+')
            echo -e " | Fehlgeschlagen: ${RED}${FAILED}${NC}"
        fi
    done | tail -20
    echo ""
}

# --- BLACKLIST-STATUS ---
show_blacklist() {
    BLACKLIST_FILE="$TARGET_MOUNT/corrupted_files.blacklist"
    
    if [ ! -f "$BLACKLIST_FILE" ]; then
        echo -e "${GREEN}Blacklist: leer${NC}"
        return
    fi
    
    BLACKLIST_COUNT=$(wc -l < "$BLACKLIST_FILE")
    
    if [ "$BLACKLIST_COUNT" -eq 0 ]; then
        echo -e "${GREEN}Blacklist: leer${NC}"
        return
    fi
    
    echo -e "${RED}Blacklist: ${BLACKLIST_COUNT} Dateien${NC}"
    echo ""
    echo "Letzte 5 Einträge:"
    tail -5 "$BLACKLIST_FILE" | while IFS= read -r file; do
        echo -e "  ${RED}✗${NC} $file"
    done
    echo ""
}

# --- HAUPTPROGRAMM ---
display_dashboard() {
    clear
    print_header
    
    calculate_progress
    print_separator
    
    list_active_workers
    print_separator
    
    show_recent_errors
    print_separator
    
    show_worker_stats
    print_separator
    
    show_blacklist
    print_separator
    
    echo -e "${CYAN}Letzte Aktualisierung: $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    
    if [ "$WATCH_MODE" -eq 1 ]; then
        echo -e "${YELLOW}Drücke Ctrl+C zum Beenden${NC}"
    fi
}

# --- WATCH-MODUS ---
if [ "$WATCH_MODE" -eq 1 ]; then
    while true; do
        display_dashboard
        sleep 10
    done
else
    display_dashboard
fi

exit 0
