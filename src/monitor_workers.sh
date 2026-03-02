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
    
    # Gesamtzahl der Videos (letzte Zeile mit dieser Info)
    TOTAL_FILES=$(grep "Video-Dateien gefunden:" "$MAIN_LOG" | tail -1 | grep -oE '[0-9]+' | tail -1)
    
    if [ -z "$TOTAL_FILES" ] || [ "$TOTAL_FILES" -eq 0 ]; then
        echo "Keine Daten"
        return
    fi
    
    # Erfolgreich verarbeitete Videos
    PROCESSED=$(grep "✓ Video verarbeitet" "$MAIN_LOG" 2>/dev/null | wc -l)
    PROCESSED=${PROCESSED:-0}
    
    # Fehlgeschlagene Videos (nur echte Fehler, nicht "Überspringe")
    FAILED_EXIT=$(grep "✗ Fehler (Exit:" "$MAIN_LOG" 2>/dev/null | wc -l)
    FAILED_EXIT=${FAILED_EXIT:-0}
    FAILED_BLACKLIST=$(grep "✗ Datei ist auf Blacklist" "$MAIN_LOG" 2>/dev/null | wc -l)
    FAILED_BLACKLIST=${FAILED_BLACKLIST:-0}
    FAILED=$(( FAILED_EXIT + FAILED_BLACKLIST ))
    
    # Übersprungene Videos (bereits vorhanden oder zu groß)
    SKIPPED=$(grep -E "(Überspringe \(bereits vorhanden|Überspringe \(in Bearbeitung|✗ Überspringe)" "$MAIN_LOG" 2>/dev/null | wc -l)
    SKIPPED=${SKIPPED:-0}
    
    # Fortschritt berechnen
    PROGRESS_PCT=$(( PROCESSED * 100 / TOTAL_FILES ))
    REMAINING=$(( TOTAL_FILES - PROCESSED - FAILED - SKIPPED ))
    
    # Verhindere negative Zahlen
    if [ "$REMAINING" -lt 0 ]; then
        REMAINING=0
    fi
    
    echo -e "${BLUE}Gesamtfortschritt:${NC}"
    echo -e "  Gesamt:         ${TOTAL_FILES} Videos"
    echo -e "  Verarbeitet:    ${GREEN}${PROCESSED}${NC} (${PROGRESS_PCT}%)"
    echo -e "  Übersprungen:   ${YELLOW}${SKIPPED}${NC}"
    echo -e "  Fehlgeschlagen: ${RED}${FAILED}${NC}"
    echo -e "  Verbleibend:    ${CYAN}${REMAINING}${NC}"
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
    find "$LOCK_DIR" -name "*.lck" -type d 2>/dev/null | while IFS= read -r lock; do
        if [ -f "$lock/info" ]; then
            LOCK_INFO=$(cat "$lock/info" 2>/dev/null || echo "unknown:0")
            WORKER_NAME=$(echo "$LOCK_INFO" | cut -d: -f1)
            LOCK_TIME=$(echo "$LOCK_INFO" | cut -d: -f2)
            NOW=$(date +%s)
            AGE_MINUTES=$(( (NOW - LOCK_TIME) / 60 ))
            
            # Finde letzte "Verarbeite:"-Zeile dieses Workers im Log
            CURRENT_FILE=$(grep "$WORKER_NAME" "$MAIN_LOG" 2>/dev/null | grep "Verarbeite:" | tail -1 | sed -E 's/.*Verarbeite: //' || echo "unbekannt")
            
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
    
    # Letzte 5 Fehler mit Kontext
    echo -e "${RED}Letzte Fehler:${NC}"
    echo ""
    
    # Sammel Fehler mit Dateinamen
    grep -B 1 -E "(✗ Fehler \(Exit:|Segfault|✗ Datei ist auf Blacklist)" "$MAIN_LOG" 2>/dev/null | \
    grep -E "(Verarbeite:|✗)" | \
    tail -10 | \
    while IFS= read -r line; do
        if [[ "$line" == *"Verarbeite:"* ]]; then
            # Extrahiere Dateinamen (alles nach "Verarbeite: ")
            FILENAME=$(echo "$line" | sed -E 's/.*Verarbeite: //')
            echo -e "  ${YELLOW}${FILENAME}${NC}"
        elif [[ "$line" == *"✗"* ]]; then
            # Zeige Fehlermeldung
            ERROR=$(echo "$line" | sed -E 's/.*\] //')
            echo -e "    ${RED}→${NC} ${ERROR}"
        fi
    done
    
    # Prüfe ob Fehler gefunden wurden
    ERROR_COUNT=$(grep -cE "(✗ Fehler \(Exit:|Segfault|✗ Datei ist auf Blacklist)" "$MAIN_LOG" 2>/dev/null || echo "0")
    if [ "$ERROR_COUNT" -eq 0 ]; then
        echo -e "${GREEN}Keine Fehler im Log${NC}"
    fi
    echo ""
}

# --- WORKER-STATISTIKEN ---
show_worker_stats() {
    if [ ! -f "$MAIN_LOG" ]; then
        return
    fi
    
    echo -e "${BLUE}Worker-Statistiken (aus STATISTIK-Einträgen):${NC}"
    echo ""
    
    # Finde alle STATISTIK-Blöcke und parse sie mit einfacherem awk
    grep -A 3 "STATISTIK (" "$MAIN_LOG" 2>/dev/null | \
    awk '
        /STATISTIK \(/ {
            # Extrahiere Worker-Name zwischen ( und )
            sub(/.*STATISTIK \(/, "")
            sub(/\):.*/, "")
            worker = $0
        }
        /Erfolgreich:/ {
            # Extrahiere letzte Zahl
            for(i=NF; i>=1; i--) {
                if($i ~ /^[0-9]+$/) {
                    success = $i
                    break
                }
            }
        }
        /Übersprungen:/ {
            for(i=NF; i>=1; i--) {
                if($i ~ /^[0-9]+$/) {
                    skipped = $i
                    break
                }
            }
        }
        /Fehlgeschlagen:/ {
            for(i=NF; i>=1; i--) {
                if($i ~ /^[0-9]+$/) {
                    failed = $i
                    break
                }
            }
            if (worker != "") {
                printf "  \033[0;36m%s\033[0m\n", worker
                printf "    Erfolgreich: \033[0;32m%s\033[0m | Übersprungen: \033[1;33m%s\033[0m | Fehlgeschlagen: \033[0;31m%s\033[0m\n", success, skipped, failed
                worker = ""
            }
        }
    ' | tail -20
    
    # Fallback falls keine Statistiken gefunden
    STAT_COUNT=$(grep -c "STATISTIK (" "$MAIN_LOG" 2>/dev/null || echo "0")
    if [ "$STAT_COUNT" -eq 0 ]; then
        echo -e "  ${YELLOW}Noch keine Statistiken verfügbar${NC}"
    fi
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
