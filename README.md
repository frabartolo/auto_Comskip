# auto_Comskip
automated removal of commercials from videos

## Features

- **Automatic commercial detection** using Comskip
- **Robust error handling** for Comskip crashes (Segmentation Faults)
- **Automatic file repair** before Comskip processing for problematic files
- **Lossless-ish cutting** with FFmpeg (re-encode with high quality settings)
- **Subtitle and metadata** preservation (.srt, .txt, .xml)
- **Automatic repair** of corrupted video files during FFmpeg processing
- **Blacklist management** for permanently broken files
- **Retry mechanism** for failed processing attempts
- **Multi-machine coordination** with distributed lock system

## Usage

### Main Processing (NEU: rsync-Modus, ohne sshfs)

```bash
cd src
./auto_process_rsync.sh
```

**Empfohlen bei mehreren Rechnern** – Rechenarbeit verteilt, Netzwerkzugriff serialisiert:
- **Nur Netzwerk serialisiert**: rsync (Lesen/Schreiben) läuft immer nur für einen Rechner; verhindert e1000e-Hang (Proxmox)
- **Rechenleistung parallel**: ffprobe, Comskip, FFmpeg laufen lokal auf jedem Rechner gleichzeitig
- Ablauf: Datei per rsync holen → Lock freigeben → lokal verarbeiten → Lock holen → Ergebnis per rsync ablegen

### Main Processing (Legacy: sshfs-Mount)

```bash
cd src
./auto_process.sh
```

Processes all video files from a remote mount, detects commercials, cuts them out, and converts to MKV.

### Monitor Processing

```bash
cd src
./monitor_workers.sh           # Single snapshot
./monitor_workers.sh --watch   # Continuous monitoring (updates every 10s)
```

Shows:
- Overall progress (% processed)
- Active workers and their current files
- Recent errors
- Worker statistics
- Blacklist status

### Retry Failed Files

```bash
cd src
./retry_failed.sh           # Re-process all failed files
./retry_failed.sh --dry-run # Preview which files would be retried
```

Parses the log file for failures and re-runs the pipeline on those files.

### Corrupted File Handling

When a file fails to process:
1. **Pre-check**: FFprobe validates file before Comskip runs
2. **Optional repair**: If ffprobe fails, file is repaired with `ffmpeg -err_detect ignore_err -c copy` before Comskip
3. **Comskip crash detection**: Exit codes 139/134 (Segmentation Fault) are logged, processing continues without EDL
4. **FFmpeg attempt**: If FFmpeg fails, automatic repair is attempted
5. **Blacklist**: If all attempts fail, file is added to `corrupted_files.blacklist`
6. **Skip on retry**: Blacklisted files are automatically skipped in future runs

To manually manage the blacklist:
```bash
# View blacklisted files
cat /srv/data/Videos/corrupted_files.blacklist

# Remove a file from blacklist
sed -i '/filename.ts/d' /srv/data/Videos/corrupted_files.blacklist
```

## Troubleshooting (auto_process_rsync.sh)

**„Keine SSH-Verbindung zu cold-lairs“** – häufige Ursachen:
- **Hostname-Auflösung**: Kurzname `cold-lairs` funktioniert oft nur im lokalen Netz mit mDNS. Prüfen: `ping cold-lairs` bzw. `ping cold-lairs.local`
- **Lösung**: IP oder FQDN verwenden, z.B.  
  `SOURCE_SSH_HOST=192.168.1.50 TARGET_SSH_HOST=192.168.1.51 ./auto_process_rsync.sh`
- Credentials in `~/.smbcredentials` mit `username=` und `password=`
- Das Skript gibt nun die echte SSH-Fehlermeldung aus – hilft bei der Diagnose

**rsync zum Ziel fehlgeschlagen** – Skript bricht ab, damit die Platten nicht volllaufen:
- Fertige Dateien werden unter `~/comskip_failed_uploads/` (oder `FAILED_UPLOAD_DIR`) gesichert
- Die rsync-Fehlermeldung wird ins Log und auf die Konsole geschrieben
- Nach Behebung: gesicherte Dateien manuell nachziehen, Skript neu starten

**"connection unexpectedly closed (0 bytes received)"** – mögliche Ursachen:
- **`ssh -n` in der rsync -e Option:** `-n` leitet stdin um, das bricht das rsync-Protokoll. Das Skript verwendet kein `-n` bei rsync.
- **Ausgabe in Remote-`.bashrc`:** Falls `[[ $- != *i* ]] && return` fehlt, kann Ausgabe das Protokoll stören.
- Manueller Test: `rsync -avz -e "ssh -o StrictHostKeyChecking=no" datei.mkv user@khanhiwara:/srv/data/Videos/...` (ohne `-n`!)

## Configuration

Edit paths in `src/auto_process.sh` and `src/retry_failed.sh`:
- `MOUNT_DIR` - Source mount point
- `TARGET_BASE` - Destination directory
- `MAIN_LOG` - Log file location
- `COMSKIP_INI` - Comskip configuration

Für `auto_process_rsync.sh`: Hosts und Pfade im Skript-Kopf; alternativ per Umgebungsvariable:
- `SOURCE_SSH_HOST`, `TARGET_SSH_HOST` – Hostname, FQDN oder IP
- `SOURCE_MOUNT_DIR`, `TARGET_MOUNT_DIR` – Wenn beide existieren und beschreibbar: Log, Blacklist und Locks liegen dort (wie bei auto_process.sh). Alle Worker teilen sich die Dateien. Default: `~/mount/cold-lairs-videos`, `~/mount/khanhiwara-videos`
- `FAILED_UPLOAD_DIR` – Verzeichnis für bei rsync-Fehler gesicherte Dateien (Default: `~/comskip_failed_uploads`)

## Requirements

- `comskip` - Commercial detection
- `ffmpeg` / `ffprobe` - Video processing
- `python3` - Processing script
- `sshpass` - Für SSH-Authentifizierung (beide Scripts)
- `rsync` - Für auto_process_rsync.sh (ersetzt sshfs)
- `sshfs` - Nur für auto_process.sh (Legacy-Modus)
