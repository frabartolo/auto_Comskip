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

### Main Processing

```bash
cd src
./auto_process.sh
```

Processes all video files from a remote mount, detects commercials, cuts them out, and converts to MKV.

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

## Configuration

Edit paths in `src/auto_process.sh` and `src/retry_failed.sh`:
- `MOUNT_DIR` - Source mount point
- `TARGET_BASE` - Destination directory
- `MAIN_LOG` - Log file location
- `COMSKIP_INI` - Comskip configuration

## Requirements

- `comskip` - Commercial detection
- `ffmpeg` / `ffprobe` - Video processing
- `python3` - Processing script
- `sshfs` / `sshpass` - For remote mounts (auto_process.sh only)
