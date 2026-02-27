## Repo intent and high level flow

This repository automates commercial detection (Comskip) and performs lossless-ish cuts using FFmpeg. The orchestration is done by shell scripts in `src/`, while actual trimming and metadata handling is implemented in `src/cut_with_edl.py`.

Key flow (files to read):
- Discovery & orchestration: `src/auto_process.sh` and `src/auto_cut.sh`
- Retry failed: `src/retry_failed.sh` (parses process_summary.log for errors, re-runs Comskip/FFmpeg for those files)
- Detection config: `src/comskip.ini` (Comskip options)
- Cutting + metadata: `src/cut_with_edl.py` (ffmpeg filter_complex construction)

Why this structure:
- Shell scripts mount remote shares (via `sshfs`/`sshpass`) and perform file discovery, logging and per-file Comskip runs.
- Comskip produces EDL files. The Python script parses the EDL and builds an ffmpeg filter graph to concatenate keep-segments and attach subtitles/metadata.

## Environment & external dependencies (explicit)
- comskip (EDL generation) — configured via `src/comskip.ini`
- ffmpeg (video cutting/encoding)
- python3 (script runtime)
- sshfs and sshpass (used by `auto_process.sh` for mounting remote SMB/NFS paths)

If you need to run or debug locally, ensure those tools are installed and available on PATH.

## Developer workflows and quick examples

Run a single-file debug pipeline (dry run) to inspect a single file end-to-end:

1) Generate an EDL with comskip:
   comskip --ini=src/comskip.ini --output=/tmp/comskip_work --quiet -- /path/to/input.mp4

2) Run the Python cutter (example):
   python3 src/cut_with_edl.py /path/to/input.mp4 /tmp/comskip_work/input.edl /tmp/output.mkv /path/to/input.srt /path/to/input.txt /tmp/video.log

3) Check logs:
   - Global run summary: `/srv/data/Videos/process_summary.log` (used by `auto_process.sh`)
   - Per-video logs are created next to `TARGET_DIR` (see `auto_process.sh` variable `VIDEO_LOG`).

**Corrupted file handling:**
- `cut_with_edl.py` automatically attempts repair with `ffmpeg -err_detect ignore_err` if a file fails to process.
- If repair fails, the file is added to `/srv/data/Videos/corrupted_files.blacklist`.
- `retry_failed.sh` skips blacklisted files to avoid repeated processing attempts.
- Exit code 9 indicates a blacklisted file was encountered.

To run the full automated process, `auto_process.sh` expects a credentials file (see variables at top of the script):
- `CRED_FILE` contains `username=<user>` and `password=<pass>` lines. The script uses `sshpass` + `sshfs` to mount `REMOTE_PATH`.

## Project-specific conventions and patterns
- File discovery mirrors the source tree into the target: the scripts compute `REL_DIR` from the source mount and create identical structure under `TARGET_BASE`.
- Output files use `.mkv` (target choice for metadata/subtitle friendliness) even if the source is `.mp4`/`.ts`.
- Temporary work dir: `$TEMP_DIR` (commonly `/tmp/comskip_work`). Scripts clear `$TEMP_DIR` after each file.
- Subtitle (`.srt`) and text (`.txt`) attachments are optional. If present, `cut_with_edl.py` maps the SRT as `-map 1:0` and sets `language=ger`.
- EDL interpretation: `cut_with_edl.py` treats lines as `start end action` and considers `action == '0'` as the commercial segment to be removed; the script builds keep segments between those ranges.

## Things an AI agent should do first when contributing
1. Read `src/auto_process.sh` and `src/cut_with_edl.py` end-to-end to understand who owns each responsibility.
2. When editing the Python cutter, keep ffmpeg filter construction intact — tests or changes should validate the filter_complex string and a small end-to-end run with a short sample file.
3. Preserve the bash variable conventions and logging locations used by the scripts (`TEMP_DIR`, `TARGET_BASE`, `MAIN_LOG`, per-video `VIDEO_LOG`).

## Common maintenance tasks and examples for an AI assistant
- Add safe argument validation to `cut_with_edl.py` (check for missing args / file existence) and return non-zero exit codes on errors so the shell pipeline can log failures.
- When changing EDL parsing, include a small unit or integration check: run comskip on a tiny test file (or use a prepared `.edl`) and assert `filter_complex` contains `concat=n=` matching expected keep segments.
- If you modify `auto_process.sh`, keep the mount logic compatible with `sshfs` and respect the `CRED_FILE` format.

## Files to reference for examples
- `src/auto_process.sh` — remote mount, discovery, comskip invocation, logging
- `src/auto_cut.sh` — simple local discovery variant
- `src/retry_failed.sh` — parse MAIN_LOG for failed files (Speicherzugriffsfehler, ✗ Keine Ausgabe, ✗ Python Exit), re-run comskip + cut_with_edl.py; use `--dry-run` to list only
- `src/cut_with_edl.py` — EDL parsing and ffmpeg invocation (primary transformation logic)
- `src/comskip.ini` — detection flags (e.g., `output_edl=1`)

## Safety and non-goals
- Do not change the destination layout (relative path mirroring) without updating both shell scripts.
- Avoid adding large binary test files to the repo; use small fixtures or mock EDL files for tests.

If any section is unclear or you'd like me to add short example unit tests / a small README section showing the debug commands, tell me which part to expand.
