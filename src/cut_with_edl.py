
#!/usr/bin/env python3
"""cut_with_edl.py

Refactored to provide small, testable helpers for EDL -> keep-segments -> filter_complex.
Provides argument validation and clear exit codes so shell scripts can react.
"""
import os
import subprocess
import sys
from typing import List, Optional, Tuple


def parse_edl_lines(lines: List[str]) -> List[Tuple[float, float, str]]:
    """Parse lines from an EDL file into (start, end, action).

    Ignores empty/comment lines and tolerates extra whitespace.
    """
    cuts = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            start = float(parts[0])
            end = float(parts[1])
            action = parts[2]
            cuts.append((start, end, action))
        except ValueError:
            # skip malformed lines
            continue
    return cuts


def keep_segments_from_cuts(cuts: List[Tuple[float, float, str]]) -> List[Tuple[float, Optional[float]]]:
    """Turn cuts (from parse_edl_lines) into keep segments (start, end) where end can be None (till EOF)."""
    keep_segments: List[Tuple[float, Optional[float]]] = []
    last_end = 0.0
    for start, end, action in cuts:
        if action == '0':
            if start > last_end:
                keep_segments.append((last_end, start))
            last_end = end
    keep_segments.append((last_end, None))
    return keep_segments


def build_filter_complex(keep_segments: List[Tuple[float, Optional[float]]]) -> str:
    """Construct the ffmpeg filter_complex string for the given keep_segments.

    Returns a string like: [0:v]trim=..., setpts=...; [0:a]atrim=...,asetpts=...; [v0][a0][v1][a1]concat=n=2:..."""
    v_filters = []
    a_filters = []
    for i, (s, e) in enumerate(keep_segments):
        end_str = f":end={e}" if e is not None else ""
        v_filters.append(f"[0:v]trim=start={s}{end_str},setpts=PTS-STARTPTS[v{i}]")
        a_filters.append(f"[0:a]atrim=start={s}{end_str},asetpts=PTS-STARTPTS[a{i}]")

    concat_inputs = "".join([f"[v{i}][a{i}]" for i in range(len(keep_segments))])
    filter_parts = []
    if v_filters:
        filter_parts.append("; ".join(v_filters) + "; ")
    if a_filters:
        filter_parts.append("; ".join(a_filters) + "; ")
    filter_parts.append(f"{concat_inputs}concat=n={len(keep_segments)}:v=1:a=1[outv][outa]")
    return "".join(filter_parts)


def cut_video(input_file: str,
              edl_file: str,
              output_file: str,
              srt_file: Optional[str] = None,
              txt_file: Optional[str] = None,
              log_file: Optional[str] = None,
              dry_run: bool = False) -> int:
    """Main entry to perform cutting. Returns exit code: 0 success, non-zero failure codes.

    dry_run=True will not call ffmpeg; instead returns 0 after printing planned filter_complex.
    """
    # Validate mandatory files
    if not os.path.exists(input_file):
        print(f"ERROR: input file not found: {input_file}", file=sys.stderr)
        return 3
    if not os.path.exists(edl_file):
        print(f"ERROR: EDL file not found: {edl_file}", file=sys.stderr)
        return 4

    with open(edl_file, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    cuts = parse_edl_lines(lines)
    keep_segments = keep_segments_from_cuts(cuts)
    if not keep_segments:
        print("ERROR: No keep segments computed from EDL", file=sys.stderr)
        return 5

    filter_complex = build_filter_complex(keep_segments)

    # If dry-run, print the filter and exit successfully
    if dry_run:
        print(filter_complex)
        return 0

    cmd = ["ffmpeg", "-i", input_file]

    if srt_file and os.path.exists(srt_file):
        cmd.extend(["-i", srt_file])
        has_srt = True
    else:
        has_srt = False

    cmd.extend(["-filter_complex", filter_complex, "-map", "[outv]", "-map", "[outa]"])

    if has_srt:
        cmd.extend(["-map", "1:0", "-c:s", "srt", "-metadata:s:s:0", "language=ger"])

    if txt_file and os.path.exists(txt_file):
        try:
            with open(txt_file, 'r', encoding='utf-8', errors='ignore') as f:
                desc = f.read().strip()
                if desc:
                    cmd.extend(["-metadata", f"comment={desc}", "-metadata", f"description={desc}"])
        except Exception:
            pass

    cmd.extend(["-c:v", "libx264", "-crf", "21", "-preset", "faster", "-c:a", "aac", "-b:a", "192k", "-y", output_file])

    # Run ffmpeg and optionally log
    try:
        if log_file:
            with open(log_file, "a", encoding='utf-8', errors='ignore') as f_log:
                f_log.write(f"\n=== FFmpeg Processing: {os.path.basename(input_file)} ===\n")
                f_log.write(f"Time: {subprocess.run(['date'], capture_output=True, text=True, shell=True).stdout.strip()}\n")
                f_log.write(f"Input: {input_file}\nOutput: {output_file}\n")
                f_log.write(f"Keep segments: {len(keep_segments)}\n\n")
                rc = subprocess.run(cmd, stdout=f_log, stderr=f_log).returncode
                f_log.write(f"\n=== FFmpeg Exit Code: {rc} ===\n\n")
        else:
            rc = subprocess.run(cmd).returncode
        if rc != 0:
            print(f"FFmpeg failed with exit code {rc}", file=sys.stderr)
            return 6
        return 0
    except FileNotFoundError:
        print("ERROR: ffmpeg not found on PATH", file=sys.stderr)
        return 7


if __name__ == "__main__":
    # Aufruf: script.py video.edl output [srt] [txt] [log]
    if len(sys.argv) < 4:
        print("Usage: cut_with_edl.py <input_video> <edl_file> <output_file> [srt_file] [txt_file] [log_file]", file=sys.stderr)
        sys.exit(2)

    input_file = sys.argv[1]
    edl_file = sys.argv[2]
    output_file = sys.argv[3]
    srt_file = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] != 'none' else None
    txt_file = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] != 'none' else None
    log_file = sys.argv[6] if len(sys.argv) > 6 and sys.argv[6] != 'none' else None

    exit_code = cut_video(input_file, edl_file, output_file, srt_file, txt_file, log_file)
    sys.exit(exit_code)
