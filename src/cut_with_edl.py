#!/usr/bin/env python3
"""Commercial removal automation using Comskip EDL files and FFmpeg.

OPTIMIZED VERSION with memory-efficient concat demuxer for large files.
"""

import os
import subprocess
import sys
import tempfile
import shutil
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple

# Blacklist path for permanently corrupted files
BLACKLIST_FILE = "/srv/data/Videos/corrupted_files.blacklist"


def edl_has_no_commercials(edl_file: str) -> bool:
    """Check if an EDL file contains no commercial segments."""
    try:
        with open(edl_file, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        action = parts[2]
                        if action == "0":
                            return False
                    except (ValueError, IndexError):
                        continue
        return True
    except (OSError, IOError):
        return True


def is_blacklisted(input_file: str) -> bool:
    """Check if file is in the corruption blacklist."""
    if not os.path.exists(BLACKLIST_FILE):
        return False
    basename = os.path.basename(input_file)
    try:
        with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip() == basename:
                    return True
    except (OSError, IOError):
        pass
    return False


def add_to_blacklist(input_file: str, log_file: Optional[str] = None) -> None:
    """Add a corrupted file to the blacklist."""
    basename = os.path.basename(input_file)
    try:
        os.makedirs(os.path.dirname(BLACKLIST_FILE), exist_ok=True)
        with open(BLACKLIST_FILE, "a", encoding="utf-8") as f:
            f.write(f"{basename}\n")
        msg = f"Added to corruption blacklist: {basename}"
        print(msg, file=sys.stderr)
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                f_log.write(f"[BLACKLIST] {msg}\n")
    except (OSError, IOError) as e:
        print(f"Warning: Could not write blacklist: {e}", file=sys.stderr)


def repair_corrupted_file(input_file: str, log_file: Optional[str] = None) -> Optional[str]:
    """Attempt to repair a corrupted video file.
    
    Returns path to repaired file if successful, None otherwise.
    """
    temp_repaired = tempfile.mktemp(suffix=".ts", prefix="repaired_")
    
    if log_file:
        with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
            f.write(f"\n[REPAIR] Attempting to repair corrupted file: {os.path.basename(input_file)}\n")
    
    # Try repair with error tolerance
    cmd = [
        "ffmpeg", "-nostdin", "-hide_banner",
        "-err_detect", "ignore_err",
        "-i", input_file,
        "-c", "copy",
        "-y", temp_repaired
    ]
    
    try:
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write("[REPAIR] Running: ffmpeg -err_detect ignore_err -i <file> -c copy\n")
                rc = subprocess.run(cmd, stdout=f, stderr=f, check=False).returncode
        else:
            rc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode
        
        if rc == 0 and os.path.exists(temp_repaired) and os.path.getsize(temp_repaired) > 1024:
            # Verify repaired file works
            verify_cmd = ["ffprobe", "-v", "error", temp_repaired]
            verify_rc = subprocess.run(verify_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False).returncode
            
            if verify_rc == 0:
                if log_file:
                    with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                        f.write("[REPAIR] ✓ Repair successful, using repaired file\n")
                return temp_repaired
        
        # Cleanup failed repair
        if os.path.exists(temp_repaired):
            os.remove(temp_repaired)
        
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"[REPAIR] ✗ Repair failed (rc={rc}), file is permanently corrupted\n")
        
        return None
        
    except Exception as e:
        if os.path.exists(temp_repaired):
            os.remove(temp_repaired)
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"[REPAIR] ✗ Exception during repair: {e}\n")
        return None


def parse_edl_lines(lines: List[str]) -> List[Tuple[float, float, str]]:
    """Parse EDL file lines into structured cut information."""
    cuts = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
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
            continue
    return cuts


def keep_segments_from_cuts(
    cuts: List[Tuple[float, float, str]],
) -> List[Tuple[float, Optional[float]]]:
    """Convert commercial cut points into segments to keep."""
    keep_segments: List[Tuple[float, Optional[float]]] = []
    last_end = 0.0
    for start, end, action in cuts:
        if action == "0":
            if start > last_end:
                keep_segments.append((last_end, start))
            last_end = end
    keep_segments.append((last_end, None))
    return keep_segments


def parse_txt_metadata(file_path: str) -> Dict[str, str]:
    """Parse metadata from a text sidecar."""
    meta: Dict[str, str] = {}
    if not os.path.exists(file_path):
        return meta

    description_lines: List[str] = []
    in_description = False
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as fh:
            for raw_line in fh:
                line = raw_line.rstrip("\n")
                if not in_description:
                    if ":" in line:
                        key_part, value_part = line.split(":", 1)
                        key = key_part.strip().lower()
                        value = value_part.strip()
                        if key == "description":
                            in_description = True
                            if value:
                                description_lines.append(value)
                            continue
                        if key == "channel":
                            meta["channel"] = value
                            continue
                        if key == "date":
                            meta["date"] = value
                            continue
                        if key == "recording":
                            meta["recording"] = value
                            continue
                    continue
                if in_description:
                    description_lines.append(line)
        if description_lines:
            meta["description"] = "\n".join(description_lines).strip()
    except (OSError, IOError):
        return {}
    return meta


def parse_xml_metadata(file_path: str) -> Dict[str, str]:
    """Parse metadata from an XML sidecar (ArchiveTableArchive format)."""
    meta: Dict[str, str] = {}
    if not os.path.exists(file_path):
        return meta

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        title_elem = root.find("ArvTitle")
        if title_elem is not None and title_elem.text:
            meta["recording"] = title_elem.text.strip()

        channel_elem = root.find("ArvProgLogo")
        if channel_elem is not None and channel_elem.text:
            meta["channel"] = channel_elem.text.strip()

        description_parts: List[str] = []

        short_elem = root.find("ArvShortInfo")
        if short_elem is not None and short_elem.text:
            description_parts.append(short_elem.text.strip())

        long_elem = root.find("ArvLongInfo")
        if long_elem is not None and long_elem.text:
            description_parts.append(long_elem.text.strip())

        if description_parts:
            meta["description"] = "\n\n".join(description_parts)

    except (ET.ParseError, OSError, IOError):
        return {}

    return meta


def process_metadata(file_path: Optional[str]) -> Dict[str, str]:
    """Dispatch metadata parsing based on sidecar extension."""
    if not file_path:
        return {}
    if file_path.lower().endswith(".xml"):
        return parse_xml_metadata(file_path)
    if file_path.lower().endswith(".txt"):
        return parse_txt_metadata(file_path)
    return {}


def build_metadata_flags(meta: Dict[str, str]) -> List[str]:
    """Build ffmpeg -metadata flags from parsed metadata."""
    if not meta:
        return []

    comment_parts: List[str] = []
    title = meta.get("recording")
    description = meta.get("description")

    if meta.get("channel"):
        comment_parts.append(f"Channel: {meta['channel']}")
    if meta.get("date"):
        comment_parts.append(f"Date: {meta['date']}")
    if title:
        comment_parts.append(title)
    if description:
        comment_parts.append(description)

    comment_text = "\n\n".join([part for part in comment_parts if part])

    flags: List[str] = []
    if title:
        flags.extend(["-metadata", f"title={title}"])
    if description:
        flags.extend(["-metadata", f"description={description}"])
    if comment_text:
        flags.extend(["-metadata", f"comment={comment_text}"])

    return flags


def convert_without_cuts(
    input_file: str,
    output_file: str,
    srt_file: Optional[str] = None,
    txt_file: Optional[str] = None,
    log_file: Optional[str] = None,
) -> int:
    """Convert video to MKV format without cutting commercials.
    
    Includes automatic repair attempt for corrupted files.
    """
    if not os.path.exists(input_file):
        print(f"ERROR: input file not found: {input_file}", file=sys.stderr)
        return 3
    
    # Check blacklist first
    if is_blacklisted(input_file):
        msg = f"File is blacklisted (permanently corrupted): {os.path.basename(input_file)}"
        print(msg, file=sys.stderr)
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"[BLACKLIST] {msg}\n")
        return 9  # New exit code for blacklisted files
    
    # First attempt with error tolerance
    working_file = input_file
    repaired_temp_file = None
    
    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error"]
    cmd.extend(["-err_detect", "ignore_err"])  # Try to ignore errors first
    cmd.extend(["-i", working_file])

    has_srt = False
    if srt_file and os.path.exists(srt_file):
        cmd.extend(["-i", srt_file])
        has_srt = True

    cmd.extend(["-map", "0:v", "-map", "0:a"])

    if has_srt:
        cmd.extend(["-map", "1:0", "-c:s", "srt", "-metadata:s:s:0", "language=ger"])

    if txt_file and os.path.exists(txt_file):
        meta = process_metadata(txt_file)
        cmd.extend(build_metadata_flags(meta))

    cmd.extend(
        [
            "-c:v",
            "libx264",
            "-crf",
            "21",
            "-preset",
            "faster",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-y",
            output_file,
        ]
    )

    try:
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                f_log.write(
                    f"\n=== FFmpeg Conversion (No Commercials): "
                    f"{os.path.basename(input_file)} ===\n"
                )
                rc = subprocess.run(
                    cmd, stdout=f_log, stderr=f_log, check=False
                ).returncode
                f_log.write(f"\n=== FFmpeg Exit Code: {rc} ===\n")
        else:
            rc = subprocess.run(cmd, check=False).returncode

        # If failed, attempt repair
        if rc != 0:
            if log_file:
                with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                    f_log.write(f"[INFO] First attempt failed (rc={rc}), attempting repair...\n")
            
            repaired_temp_file = repair_corrupted_file(input_file, log_file)
            
            if repaired_temp_file:
                # Retry with repaired file
                working_file = repaired_temp_file
                cmd[cmd.index(input_file)] = repaired_temp_file
                
                if log_file:
                    with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                        f_log.write("\n=== Retry with Repaired File ===\n")
                        rc = subprocess.run(
                            cmd, stdout=f_log, stderr=f_log, check=False
                        ).returncode
                        f_log.write(f"\n=== FFmpeg Exit Code (after repair): {rc} ===\n\n")
                else:
                    rc = subprocess.run(cmd, check=False).returncode
            
            # If still failed, add to blacklist
            if rc != 0:
                add_to_blacklist(input_file, log_file)
                print(f"FFmpeg failed with exit code {rc} (file added to blacklist)", file=sys.stderr)
                return 6

        return 0
        
    except FileNotFoundError:
        print("ERROR: ffmpeg not found on PATH", file=sys.stderr)
        return 7
    finally:
        # Cleanup repaired temp file
        if repaired_temp_file and os.path.exists(repaired_temp_file):
            try:
                os.remove(repaired_temp_file)
            except OSError:
                pass


def cut_video_with_concat_demuxer(
    input_file: str,
    keep_segments: List[Tuple[float, Optional[float]]],
    output_file: str,
    srt_file: Optional[str] = None,
    txt_file: Optional[str] = None,
    log_file: Optional[str] = None,
) -> int:
    """Cut video using concat demuxer (MEMORY-EFFICIENT for large files).
    
    This method:
    1. Extracts each segment with stream copy (fast, no re-encoding)
    2. Concatenates segments using concat demuxer
    3. Re-encodes only once at the end
    
    Uses much less RAM than filter_complex approach.
    """
    
    temp_dir = tempfile.mkdtemp(prefix="ffmpeg_segments_")
    segment_files = []
    concat_list = os.path.join(temp_dir, "segments.txt")
    
    try:
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"\n=== Using concat demuxer (memory-efficient) ===\n")
                f.write(f"Segments: {len(keep_segments)}\n")
        
        # Step 1: Extract each segment with stream copy (fast!)
        for i, (start, end) in enumerate(keep_segments):
            segment_file = os.path.join(temp_dir, f"segment_{i:03d}.ts")
            segment_files.append(segment_file)
            
            cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error"]
            cmd.extend(["-i", input_file])
            cmd.extend(["-ss", str(start)])
            
            if end is not None:
                cmd.extend(["-to", str(end)])
            
            # CRITICAL: Use copy codec to avoid re-encoding (saves RAM and time)
            cmd.extend([
                "-c", "copy",
                "-avoid_negative_ts", "make_zero",
                "-y",
                segment_file
            ])
            
            if log_file:
                with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                    f.write(f"Extracting segment {i+1}/{len(keep_segments)}: {start} - {end}\n")
                    rc = subprocess.run(cmd, stdout=f, stderr=f, check=False).returncode
                    if rc != 0:
                        f.write(f"WARNING: Segment {i+1} extraction failed (rc={rc})\n")
                        return 6
            else:
                rc = subprocess.run(cmd, check=False).returncode
                if rc != 0:
                    return 6
        
        # Step 2: Create concat list
        with open(concat_list, "w", encoding="utf-8") as f:
            for seg_file in segment_files:
                # Use absolute path for safety
                f.write(f"file '{os.path.abspath(seg_file)}'\n")
        
        # Step 3: Concatenate and re-encode in one pass
        cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error"]
        cmd.extend(["-f", "concat", "-safe", "0", "-i", concat_list])
        
        # Add subtitles if available
        if srt_file and os.path.exists(srt_file):
            cmd.extend(["-i", srt_file])
            cmd.extend(["-map", "0:v", "-map", "0:a", "-map", "1:0"])
            cmd.extend(["-c:s", "srt", "-metadata:s:s:0", "language=ger"])
        else:
            cmd.extend(["-map", "0"])
        
        # Add metadata if available
        if txt_file and os.path.exists(txt_file):
            meta = process_metadata(txt_file)
            cmd.extend(build_metadata_flags(meta))
        
        # Encoding settings
        cmd.extend([
            "-c:v", "libx264",
            "-crf", "21",
            "-preset", "faster",
            "-c:a", "aac",
            "-b:a", "192k",
            "-y",
            output_file
        ])
        
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"\nConcatenating {len(segment_files)} segments...\n")
                rc = subprocess.run(cmd, stdout=f, stderr=f, check=False).returncode
                f.write(f"\n=== FFmpeg Exit Code: {rc} ===\n\n")
        else:
            rc = subprocess.run(cmd, check=False).returncode
        
        if rc != 0:
            print(f"FFmpeg concat failed with exit code {rc}", file=sys.stderr)
            return 6
        
        return 0
        
    finally:
        # Cleanup temp files
        shutil.rmtree(temp_dir, ignore_errors=True)


def build_filter_complex(keep_segments: List[Tuple[float, Optional[float]]]) -> str:
    """Build an FFmpeg filter_complex string (for small files only)."""
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
    filter_parts.append(
        f"{concat_inputs}concat=n={len(keep_segments)}:v=1:a=1[outv][outa]"
    )
    return "".join(filter_parts)


def cut_video_with_filter_complex(
    input_file: str,
    keep_segments: List[Tuple[float, Optional[float]]],
    output_file: str,
    srt_file: Optional[str] = None,
    txt_file: Optional[str] = None,
    log_file: Optional[str] = None,
) -> int:
    """Cut video using filter_complex (original method, for small files)."""
    
    filter_complex = build_filter_complex(keep_segments)
    
    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-i", input_file]

    if srt_file and os.path.exists(srt_file):
        cmd.extend(["-i", srt_file])
        has_srt = True
    else:
        has_srt = False

    cmd.extend(["-filter_complex", filter_complex, "-map", "[outv]", "-map", "[outa]"])

    if has_srt:
        cmd.extend(["-map", "1:0", "-c:s", "srt", "-metadata:s:s:0", "language=ger"])

    if txt_file and os.path.exists(txt_file):
        meta = process_metadata(txt_file)
        cmd.extend(build_metadata_flags(meta))

    cmd.extend([
        "-c:v", "libx264",
        "-crf", "21",
        "-preset", "faster",
        "-c:a", "aac",
        "-b:a", "192k",
        "-y",
        output_file,
    ])

    try:
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                f_log.write(
                    f"\n=== FFmpeg Processing (filter_complex): "
                    f"{os.path.basename(input_file)} ===\n"
                )
                f_log.write(f"Keep segments: {len(keep_segments)}\n\n")
                rc = subprocess.run(
                    cmd, stdout=f_log, stderr=f_log, check=False
                ).returncode
                f_log.write(f"\n=== FFmpeg Exit Code: {rc} ===\n\n")
        else:
            rc = subprocess.run(cmd, check=False).returncode
            
        if rc != 0:
            print(f"FFmpeg failed with exit code {rc}", file=sys.stderr)
            return 6
        return 0
    except FileNotFoundError:
        print("ERROR: ffmpeg not found on PATH", file=sys.stderr)
        return 7


def cut_video(
    input_file: str,
    edl_file: str,
    output_file: str,
    srt_file: Optional[str] = None,
    txt_file: Optional[str] = None,
    log_file: Optional[str] = None,
    dry_run: bool = False,
) -> int:
    """Main entry point for video processing with intelligent method selection."""
    
    # Validate input
    if not os.path.exists(input_file):
        print(f"ERROR: input file not found: {input_file}", file=sys.stderr)
        return 3
        
    if not edl_file or edl_file == "none":
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"[INFO] No EDL for {os.path.basename(input_file)}. Converting without cuts.\n")
        return convert_without_cuts(input_file, output_file, srt_file, txt_file, log_file)
        
    if not os.path.exists(edl_file):
        print(f"ERROR: EDL file not found: {edl_file}", file=sys.stderr)
        return 4

    # Check if EDL has no commercials
    if edl_has_no_commercials(edl_file):
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"[INFO] No commercials in EDL for {os.path.basename(input_file)}. Converting without cuts.\n")
        return convert_without_cuts(input_file, output_file, srt_file, txt_file, log_file)

    # Parse EDL
    with open(edl_file, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    cuts = parse_edl_lines(lines)
    keep_segments = keep_segments_from_cuts(cuts)
    
    if not keep_segments:
        print("ERROR: No keep segments computed from EDL", file=sys.stderr)
        return 5

    if dry_run:
        print(f"Would process {len(keep_segments)} segments")
        return 0

    # INTELLIGENT METHOD SELECTION
    # Use concat_demuxer for large files or many segments (memory-efficient)
    # Use filter_complex for small files (faster)
    
    file_size_mb = os.path.getsize(input_file) / (1024 * 1024)
    num_segments = len(keep_segments)
    
    # Use concat demuxer if:
    # - More than 5 segments OR
    # - File larger than 500MB OR
    # - File larger than 200MB AND more than 3 segments
    use_concat = (
        num_segments > 5 or
        file_size_mb > 500 or
        (file_size_mb > 200 and num_segments > 3)
    )
    
    if log_file:
        with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
            f.write(f"File size: {file_size_mb:.1f}MB, Segments: {num_segments}\n")
            f.write(f"Method: {'concat_demuxer (memory-efficient)' if use_concat else 'filter_complex (fast)'}\n")
    
    if use_concat:
        return cut_video_with_concat_demuxer(
            input_file, keep_segments, output_file, srt_file, txt_file, log_file
        )
    else:
        return cut_video_with_filter_complex(
            input_file, keep_segments, output_file, srt_file, txt_file, log_file
        )


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(
            "Usage: cut_with_edl.py <input_video> <edl_file|none> "
            "<output_file> [srt_file] [txt_file] [log_file]",
            file=sys.stderr,
        )
        sys.exit(2)

    input_video = sys.argv[1]
    edl_video = sys.argv[2]
    output_video = sys.argv[3]
    srt_subtitle = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] != "none" else None
    txt_metadata = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] != "none" else None
    log_output = sys.argv[6] if len(sys.argv) > 6 and sys.argv[6] != "none" else None

    EXIT_CODE = cut_video(
        input_video, edl_video, output_video, srt_subtitle, txt_metadata, log_output
    )
    sys.exit(EXIT_CODE)
