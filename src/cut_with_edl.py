#!/usr/bin/env python3
"""Commercial removal automation using Comskip EDL files and FFmpeg.

This module processes video files by parsing Comskip-generated EDL (Edit Decision List)
files to identify commercial segments, then uses FFmpeg to cut and concatenate the
non-commercial portions into a clean output file.

Typical usage:
    python3 cut_with_edl.py input.mp4 input.edl output.mkv [input.srt] [input.txt] [log.txt]

Exit codes:
    0: Success
    2: Invalid command-line arguments
    3: Input video file not found
    4: EDL file not found
    5: No keep segments computed from EDL
    6: FFmpeg processing failed
    7: FFmpeg executable not found on PATH
    8: Subtitle/metadata file format error

Module structure:
    - edl_has_no_commercials: Check if EDL file contains no commercial markers
    - parse_edl_lines: Parse EDL format into structured cuts
    - keep_segments_from_cuts: Convert cuts into keep-segments
    - build_filter_complex: Generate FFmpeg filter graph
    - convert_without_cuts: Convert video to MKV without cutting commercials
    - cut_video: Main orchestration function
"""

import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple


def edl_has_no_commercials(edl_file: str) -> bool:
    """Check if an EDL file contains no commercial segments.

    Reads an EDL file and determines if it has any actual commercial markers
    (lines with action='0'). Returns True if the EDL is empty or contains
    only comments and empty lines.

    Args:
        edl_file: Path to the EDL file to check.

    Returns:
        True if no commercials found (EDL is empty or has only comments),
        False if at least one commercial segment exists.

    Examples:
        >>> # Create a test EDL with no commercials
        >>> with open('test.edl', 'w') as f:
        ...     f.write('# Comment only\\n')
        >>> edl_has_no_commercials('test.edl')
        True

        >>> # Create a test EDL with a commercial
        >>> with open('test.edl', 'w') as f:
        ...     f.write('10.0 20.0 0\\n')
        >>> edl_has_no_commercials('test.edl')
        False
    """
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


def convert_without_cuts(
    input_file: str,
    output_file: str,
    srt_file: Optional[str] = None,
    txt_file: Optional[str] = None,
    log_file: Optional[str] = None,
) -> int:
    """Convert video to MKV format without cutting commercials.

    Used when Comskip detects no commercials in the video. Simply converts
    the video to MKV format and optionally attaches subtitle and metadata files.

    Args:
        input_file: Path to input video file.
        output_file: Path for output video (typically .mkv).
        srt_file: Optional path to subtitle file (.srt) to attach with language=ger.
                 Pass None or 'none' to skip.
        txt_file: Optional path to text file for metadata.
                 Content will be added as comment and description metadata.
        log_file: Optional path to log file for recording the operation.

    Returns:
        Integer exit code:
            0: Success
            3: Input video file not found
            6: FFmpeg execution failed
            7: FFmpeg not found on system PATH

    FFmpeg settings:
        - Video: libx264 codec, CRF 21, faster preset
        - Audio: AAC codec, 192k bitrate
        - Subtitles: SRT codec with German language tag (if provided)
    """
    if not os.path.exists(input_file):
        print(f"ERROR: input file not found: {input_file}", file=sys.stderr)
        return 3

    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-i", input_file]

    has_srt = False
    if srt_file and os.path.exists(srt_file):
        cmd.extend(["-i", srt_file])
        has_srt = True

    # Map streams directly without cutting
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
                    f"\n=== FFmpeg Conversion (No Commercials Detected): "
                    f"{os.path.basename(input_file)} ===\n"
                )
                f_log.write(
                    f"Time: {subprocess.run(['date'],
                                            capture_output=True,
                                            text=True,
                                            shell=True,
                                            check=False).stdout.strip()}\n"
                )
                f_log.write(f"Input: {input_file}\nOutput: {output_file}\n")
                f_log.write(
                    "Note: No commercials detected in EDL. Converting without cuts.\n\n"
                )
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


def parse_edl_lines(lines: List[str]) -> List[Tuple[float, float, str]]:
    """Parse EDL file lines into structured cut information.

    Reads Comskip EDL format lines and extracts commercial segment boundaries.
    Each valid line contains: start_time end_time action_code.
    Action code '0' typically indicates a commercial segment to remove.

    Args:
        lines: Raw text lines from an EDL file, including newlines.

    Returns:
        List of tuples containing (start_time, end_time, action_code).
        Times are in seconds as floats, action_code is a string.

    Examples:
        >>> lines = ["10.5 20.3 0\n", "# comment\n", "30.0 45.2 0\n"]
        >>> parse_edl_lines(lines)
        [(10.5, 20.3, '0'), (30.0, 45.2, '0')]

    Note:
        Malformed lines, comments (starting with #), and empty lines are silently ignored.
    """
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
            # skip malformed lines
            continue
    return cuts


def keep_segments_from_cuts(
    cuts: List[Tuple[float, float, str]],
) -> List[Tuple[float, Optional[float]]]:
    """Convert commercial cut points into segments to keep.

    Inverts the commercial detection logic: given segments to remove (action='0'),
    computes the video portions to preserve. The algorithm identifies gaps between
    commercial blocks and creates keep-segments spanning those gaps.

    Args:
        cuts: List of (start, end, action) tuples from parse_edl_lines.
              Only cuts with action='0' are treated as commercials to remove.

    Returns:
        List of (start, end) tuples representing video segments to keep.
        The final segment has end=None, meaning "keep until end of file".

    Examples:
        >>> cuts = [(10.0, 20.0, '0'), (40.0, 50.0, '0')]  # commercials at 10-20 and 40-50
        >>> keep_segments_from_cuts(cuts)
        [(0.0, 10.0), (20.0, 40.0), (50.0, None)]

    Note:
        Always returns at least one segment, even if no commercials are detected.
        If the EDL is empty, returns [(0.0, None)] to keep the entire video.
    """
    keep_segments: List[Tuple[float, Optional[float]]] = []
    last_end = 0.0
    for start, end, action in cuts:
        if action == "0":
            if start > last_end:
                keep_segments.append((last_end, start))
            last_end = end
    keep_segments.append((last_end, None))
    return keep_segments


def build_filter_complex(keep_segments: List[Tuple[float, Optional[float]]]) -> str:
    """Build an FFmpeg filter_complex string for concatenating keep segments.

    Constructs a complex FFmpeg filter graph that:
    1. Trims each keep segment from video and audio streams
    2. Resets presentation timestamps (PTS) to start from zero
    3. Concatenates all trimmed segments into continuous output

    Args:
        keep_segments: List of (start, end) tuples defining segments to preserve.
                      end=None means trim to end of input file.

    Returns:
        Complete FFmpeg filter_complex string ready for -filter_complex argument.

    Examples:
        >>> segments = [(0.0, 10.0), (20.0, 30.0), (40.0, None)]
        >>> fc = build_filter_complex(segments)
        >>> "concat=n=3" in fc
        True

    Filter structure:
        - Video filters: [0:v]trim=start=X:end=Y,setpts=PTS-STARTPTS[vN]
        - Audio filters: [0:a]atrim=start=X:end=Y,asetpts=PTS-STARTPTS[aN]
        - Concatenation: [v0][a0][v1][a1]...concat=n=N:v=1:a=1[outv][outa]
    """
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


def parse_txt_metadata(file_path: str) -> Dict[str, str]:
    """Parse metadata from a text sidecar.

    Expected format (keys followed by a colon), example:
        Channel     : ZDF HD
        Date        : 05.06.2022
        Recording   : 01:35-03:11 3 Engel für Charlie - Volle Power
        Description : Actionfilm, USA 2003
        <additional description lines>

    The description block can span multiple lines after the Description key.
    """
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
                    # Non-metadata line before description starts is skipped
                    continue

                # Collect all lines after Description (including blanks) as free-form text
                if in_description:
                    description_lines.append(line)
        if description_lines:
            meta["description"] = "\n".join(description_lines).strip()
    except (OSError, IOError):
        return {}
    return meta


def parse_xml_metadata(file_path: str) -> Dict[str, str]:
    """Parse metadata from an XML sidecar (ArchiveTableArchive format).

    Expected format:
        <ArchiveTableArchive>
            <ArvTitle>16 Blocks</ArvTitle>
            <ArvShortInfo>Short description...</ArvShortInfo>
            <ArvLongInfo>Detailed description with cast...</ArvLongInfo>
            <ArvProgLogo>kabel eins</ArvProgLogo>
        </ArchiveTableArchive>
    """
    meta: Dict[str, str] = {}
    if not os.path.exists(file_path):
        return meta

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Extract title
        title_elem = root.find("ArvTitle")
        if title_elem is not None and title_elem.text:
            meta["recording"] = title_elem.text.strip()

        # Extract channel from ArvProgLogo
        channel_elem = root.find("ArvProgLogo")
        if channel_elem is not None and channel_elem.text:
            meta["channel"] = channel_elem.text.strip()

        # Build description from ArvShortInfo and ArvLongInfo
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


def cut_video(
    input_file: str,
    edl_file: str,
    output_file: str,
    srt_file: Optional[str] = None,
    txt_file: Optional[str] = None,
    log_file: Optional[str] = None,
    dry_run: bool = False,
) -> int:
    """Process video file to remove commercials based on EDL file.

    Main orchestration function that:
    1. Validates input files exist
    2. Parses EDL to identify commercial segments
    3. Computes keep-segments (non-commercial portions)
    4. Builds FFmpeg filter_complex for concatenation
    5. Invokes FFmpeg with encoding settings
    6. Optionally attaches subtitle and metadata files
    7. Logs all operations to consolidated log file

    Args:
        input_file: Path to input video file (any format FFmpeg supports).
        edl_file: Path to Comskip EDL file with commercial markers.
        output_file: Path for output video (typically .mkv for metadata support).
        srt_file: Optional path to subtitle file (.srt) to attach with language=ger.
                 Pass None or 'none' to skip subtitle attachment.
        txt_file: Optional path to text file for video description metadata.
                 Content will be added as comment and description metadata.
        log_file: Optional path to consolidated log file for all output.
                 If provided, FFmpeg stdout/stderr and processing info are appended.
        dry_run: If True, print filter_complex and exit without running FFmpeg.
                Useful for testing and debugging filter construction.

    Returns:
        Integer exit code:
            0: Success - video processed and written
            3: Input video file not found
            4: EDL file not found
            5: No keep segments computed (EDL may be invalid)
            6: FFmpeg execution failed (non-zero exit)
            7: FFmpeg not found on system PATH

    FFmpeg settings:
        - Video: libx264 codec, CRF 21, faster preset
        - Audio: AAC codec, 192k bitrate
        - Subtitles: SRT codec with German language tag
        - Output overwrites existing file (-y flag)

    Examples:
        >>> # Basic usage
        >>> exit_code = cut_video('video.mp4', 'video.edl', 'output.mkv')
        >>> # With subtitles and logging
        >>> exit_code = cut_video('video.mp4', 'video.edl', 'output.mkv',
        ...                      srt_file='video.srt', log_file='process.log')
        >>> # Dry run to test filter
        >>> cut_video('video.mp4', 'video.edl', 'output.mkv', dry_run=True)

    Note:
        This function directly invokes subprocess.run() for FFmpeg execution.
        Ensure FFmpeg is installed and available on PATH before calling.
    """
    # Validate mandatory files
    if not os.path.exists(input_file):
        print(f"ERROR: input file not found: {input_file}", file=sys.stderr)
        return 3
    if not os.path.exists(edl_file):
        print(f"ERROR: EDL file not found: {edl_file}", file=sys.stderr)
        return 4

    # Check if EDL has no commercials (empty or only comments)
    if edl_has_no_commercials(edl_file):
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(
                    f"[INFO] No commercials found in EDL for {os.path.basename(input_file)}."
                    f" Converting without cuts.\n"
                )
        return convert_without_cuts(
            input_file, output_file, srt_file, txt_file, log_file
        )

    with open(edl_file, "r", encoding="utf-8", errors="ignore") as f:
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
    # Im Python-Skript anpassen:
    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-i", input_file]
    # cmd = ["ffmpeg", "-i", input_file]

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

    # Run ffmpeg and optionally log
    try:
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                f_log.write(
                    f"\n=== FFmpeg Processing: {os.path.basename(input_file)} ===\n"
                )
                f_log.write(
                    f"Time: {subprocess.run(['date'],
                                            capture_output=True,
                                            text=True,
                                            shell=True,
                                            check=False).stdout.strip()}\n"
                )
                f_log.write(f"Input: {input_file}\nOutput: {output_file}\n")
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


if __name__ == "__main__":
    # Aufruf: script.py input_video edl_file output [srt] [txt] [log]
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
