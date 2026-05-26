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
# This will be set dynamically based on the log file location if provided
BLACKLIST_FILE = None

# --- GPU / Hardware-Video-Encoding (COMSKIP_VENC env) ---
_ENCODERS_OUT: Optional[str] = None


def _ffmpeg_encoders_output() -> str:
    global _ENCODERS_OUT
    if _ENCODERS_OUT is None:
        r = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
            check=False,
        )
        _ENCODERS_OUT = r.stdout or ""
    return _ENCODERS_OUT


def _encoder_available(name: str) -> bool:
    return name in _ffmpeg_encoders_output()


def _nvidia_driver_ok() -> bool:
    try:
        r = subprocess.run(
            ["nvidia-smi", "-L"],
            capture_output=True,
            timeout=5,
            check=False,
        )
        return r.returncode == 0 and b"GPU" in r.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _vaapi_device_path() -> Optional[str]:
    dev = os.environ.get("COMSKIP_VAAPI_DEVICE", "/dev/dri/renderD128")
    return dev if os.path.exists(dev) else None


def _detect_auto_profile() -> str:
    if _encoder_available("h264_nvenc") and _nvidia_driver_ok():
        return "nvenc"
    if _vaapi_device_path() and _encoder_available("h264_vaapi"):
        return "vaapi"
    if _encoder_available("h264_qsv"):
        return "qsv"
    return "cpu"


def _resolve_venc_from_env(raw: str) -> str:
    r = (raw or "auto").strip().lower()
    if r in ("", "auto"):
        return _detect_auto_profile()
    if r in ("cpu", "libx264", "x264"):
        return "cpu"
    if r in ("nvenc", "h264_nvenc"):
        return "nvenc" if _encoder_available("h264_nvenc") else "cpu"
    if r in ("vaapi", "h264_vaapi"):
        if _vaapi_device_path() and _encoder_available("h264_vaapi"):
            return "vaapi"
        return "cpu"
    if r in ("qsv", "h264_qsv"):
        return "qsv" if _encoder_available("h264_qsv") else "cpu"
    return _detect_auto_profile()


def resolve_video_profile(for_filter_complex: bool, log_file: Optional[str]) -> str:
    """Wählt libx264 vs. NVENC/VAAPI/QSV. VAAPI + filter_complex -> CPU."""
    profile = _resolve_venc_from_env(os.environ.get("COMSKIP_VENC", "auto"))
    if for_filter_complex and profile == "vaapi":
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(
                    "[ENCODE] VAAPI mit filter_complex nicht unterstützt — nutze libx264\n"
                )
        profile = "cpu"
    return profile


def build_video_encode_args(profile: str) -> List[str]:
    """FFmpeg-Argumente für Video (inkl. ggf. -vaapi_device/-vf vor -c:v)."""
    if profile == "cpu":
        return ["-c:v", "libx264", "-crf", "21", "-preset", "faster"]
    if profile == "nvenc":
        cq = os.environ.get("COMSKIP_NVENC_CQ", "23")
        return ["-c:v", "h264_nvenc", "-preset", "p4", "-cq", cq, "-b:v", "0"]
    if profile == "vaapi":
        dev = os.environ.get("COMSKIP_VAAPI_DEVICE", "/dev/dri/renderD128")
        qp = os.environ.get("COMSKIP_VAAPI_QP", "23")
        return [
            "-vaapi_device",
            dev,
            "-vf",
            "format=nv12,hwupload",
            "-c:v",
            "h264_vaapi",
            "-qp",
            qp,
        ]
    if profile == "qsv":
        q = os.environ.get("COMSKIP_QSV_QUALITY", "23")
        return ["-c:v", "h264_qsv", "-global_quality", q, "-preset", "medium"]
    return build_video_encode_args("cpu")


def log_encode_profile(profile: str, log_file: Optional[str]) -> None:
    if not log_file:
        return
    with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
        f.write(f"[ENCODE] Video-Profil: {profile}\n")


def run_ffmpeg_with_profile_fallback(
    cmd_prefix: List[str],
    cmd_suffix: List[str],
    preferred_profile: str,
    log_file: Optional[str],
) -> int:
    """Run FFmpeg with preferred profile; on failure retry with CPU once.

    Returns FFmpeg exit code.
    """
    profiles = [preferred_profile]
    if preferred_profile != "cpu":
        profiles.append("cpu")

    last_rc = 1
    for p in profiles:
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"[ENCODE] Versuch: {p}\n")
        cmd = list(cmd_prefix)
        cmd.extend(build_video_encode_args(p))
        cmd.extend(cmd_suffix)
        try:
            if log_file:
                with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                    f.write("[ENCODE] FFmpeg cmd: " + " ".join(cmd) + "\n")
                    last_rc = subprocess.run(cmd, stdout=f, stderr=f, check=False).returncode
            else:
                last_rc = subprocess.run(cmd, check=False).returncode
        except FileNotFoundError:
            return 7

        if last_rc == 0:
            return 0

        if p != "cpu" and log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(
                    f"[ENCODE] Hardware-Encode fehlgeschlagen (rc={last_rc}), "
                    "Fallback auf CPU...\n"
                )

    return last_rc


def print_venc_probe() -> None:
    """Diagnose für Kommandozeile (--probe-venc)."""
    auto = _detect_auto_profile()
    print(f"COMSKIP_VENC (auto-Erkennung): {auto}")
    print(f"  h264_nvenc in ffmpeg: {_encoder_available('h264_nvenc')}")
    print(f"  nvidia-smi -L OK:     {_nvidia_driver_ok()}")
    print(f"  VAAPI-Gerät:          {_vaapi_device_path() or '(keins)'}")
    print(f"  h264_vaapi in ffmpeg: {_encoder_available('h264_vaapi')}")
    print(f"  h264_qsv in ffmpeg:   {_encoder_available('h264_qsv')}")

def get_blacklist_path(log_file: Optional[str] = None) -> str:
    """Determine blacklist file path from log file location or use default."""
    if log_file:
        log_dir = os.path.dirname(log_file)
        return os.path.join(log_dir, "corrupted_files.blacklist")
    return "/tmp/corrupted_files.blacklist"


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


def is_blacklisted(input_file: str, log_file: Optional[str] = None) -> bool:
    """Check if file is in the corruption blacklist."""
    blacklist_file = get_blacklist_path(log_file)
    if not os.path.exists(blacklist_file):
        return False
    basename = os.path.basename(input_file)
    try:
        with open(blacklist_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip() == basename:
                    return True
    except (OSError, IOError):
        pass
    return False


def add_to_blacklist(input_file: str, log_file: Optional[str] = None) -> None:
    """Add a corrupted file to the blacklist."""
    blacklist_file = get_blacklist_path(log_file)
    basename = os.path.basename(input_file)
    try:
        os.makedirs(os.path.dirname(blacklist_file), exist_ok=True)
        with open(blacklist_file, "a", encoding="utf-8") as f:
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
    if is_blacklisted(input_file, log_file):
        msg = f"File is blacklisted (permanently corrupted): {os.path.basename(input_file)}"
        print(msg, file=sys.stderr)
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"[BLACKLIST] {msg}\n")
        return 9  # New exit code for blacklisted files
    
    # First attempt with error tolerance
    working_file = input_file
    repaired_temp_file = None

    try:
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                f_log.write(
                    f"\n=== FFmpeg Conversion (No Commercials): "
                    f"{os.path.basename(input_file)} ===\n"
                )
        profile = resolve_video_profile(False, log_file)
        log_encode_profile(profile, log_file)

        def _build_prefix(current_input: str) -> List[str]:
            cmd_prefix = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error"]
            cmd_prefix.extend(["-err_detect", "ignore_err"])
            cmd_prefix.extend(["-i", current_input])
            has_srt_local = False
            if srt_file and os.path.exists(srt_file):
                cmd_prefix.extend(["-i", srt_file])
                has_srt_local = True
            cmd_prefix.extend(["-map", "0:v", "-map", "0:a"])
            if has_srt_local:
                cmd_prefix.extend(
                    ["-map", "1:0", "-c:s", "srt", "-metadata:s:s:0", "language=ger"]
                )
            if txt_file and os.path.exists(txt_file):
                meta = process_metadata(txt_file)
                cmd_prefix.extend(build_metadata_flags(meta))
            return cmd_prefix

        cmd_suffix = ["-c:a", "aac", "-b:a", "192k", "-y", output_file]
        rc = run_ffmpeg_with_profile_fallback(
            _build_prefix(working_file), cmd_suffix, profile, log_file
        )
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                f_log.write(f"\n=== FFmpeg Exit Code: {rc} ===\n")

        # If failed, attempt repair
        if rc != 0:
            if log_file:
                with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                    f_log.write(f"[INFO] First attempt failed (rc={rc}), attempting repair...\n")
            
            repaired_temp_file = repair_corrupted_file(input_file, log_file)
            
            if repaired_temp_file:
                # Retry with repaired file
                working_file = repaired_temp_file
                if log_file:
                    with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                        f_log.write("\n=== Retry with Repaired File ===\n")

                rc = run_ffmpeg_with_profile_fallback(
                    _build_prefix(working_file), cmd_suffix, profile, log_file
                )
                if log_file:
                    with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                        f_log.write(
                            f"\n=== FFmpeg Exit Code (after repair): {rc} ===\n\n"
                        )
            
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
        cmd_prefix = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error"]
        cmd_prefix.extend(["-f", "concat", "-safe", "0", "-i", concat_list])
        
        # Add subtitles if available
        if srt_file and os.path.exists(srt_file):
            cmd_prefix.extend(["-i", srt_file])
            cmd_prefix.extend(["-map", "0:v", "-map", "0:a", "-map", "1:0"])
            cmd_prefix.extend(["-c:s", "srt", "-metadata:s:s:0", "language=ger"])
        else:
            cmd_prefix.extend(["-map", "0"])
        
        # Add metadata if available
        if txt_file and os.path.exists(txt_file):
            meta = process_metadata(txt_file)
            cmd_prefix.extend(build_metadata_flags(meta))
        
        # Encoding settings (GPU/CPU je nach COMSKIP_VENC, mit CPU-Fallback)
        profile = resolve_video_profile(False, log_file)
        log_encode_profile(profile, log_file)
        cmd_suffix = ["-c:a", "aac", "-b:a", "192k", "-y", output_file]
        
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"\nConcatenating {len(segment_files)} segments...\n")
        rc = run_ffmpeg_with_profile_fallback(cmd_prefix, cmd_suffix, profile, log_file)
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write(f"\n=== FFmpeg Exit Code: {rc} ===\n\n")
        
        if rc != 0:
            print(f"FFmpeg concat failed with exit code {rc}", file=sys.stderr)
            return 6
        
        return 0
        
    finally:
        # Cleanup temp files
        shutil.rmtree(temp_dir, ignore_errors=True)


def build_filter_complex(keep_segments: List[Tuple[float, Optional[float]]], has_audio: bool = True) -> str:
    """Build an FFmpeg filter_complex string (for small files only)."""
    v_filters = []
    a_filters = []
    for i, (s, e) in enumerate(keep_segments):
        end_str = f":end={e}" if e is not None else ""
        v_filters.append(f"[0:v]trim=start={s}{end_str},setpts=PTS-STARTPTS[v{i}]")
        if has_audio:
            a_filters.append(f"[0:a]atrim=start={s}{end_str},asetpts=PTS-STARTPTS[a{i}]")

    filter_parts = []
    if v_filters:
        filter_parts.append("; ".join(v_filters) + "; ")
    if a_filters:
        filter_parts.append("; ".join(a_filters) + "; ")
    
    if has_audio:
        concat_inputs = "".join([f"[v{i}][a{i}]" for i in range(len(keep_segments))])
        filter_parts.append(
            f"{concat_inputs}concat=n={len(keep_segments)}:v=1:a=1[outv][outa]"
        )
    else:
        concat_inputs = "".join([f"[v{i}]" for i in range(len(keep_segments))])
        filter_parts.append(
            f"{concat_inputs}concat=n={len(keep_segments)}:v=1:a=0[outv]"
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
    
    # Check if file has audio stream
    has_audio = True
    try:
        probe_cmd = ["ffprobe", "-v", "error", "-select_streams", "a:0", "-show_entries", "stream=codec_type", "-of", "default=noprint_wrappers=1:nokey=1", input_file]
        result = subprocess.run(probe_cmd, capture_output=True, text=True, check=False)
        has_audio = result.stdout.strip() == "audio"
    except:
        pass  # Assume has audio if probe fails
    
    filter_complex = build_filter_complex(keep_segments, has_audio=has_audio)
    
    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-i", input_file]

    if srt_file and os.path.exists(srt_file):
        cmd.extend(["-i", srt_file])
        has_srt = True
    else:
        has_srt = False

    cmd.extend(["-filter_complex", filter_complex])
    
    if has_audio:
        cmd.extend(["-map", "[outv]", "-map", "[outa]"])
    else:
        cmd.extend(["-map", "[outv]"])

    if has_srt:
        cmd.extend(["-map", "1:0", "-c:s", "srt", "-metadata:s:s:0", "language=ger"])

    if txt_file and os.path.exists(txt_file):
        meta = process_metadata(txt_file)
        cmd.extend(build_metadata_flags(meta))

    profile = resolve_video_profile(True, log_file)
    log_encode_profile(profile, log_file)
    
    if has_audio:
        cmd_suffix = ["-c:a", "aac", "-b:a", "192k", "-y", output_file]
    else:
        cmd_suffix = ["-y", output_file]

    try:
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                f_log.write(
                    f"\n=== FFmpeg Processing (filter_complex): "
                    f"{os.path.basename(input_file)} ===\n"
                )
                f_log.write(f"Keep segments: {len(keep_segments)}\n")
                f_log.write(f"Audio stream: {'present' if has_audio else 'absent'}\n\n")
        rc = run_ffmpeg_with_profile_fallback(cmd, cmd_suffix, profile, log_file)
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f_log:
                f_log.write(f"\n=== FFmpeg Exit Code: {rc} ===\n\n")
            
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
    
    # Try selected method first
    if use_concat:
        result = cut_video_with_concat_demuxer(
            input_file, keep_segments, output_file, srt_file, txt_file, log_file
        )
    else:
        result = cut_video_with_filter_complex(
            input_file, keep_segments, output_file, srt_file, txt_file, log_file
        )
    
    # If filter_complex failed, try concat_demuxer as fallback
    if result == 6 and not use_concat:
        if log_file:
            with open(log_file, "a", encoding="utf-8", errors="ignore") as f:
                f.write("[INFO] filter_complex failed, retrying with concat_demuxer...\n")
        result = cut_video_with_concat_demuxer(
            input_file, keep_segments, output_file, srt_file, txt_file, log_file
        )
    
    return result


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--probe-venc":
        print_venc_probe()
        sys.exit(0)

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
