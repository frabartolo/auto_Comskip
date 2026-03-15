#!/usr/bin/env python3
"""Schallplatten-Rip: Eine lange Audio-Datei (MP3/WAV) an Stille in Einzeltracks zerlegen.

Verwendet ffmpeg silencedetect zur Erkennung von Pausen zwischen Tracks.
Ausgabe: nummerierte Dateien (z.B. 01.wav, 02.wav) oder mit optionalen Track-Namen.

Hinweis: Deezer Spleeter trennt Quellen (Gesang/Begleitung), nicht Tracks.
Für reine Track-Trennung reicht Stille-Erkennung; Spleeter kann optional nachgelagert genutzt werden.
"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple


def get_audio_duration_seconds(path: str) -> float:
    """Dauer der Audiodatei in Sekunden via ffprobe."""
    cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", path
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe fehlgeschlagen: {result.stderr}")
    return float(result.stdout.strip())


def run_silencedetect(
    input_path: str,
    noise_db: float = -35,
    min_silence_duration: float = 2.0,
) -> List[Tuple[float, float]]:
    """Stille-Intervalle ermitteln. Returns [(start, end), ...] in Sekunden."""
    # silencedetect liefert Ausgabe auf stderr
    cmd = [
        "ffmpeg", "-nostdin", "-i", input_path,
        "-af", f"silencedetect=noise={noise_db}dB:d={min_silence_duration}",
        "-f", "null", "-"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    # stderr enthält: [silencedetect @ ...] silence_start: 123.45
    #                [silencedetect @ ...] silence_end: 126.78
    stderr = result.stderr or ""
    starts = [float(m.group(1)) for m in re.finditer(r"silence_start:\s*([\d.]+)", stderr)]
    ends = [float(m.group(1)) for m in re.finditer(r"silence_end:\s*([\d.]+)", stderr)]
    if len(starts) != len(ends):
        # Fallback: nur starts nutzen, ends aus nächstem start
        if starts and not ends:
            duration = get_audio_duration_seconds(input_path)
            ends = starts[1:] + [duration]
        elif len(ends) > len(starts):
            ends = ends[: len(starts)]
        else:
            starts = starts[: len(ends)]
    return list(zip(starts, ends))


def silence_to_segments(
    silences: List[Tuple[float, float]],
    total_duration: float,
    min_track_length: float = 10.0,
    padding: float = 0.2,
) -> List[Tuple[float, Optional[float]]]:
    """Aus Stille-Intervallen die zu behaltenden Segmente (start, end) erzeugen.

    - padding: Sekunden vor/nach der Stille abziehen, damit keine Klicks am Schnitt sind.
    - min_track_length: Zu kurze Segmente werden übersprungen (z.B. Störungen).
    """
    segments = []
    start = 0.0
    for s_start, s_end in silences:
        seg_end = max(start, s_start + padding)
        if seg_end - start >= min_track_length:
            segments.append((start, seg_end))
        start = max(start, s_end - padding)
    if start < total_duration and (total_duration - start) >= min_track_length:
        segments.append((start, None))  # bis Ende
    return segments


def split_audio(
    input_path: str,
    output_dir: str,
    segments: List[Tuple[float, Optional[float]]],
    output_format: str = "mp3",
    copy_codec: bool = False,
) -> List[str]:
    """Segmente mit ffmpeg ausschneiden. Gibt Liste der erzeugten Dateien zurück."""
    os.makedirs(output_dir, exist_ok=True)
    input_ext = Path(input_path).suffix.lstrip(".").lower()
    use_copy = output_format == "copy" or (copy_codec and input_ext in ("mp3", "wav", "flac"))
    out_files = []
    for i, (seg_start, seg_end) in enumerate(segments, start=1):
        ext = input_ext if output_format == "copy" else output_format
        out_name = f"{i:02d}.{ext}"
        out_path = os.path.join(output_dir, out_name)
        cmd = ["ffmpeg", "-nostdin", "-y", "-hide_banner", "-loglevel", "error",
               "-ss", str(seg_start), "-i", input_path]
        if seg_end is not None:
            cmd += ["-to", str(seg_end)]
        if use_copy:
            cmd += ["-c", "copy", out_path]
        else:
            if output_format == "mp3":
                cmd += ["-vn", "-c:a", "libmp3lame", "-q:a", "2", out_path]
            else:
                cmd += ["-vn", "-c:a", "pcm_s16le", out_path]
        subprocess.run(cmd, check=True)
        out_files.append(out_path)
    return out_files


def main():
    parser = argparse.ArgumentParser(description="Audio per Stille in Tracks aufteilen")
    parser.add_argument("input", help="Eingabe-Audiodatei (MP3/WAV/…)")
    parser.add_argument("-o", "--output-dir", default=None,
                        help="Ausgabeordner (Default: neben der Datei / <name>_tracks)")
    parser.add_argument("--noise-db", type=float, default=-35,
                        help="Stille-Schwellwert in dB (default: -35)")
    parser.add_argument("--min-silence", type=float, default=2.0,
                        help="Mindestdauer Stille in Sekunden (default: 2)")
    parser.add_argument("--min-track", type=float, default=10.0,
                        help="Mindestlänge eines Tracks in Sekunden (default: 10)")
    parser.add_argument("--padding", type=float, default=0.2,
                        help="Sekunden vor/nach Stille abziehen (default: 0.2)")
    parser.add_argument("--format", choices=("mp3", "wav", "copy"), default="mp3",
                        help="Ausgabeformat (default: mp3)")
    parser.add_argument("--dry-run", action="store_true", help="Nur Segmente anzeigen, nicht schneiden")
    args = parser.parse_args()

    input_path = os.path.abspath(args.input)
    if not os.path.isfile(input_path):
        print(f"Datei nicht gefunden: {input_path}", file=sys.stderr)
        sys.exit(1)

    if args.output_dir:
        output_dir = os.path.abspath(args.output_dir)
    else:
        base = Path(input_path).stem
        output_dir = str(Path(input_path).parent / f"{base}_tracks")

    duration = get_audio_duration_seconds(input_path)
    silences = run_silencedetect(input_path, noise_db=args.noise_db, min_silence_duration=args.min_silence)
    segments = silence_to_segments(
        silences, duration,
        min_track_length=args.min_track,
        padding=args.padding,
    )

    print(f"Dauer: {duration:.1f} s, Stille-Intervalle: {len(silences)}, Segmente: {len(segments)}")
    for i, (a, b) in enumerate(segments, 1):
        end_s = b if b is not None else duration
        print(f"  Track {i:02d}: {a:.1f} s – {end_s:.1f} s ({end_s - a:.1f} s)")
    if args.dry_run:
        return
    files = split_audio(input_path, output_dir, segments, output_format=args.format)
    print(f"Geschrieben: {output_dir}")
    for f in files:
        print(f"  {f}")


if __name__ == "__main__":
    main()
