#!/usr/bin/env python3
"""Getrennte Track-Dateien nach OCR-Metadaten umbenennen (und optional mit ffmpeg taggen)."""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path


def safe_filename(name: str, max_len: int = 120) -> str:
    """Dateinamen-sichere Zeichenkette."""
    s = re.sub(r'[<>:"/\\|?*]', "_", name).strip()
    s = re.sub(r"\s+", " ", s)
    return s[:max_len].strip(" .")


def rename_and_tag(
    track_dir: str,
    album: str,
    tracks: list,
    dry_run: bool = False,
    write_tags: bool = True,
) -> None:
    """Dateien 01.xxx, 02.xxx umbenennen in '01 Künstlername.mp3' und ID3/Metadata setzen."""
    files = sorted(
        [f for f in os.listdir(track_dir) if f.lower().endswith((".mp3", ".wav", ".flac"))],
        key=lambda x: (int(re.match(r"^(\d+)", x).group(1)) if re.match(r"^(\d+)", x) else 999, x),
    )
    for i, filename in enumerate(files):
        if i >= len(tracks):
            continue
        path = os.path.join(track_dir, filename)
        if not os.path.isfile(path):
            continue
        ext = Path(filename).suffix
        new_name = f"{i + 1:02d} {safe_filename(tracks[i])}{ext}"
        new_path = os.path.join(track_dir, new_name)
        if path == new_path or new_name == filename:
            continue
        if dry_run:
            print(f"  {filename}  →  {new_name}")
            continue
        if os.path.exists(new_path):
            print(f"Überspringe (Ziel existiert): {new_name}", file=sys.stderr)
            continue
        os.rename(path, new_path)
        print(f"  {filename}  →  {new_name}")
        if write_tags and new_path.lower().endswith(".mp3"):
            # ffmpeg -i f -metadata title="..." -metadata album="..." -c copy
            cmd = [
                "ffmpeg", "-nostdin", "-y", "-i", new_path,
                "-metadata", f"title={tracks[i]}",
                "-metadata", f"album={album}",
                "-metadata", f"track={i + 1}/{len(tracks)}",
                "-c", "copy",
                new_path + ".tagged",
            ]
            if subprocess.run(cmd, capture_output=True).returncode == 0:
                os.replace(new_path + ".tagged", new_path)


def main():
    parser = argparse.ArgumentParser(description="Tracks nach OCR-Metadaten umbenennen und taggen")
    parser.add_argument("dir", help="Ordner mit 01.xxx, 02.xxx, …")
    parser.add_argument("--meta", "-m", help="JSON-Datei mit album + tracks (von ocr_metadata.py --json)")
    parser.add_argument("--album", "-a", help="Albumtitel (wenn nicht in --meta)")
    parser.add_argument("--tracks", "-t", nargs="+", help="Tracktitel (wenn nicht in --meta)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-tags", action="store_true", help="Nur umbenennen, keine ID3-Tags setzen")
    args = parser.parse_args()

    if args.meta:
        with open(args.meta, encoding="utf-8") as f:
            data = json.load(f)
        album = data.get("album", args.album or "Unbekannt")
        tracks = data.get("tracks", args.tracks or [])
    else:
        album = args.album or "Unbekannt"
        tracks = args.tracks or []
    if not tracks:
        print("Keine Trackliste (--meta oder --tracks).", file=sys.stderr)
        sys.exit(1)
    rename_and_tag(args.dir, album, tracks, dry_run=args.dry_run, write_tags=not args.no_tags)


if __name__ == "__main__":
    main()
