#!/usr/bin/env python3
"""Metadaten aus Schallplatten-Bildern (Cover/Label) per OCR extrahieren.

Voraussetzung: Lokaler OCR-Dienst, z.B. DeepSeek OCR auf dem OpenClaw-Rechner.
Konfiguration über Umgebungsvariable OCR_CMD oder OCR_URL (siehe README).

Erkannt werden: Albumtitel und Trackliste (nummerierte Zeilen).
Ausgabe: JSON für die Pipeline (z.B. Umbenennung/Tagging der getrennten Tracks).
"""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# Typische Muster für Track-Zeilen: "1. Titel", "A1 Titel", "01 - Titel", "01  Titel"
TRACK_PATTERNS = [
    re.compile(r"^\s*(?:[A-D]?\s*)?(\d{1,2})[\.\-\:\s]+\s*(.+)$", re.IGNORECASE),  # 1. Title, A1 Title
    re.compile(r"^\s*(\d{1,2})\s+([^\d].+)$"),   # 01  Title
]


def get_ocr_command() -> Optional[str]:
    """OCR-Befehl aus Umgebung (z.B. Wrapper der per SSH OpenClaw aufruft)."""
    return os.environ.get("OCR_CMD") or os.environ.get("VINYL_OCR_CMD")


def get_ocr_url() -> Optional[str]:
    """OCR-API-URL falls ein HTTP-Dienst genutzt wird."""
    return os.environ.get("OCR_URL") or os.environ.get("VINYL_OCR_URL")


def run_ocr_on_image(image_path: str) -> str:
    """Einzelnes Bild an OCR übergeben, erkannten Text zurückgeben."""
    cmd = get_ocr_command()
    if cmd:
        # Einzelbefehl mit Pfad: z.B. "ocr_wrapper.sh" oder "ssh openClaw deepseek-ocr"
        # Bei mehreren Bildern: Aufruf pro Bild und Texte konkatenieren
        result = subprocess.run(
            [cmd, os.path.abspath(image_path)],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=os.path.dirname(os.path.abspath(image_path)) or ".",
        )
        if result.returncode != 0:
            raise RuntimeError(f"OCR fehlgeschlagen (exit {result.returncode}): {result.stderr}")
        return (result.stdout or "").strip()
    url = get_ocr_url()
    if url:
        try:
            import urllib.request
            with open(image_path, "rb") as f:
                data = f.read()
            req = urllib.request.Request(url, data=data, method="POST")
            req.add_header("Content-Type", "application/octet-stream")
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.read().decode("utf-8", errors="replace").strip()
        except Exception as e:
            raise RuntimeError(f"OCR-API Fehler: {e}") from e
    raise RuntimeError(
        "Kein OCR konfiguriert. Setze OCR_CMD (Befehl + Bildpfad) oder OCR_URL (POST mit Bild)."
    )


def run_ocr_on_images(image_paths: List[str]) -> str:
    """Mehrere Bilder nacheinander OCR-en und Text zusammenführen."""
    texts = []
    for path in image_paths:
        path = os.path.abspath(path)
        if not os.path.isfile(path):
            continue
        texts.append(run_ocr_on_image(path))
    return "\n\n".join(t for t in texts if t)


def parse_album_and_tracks(ocr_text: str) -> Dict[str, Any]:
    """Aus OCR-Text Albumtitel und Trackliste extrahieren (heuristisch)."""
    lines = [ln.strip() for ln in ocr_text.splitlines() if ln.strip()]
    album_title = ""
    tracks: List[str] = []
    seen_indices = set()

    for i, line in enumerate(lines):
        # Track-Zeile? (Nummer am Anfang)
        for pat in TRACK_PATTERNS:
            m = pat.match(line)
            if m:
                num_str, title = m.group(1), m.group(2).strip()
                try:
                    num = int(num_str)
                except ValueError:
                    continue
                if num not in seen_indices and len(title) > 1:
                    seen_indices.add(num)
                    # Doppelte Nummern (z.B. A1 und 1): nur eine behalten
                    if num <= len(tracks) and num >= 1 and len(tracks) >= num:
                        if not tracks[num - 1] or len(title) > len(tracks[num - 1]):
                            tracks[num - 1] = title
                    else:
                        while len(tracks) < num:
                            tracks.append("")
                        if num - 1 < len(tracks):
                            tracks[num - 1] = title
                        else:
                            tracks.append(title)
                break
        else:
            # Kein Track – evtl. Albumtitel (erste längere Zeile ohne führende Zahl)
            if not re.match(r"^\s*\d+[\.\-\s]", line) and len(line) > 2:
                if not album_title and len(line) > 1:
                    album_title = line
                # Auch Zeilen wie "Side A" etc. überspringen
                if re.match(r"^(side\s+[A-D]|seite\s+[A-D])", line, re.I):
                    continue

    # Leere Slots entfernen, fortlaufend nummeriert
    tracks = [t for t in tracks if t]
    return {"album": album_title or "Unbekannt", "tracks": tracks}


def apply_track_names_to_files(
    track_names: List[str],
    file_paths: List[str],
    out_dir: Optional[str] = None,
) -> List[Tuple[str, str]]:  # (path, new_basename)
    """Vorschlag: (Quelldatei, neuer Basisname) für Umbenennung.
    Gibt Liste von (path, neuer_name_ohne_Endung) zurück.
    """
    out_dir = out_dir or (os.path.dirname(file_paths[0]) if file_paths else ".")
    renames = []
    for i, path in enumerate(file_paths):
        base = Path(path).stem
        if i < len(track_names) and track_names[i]:
            safe = re.sub(r'[<>:"/\\|?*]', "_", track_names[i]).strip()[:80]
            new_base = f"{i + 1:02d} {safe}"
        else:
            new_base = f"{i + 1:02d} {base}"
        renames.append((path, new_base))
    return renames


def main():
    parser = argparse.ArgumentParser(description="OCR von Schallplatten-Bildern → Album/Track-Metadaten")
    parser.add_argument("images", nargs="+", help="Bilddatei(en): Cover, Label, …")
    parser.add_argument("--json", action="store_true", help="Nur JSON ausgeben (album + tracks)")
    parser.add_argument("--apply-dir", metavar="DIR", default=None,
                        help="Ordner mit bereits getrennten Track-Dateien (01.xxx, 02.xxx) → Umbenennungsvorschlag")
    parser.add_argument("--dry-run", action="store_true", help="Kein OCR, nur Beispiel-Output bei --json")
    args = parser.parse_args()

    if args.dry_run:
        out = {"album": "Beispiel-Album", "tracks": ["Track Eins", "Track Zwei", "Track Drei"]}
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    try:
        text = run_ocr_on_images(args.images)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    if not text and not args.json:
        print("Kein Text erkannt.", file=sys.stderr)
        sys.exit(0)

    meta = parse_album_and_tracks(text)
    if args.json:
        print(json.dumps(meta, ensure_ascii=False, indent=2))
        return

    print("Album:", meta["album"])
    print("Tracks:")
    for i, t in enumerate(meta["tracks"], 1):
        print(f"  {i:2d}. {t}")

    if args.apply_dir and os.path.isdir(args.apply_dir):
        files = sorted(
            [os.path.join(args.apply_dir, f) for f in os.listdir(args.apply_dir)
             if f.endswith((".mp3", ".wav", ".flac")) and os.path.isfile(os.path.join(args.apply_dir, f))]
        )
        renames = apply_track_names_to_files(meta["tracks"], files, args.apply_dir)
        print("\nUmbenennungsvorschlag:")
        for path, new_base in renames:
            ext = Path(path).suffix
            print(f"  {Path(path).name}  →  {new_base}{ext}")


if __name__ == "__main__":
    main()
