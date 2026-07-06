#!/usr/bin/env python3
"""Konsolidiert process_summary.log: entfernt Comskip/FFmpeg-Rauschen, behält Wesentliches."""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime

TS = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\] (.*)$")
COMSKIP_PROGRESS = re.compile(r"frames in .* fps")
SKIP_LINE = re.compile(
    r"^(Comskip |Donator build|Setting ini |Using .* for initiation|"
    r"\[mpeg2video @|Invalid frame dimensions|Last message repeated|"
    r"Extracting segment |\t--|^\s+comskip)",
    re.I,
)


def keep_timestamped(msg: str) -> bool:
    if msg.startswith("="):
        return False
    if any(
        x in msg
        for x in (
            "Mount-Prüfung",
            "System-Info",
            "Quell-Server (",
            "Ziel-Server (",
            "Quell-Mount:",
            "Ziel-Mount:",
            "RAM:",
            "Schritt 1:",
            "Schritt 2:",
            "Schritt 4:",
            "Schritt 5:",
            "  -> Schritt 4 nutzt",
            "  -> Untertitel gefunden",
            "  -> TXT-Metadaten",
            "  -> XML-Metadaten",
            "  ✓ ffprobe OK",
            "  ✓ Untertitel kopiert",
            "  ✓ TXT kopiert",
            "  ✓ XML kopiert",
            "  -> Bereits in Bearbeitung von:",
            "Lade Blacklist",
            "Hole Dateiliste",
            "Start-Cleanup",
            "  [cleanup]",
        )
    ):
        return False
    if msg.startswith("  -> ") and "Reparatur" in msg:
        return False
    return any(
        pat in msg
        for pat in (
            "Verarbeite:",
            "✓ Erfolgreich verarbeitet",
            "✓ Video verarbeitet",
            "✗ ",
            "STATISTIK",
            "Start Verarbeitung:",
            "Ende:",
            "Video-Dateien gefunden:",
            "Gefunden:",
            "Worker-ID:",
            "Überspringe",
            "Umbenennen:",
            "Gesichert unter:",
            "als dirty",
            "Segfault",
            "Prüfe Umbenennung",
            "Keine Dateien zu verarbeiten",
            "FEHLER:",
            "⚠ ",
        )
    )


def keep_plain(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if COMSKIP_PROGRESS.search(s) or SKIP_LINE.search(s):
        return False
    if s.startswith("Commercials were found"):
        return False
    if s.startswith("=== Using concat"):
        return False
    return s.startswith(
        ("File size:", "Method:", "[ENCODE]", "=== FFmpeg Exit Code:")
    ) and not s.startswith("[ENCODE] FFmpeg cmd:")


def consolidate(in_path: str, out_path: str) -> tuple[int, int]:
    kept = total = 0
    header = (
        f"# Konsolidiert am {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
        f"aus {os.path.basename(in_path)}\n"
        f"# Enthält: Verarbeite/Ergebnis/Fehler/Statistik/Überspringe, "
        f"FFmpeg-Methode + Exit-Code\n"
    )
    with open(in_path, encoding="utf-8", errors="ignore") as fin, open(
        out_path, "w", encoding="utf-8"
    ) as fout:
        fout.write(header)
        for line in fin:
            total += 1
            line = line.rstrip("\n\r")
            m = TS.match(line)
            if m:
                if keep_timestamped(m.group(2)):
                    fout.write(line + "\n")
                    kept += 1
            elif keep_plain(line):
                fout.write(line + "\n")
                kept += 1
    return kept, total


def main() -> int:
    p = argparse.ArgumentParser(description="process_summary.log konsolidieren")
    p.add_argument("log_file", help="Pfad zu process_summary.log")
    p.add_argument(
        "-o",
        "--output",
        help="Ausgabedatei (Default: <log>.consolidated, dann ersetzen)",
    )
    p.add_argument(
        "--in-place",
        action="store_true",
        help="Original sichern als .bak und durch konsolidierte Version ersetzen",
    )
    args = p.parse_args()

    if not os.path.isfile(args.log_file):
        print(f"FEHLER: {args.log_file} nicht gefunden", file=sys.stderr)
        return 1

    out = args.output or args.log_file + ".consolidated"
    kept, total = consolidate(args.log_file, out)
    print(f"Zeilen: {kept:,} behalten von {total:,} ({100 * kept / max(total, 1):.1f}%)")
    print(f"Geschrieben: {out}")

    if args.in_place:
        bak = args.log_file + ".bak"
        if os.path.exists(bak):
            os.remove(bak)
        os.rename(args.log_file, bak)
        os.rename(out, args.log_file)
        print(f"Original: {bak}")
        print(f"Ersetzt:  {args.log_file}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
