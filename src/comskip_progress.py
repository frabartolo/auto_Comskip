#!/usr/bin/env python3
"""Dateibasierter Fortschritt: Quelle vs. Ziel, Blacklist, Fehler aus Log."""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Dict, Set, Tuple

VIDEO_EXTS = {
    ".mp4", ".m4v", ".mkv", ".ts", ".mpeg", ".mpg", ".mov", ".webm",
    ".asf", ".wmv", ".avi", ".divx",
}


def clean_name(name: str) -> str:
    return re.sub(r"__.*", "", name)


def iter_source_videos(source_root: str) -> Dict[str, Tuple[str, str, str]]:
    """rel_key -> (rel_dir, basename_without_ext, ext)."""
    out: Dict[str, Tuple[str, str, str]] = {}
    for root, _, files in os.walk(source_root):
        for fname in files:
            base, ext = os.path.splitext(fname)
            if ext.lower() not in VIDEO_EXTS:
                continue
            rel_dir = os.path.relpath(root, source_root)
            if rel_dir == ".":
                rel_dir = ""
            rel_key = os.path.join(rel_dir, fname) if rel_dir else fname
            out[rel_key] = (rel_dir, base, ext.lower())
    return out


def load_target_mkv_keys(target_root: str) -> Set[Tuple[str, str]]:
    keys: Set[Tuple[str, str]] = set()
    for root, _, files in os.walk(target_root):
        for fname in files:
            if not fname.lower().endswith(".mkv"):
                continue
            rel_dir = os.path.relpath(root, target_root)
            if rel_dir == ".":
                rel_dir = ""
            base = os.path.splitext(fname)[0]
            keys.add((rel_dir, base))
    return keys


def load_blacklist(path: str) -> Set[str]:
    if not path or not os.path.isfile(path):
        return set()
    names: Set[str] = set()
    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            name = line.strip()
            if name:
                names.add(name)
    return names


def source_on_target(rel_dir: str, base: str, target_keys: Set[Tuple[str, str]]) -> bool:
    if (rel_dir, base) in target_keys:
        return True
    if (rel_dir, clean_name(base)) in target_keys:
        return True
    return False


def parse_log_failures(log_path: str) -> Set[str]:
    """Eindeutige Dateinamen mit Fehler im Log (ohne Erfolg danach pro Datei)."""
    if not log_path or not os.path.isfile(log_path):
        return set()

    current: str | None = None
    last_error: Dict[str, str] = {}
    succeeded: Set[str] = set()

    with open(log_path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = re.search(r"Verarbeite: (.+)$", line)
            if m:
                current = os.path.basename(m.group(1).strip())
                continue
            if current and (
                "✗ Fehler (Exit:" in line
                or "✗ Datei ist auf Blacklist" in line
                or "als dirty markiert" in line
                or "Segfault" in line
            ):
                last_error[current] = line.strip()
            if "✓ Video verarbeitet" in line or "✓ Erfolgreich verarbeitet" in line:
                if current:
                    succeeded.add(current)
                current = None

    failed = set(last_error.keys()) - succeeded
    return failed


def main() -> int:
    p = argparse.ArgumentParser(description="Comskip-Fortschritt (dateibasiert)")
    p.add_argument("--source", required=True, help="Quell-Mount")
    p.add_argument("--target", required=True, help="Ziel-Mount")
    p.add_argument("--blacklist", default="", help="Pfad corrupted_files.blacklist")
    p.add_argument("--log", default="", help="process_summary.log")
    args = p.parse_args()

    if not os.path.isdir(args.source):
        print("ERROR=Quell-Mount nicht erreichbar", file=sys.stderr)
        return 1
    if not os.path.isdir(args.target):
        print("ERROR=Ziel-Mount nicht erreichbar", file=sys.stderr)
        return 1

    sources = iter_source_videos(args.source)
    target_keys = load_target_mkv_keys(args.target)
    blacklist_names = load_blacklist(args.blacklist)
    log_failed_names = parse_log_failures(args.log)

    total = len(sources)
    done = 0
    blacklisted = 0
    failed = 0
    open_count = 0

    for rel_key, (rel_dir, base, ext) in sources.items():
        fname = f"{base}{ext}"
        on_target = source_on_target(rel_dir, base, target_keys)
        in_bl = fname in blacklist_names or rel_key in blacklist_names
        in_log_fail = fname in log_failed_names

        if on_target:
            done += 1
        elif in_bl:
            blacklisted += 1
        elif in_log_fail:
            failed += 1
        else:
            open_count += 1

    mkv_total = sum(
        1
        for _root, _dirs, files in os.walk(args.target)
        for f in files
        if f.lower().endswith(".mkv")
    )
    mkv_unrenamed = sum(
        1
        for _root, _dirs, files in os.walk(args.target)
        for f in files
        if f.lower().endswith(".mkv") and "__" in os.path.splitext(f)[0]
    )

    handled = done + blacklisted + failed
    accounted = handled + open_count
    pct_done = (done * 100 // total) if total else 0
    pct_handled = (handled * 100 // total) if total else 0

    print(f"TOTAL={total}")
    print(f"DONE={done}")
    print(f"BLACKLIST={blacklisted}")
    print(f"FAILED={failed}")
    print(f"HANDLED={handled}")
    print(f"OPEN={open_count}")
    print(f"MKV_ON_TARGET={mkv_total}")
    print(f"MKV_UNRENAMED={mkv_unrenamed}")
    print(f"PCT_DONE={pct_done}")
    print(f"PCT_HANDLED={pct_handled}")
    print(f"PCT_NO_LONGER_PENDING={pct_handled}")
    print(f"ACCOUNTED={accounted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
