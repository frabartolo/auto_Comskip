#!/bin/bash
# Auf Remote-Server ausführen: ./rename_duplicates_remote.sh /path/to/Videos
# Benennt *__*.mkv in *.mkv um, wenn die saubere Version noch nicht existiert.
set -e
find "$1" -type f -name '*__*.mkv' 2>/dev/null | while read -r f; do
  d=$(dirname "$f")
  b=$(basename "$f" .mkv)
  n=$(echo "$b" | sed 's/__.*//')
  [ -f "$d/$n.mkv" ] && continue
  [ "$b" != "$n" ] && mv "$f" "$d/$n.mkv"
  for ext in txt xml srt; do
    [ -f "$d/$b.$ext" ] && [ ! -f "$d/$n.$ext" ] && mv "$d/$b.$ext" "$d/$n.$ext"
  done
done
