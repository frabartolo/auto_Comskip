#!/bin/bash
# Beispiel-Wrapper: OCR auf OpenClaw (DeepSeek) aufrufen.
# Nutzung: OCR_CMD=/pfad/zu/ocr_openclaw.sh python3 ocr_metadata.py cover.jpg label.jpg
#
# Variante A: Bild per SCP rüberkopieren, OCR auf OpenClaw ausführen, Text zurück
# Variante B: Gemeinsamer Mount – Bildpfad ist auf OpenClaw sichtbar
# Variante C: Lokaler DeepSeek-OCR (wenn auf diesem Rechner installiert)
#
# Setze OPENCLAW_HOST und ggf. REMOTE_OCR_CMD (z.B. "deepseek-ocr" oder "python3 /opt/ocr/run.py")

OPENCLAW_HOST="${OPENCLAW_HOST:-openclaw}"
REMOTE_OCR_CMD="${REMOTE_OCR_CMD:-deepseek-ocr}"
IMAGE="$1"
if [ -z "$IMAGE" ] || [ ! -f "$IMAGE" ]; then
  echo "Usage: $0 <image_path>" >&2
  exit 1
fi

# Beispiel: Bild temporär kopieren, auf OpenClaw OCR ausführen
TMP="/tmp/vinyl_ocr_$$"
scp -q "$IMAGE" "$OPENCLAW_HOST:$TMP" || exit 1
ssh "$OPENCLAW_HOST" "$REMOTE_OCR_CMD $TMP; rm -f $TMP" 2>/dev/null || ssh "$OPENCLAW_HOST" "rm -f $TMP; exit 1"
