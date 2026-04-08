#!/usr/bin/env bash
#
# auto_process_rsync_gpu.sh — gleicher Ablauf wie auto_process_rsync.sh, mit
# optionaler GPU-Nutzung für den FFmpeg-Encode (cut_with_edl.py).
#
# Erkennung: COMSKIP_VENC=auto (Standard)
#   - NVIDIA: h264_nvenc, wenn nvidia-smi -L funktioniert
#   - sonst VAAPI: h264_vaapi + COMSKIP_VAAPI_DEVICE (Standard /dev/dri/renderD128)
#   - sonst Intel QSV (h264_qsv), sonst libx264
#
# Explizit: COMSKIP_VENC=cpu|nvenc|vaapi|qsv
#
# Weitere Variablen:
#   COMSKIP_NVENC_CQ   (Standard 23)
#   COMSKIP_VAAPI_QP   (Standard 23)
#   COMSKIP_QSV_QUALITY (Standard 23)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RSYNC_MAIN="$SCRIPT_DIR/auto_process_rsync.sh"
PY_CUT="$SCRIPT_DIR/cut_with_edl.py"

if [ ! -f "$RSYNC_MAIN" ]; then
    echo "FEHLER: $RSYNC_MAIN nicht gefunden." >&2
    exit 1
fi
if [ ! -f "$PY_CUT" ]; then
    echo "FEHLER: $PY_CUT nicht gefunden." >&2
    exit 1
fi

usage() {
    sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'
}

case "${1:-}" in
    -h|--help)
        usage
        exit 0
        ;;
    --probe-venc)
        export COMSKIP_VENC="${COMSKIP_VENC:-auto}"
        python3 "$PY_CUT" --probe-venc
        exit 0
        ;;
esac

export PYTHON_SCRIPT="$PY_CUT"
export COMSKIP_VENC="${COMSKIP_VENC:-auto}"
export COMSKIP_VAAPI_DEVICE="${COMSKIP_VAAPI_DEVICE:-/dev/dri/renderD128}"
export COMSKIP_NVENC_CQ="${COMSKIP_NVENC_CQ:-23}"
export COMSKIP_VAAPI_QP="${COMSKIP_VAAPI_QP:-23}"
export COMSKIP_QSV_QUALITY="${COMSKIP_QSV_QUALITY:-23}"

exec bash "$RSYNC_MAIN" "$@"
