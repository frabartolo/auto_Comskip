
#!/usr/bin/env python3
import sys
import os
import subprocess

def cut_video(input_file, edl_file, output_file, srt_file=None, txt_file=None, log_file=None):
    if not os.path.exists(edl_file):
        return

    with open(edl_file, 'r') as f:
        cuts = [line.split() for line in f if line.strip()]

    keep_segments = []
    last_end = 0.0
    for start, end, action in cuts:
        if action == '0':
            if float(start) > last_end:
                keep_segments.append((last_end, float(start)))
            last_end = float(end)
    keep_segments.append((last_end, None))

    v_filters = ""
    a_filters = ""
    for i, (s, e) in enumerate(keep_segments):
        end_str = f":end={e}" if e else ""
        v_filters += f"[0:v]trim=start={s}{end_str},setpts=PTS-STARTPTS[v{i}]; "
        a_filters += f"[0:a]atrim=start={s}{end_str},asetpts=PTS-STARTPTS[a{i}]; "

    concat_inputs = "".join([f"[v{i}][a{i}]" for i in range(len(keep_segments))])
    filter_complex = f"{v_filters}{a_filters}{concat_inputs}concat=n={len(keep_segments)}:v=1:a=1[outv][outa]"

    cmd = ["ffmpeg", "-i", input_file]

    if srt_file and os.path.exists(srt_file):
        cmd.extend(["-i", srt_file])
        has_srt = True
    else:
        has_srt = False

    cmd.extend(["-filter_complex", filter_complex, "-map", "[outv]", "-map", "[outa]"])

    if has_srt:
        cmd.extend(["-map", "1:0", "-c:s", "srt", "-metadata:s:s:0", "language=ger"])

    if txt_file and os.path.exists(txt_file):
        try:
            with open(txt_file, 'r', encoding='utf-8', errors='ignore') as f:
                desc = f.read().strip()
                cmd.extend(["-metadata", f"comment={desc}", "-metadata", f"description={desc}"])
        except Exception: pass

    cmd.extend(["-c:v", "libx264", "-crf", "21", "-preset", "faster", "-c:a", "aac", "-b:a", "192k", "-y", output_file])

    # Log-Handling: FFmpeg Output in Datei umleiten
    if log_file:
        with open(log_file, "a") as f_log:
            f_log.write(f"\n--- FFmpeg Start für {input_file} ---\n")
            subprocess.run(cmd, stdout=f_log, stderr=f_log)
    else:
        subprocess.run(cmd)

if __name__ == "__main__":
    # Aufruf: script.py video edl output srt txt log
    v, e, o, s, t, l = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6]
    cut_video(v, e, o, s, t, l)
