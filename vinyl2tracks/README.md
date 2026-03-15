# vinyl2tracks – Schallplatten-Rips in Einzeltracks

Toolchain zum Aufteilen langer Audio-Aufnahmen (eine MP3/WAV pro Seite oder Platte) in einzelne Tracks per **Stille-Erkennung** und optionalem **OCR** von Cover/Label für Titel und Metadaten.

**Hinweis zu Spleeter:** [Deezer Spleeter](https://github.com/deezer/spleeter) trennt **Quellen** (Gesang vs. Begleitung), nicht eine Aufnahme in Tracks. Die Track-Trennung macht dieses Projekt mit ffmpeg `silencedetect`. Spleeter kann optional nachgelagert genutzt werden (z.B. pro Track Stems erzeugen).

## Ablauf

1. **Track-Split** – `split_by_silence.py`: Eine Datei → viele Tracks (Stille zwischen Titeln).
2. **OCR (optional)** – `ocr_metadata.py`: Bilder von Cover/Label → Albumtitel + Trackliste (z.B. mit DeepSeek OCR auf OpenClaw).
3. **Umbenennen & Taggen** – `rename_tracks.py`: Tracks nach OCR-Ergebnis benennen und ID3/Metadaten setzen.
4. **Spleeter (optional)** – Pro Track Stems (vocals/accompaniment) erzeugen.

## Anforderungen

- **ffmpeg** (inkl. ffprobe)
- **Python 3**
- Optional: OCR (siehe unten)
- Optional: **Spleeter** (`pip install spleeter`) für Stem-Separation

## Verwendung

### Nur Tracks aufteilen

```bash
cd vinyl2tracks
python3 split_by_silence.py /pfad/zur/aufnahme_seite_a.mp3 -o ./ausgabe
# Ausgabe: 01.mp3, 02.mp3, …
```

Parameter:

- `--noise-db` – Stille-Schwelle (z.B. -35)
- `--min-silence` – Mindestdauer Stille in Sekunden (z.B. 2)
- `--min-track` – Kürzeste Track-Länge (Sekunden)
- `--format mp3|wav|copy` – Ausgabeformat
- `--dry-run` – Nur Segmente anzeigen, nicht schneiden

### Mit OCR (Cover/Label → Metadaten)

OCR wird über **OCR_CMD** oder **OCR_URL** konfiguriert.

**Variante: Befehl (z.B. OpenClaw/DeepSeek)**

Der Befehl erhält den **absoluten Bildpfad** als einziges Argument und gibt den erkannten Text auf **stdout** aus.

```bash
# Beispiel: Wrapper-Skript (siehe ocr_openclaw.example.sh)
export OCR_CMD="/pfad/zu/ocr_openclaw.example.sh"
# oder z.B. lokaler Aufruf:
# export OCR_CMD="tesseract"   # falls tesseract stdin/stdout nutzt, ggf. eigenes Wrapper-Skript

python3 ocr_metadata.py cover.jpg label.jpg --json > metadata.json
python3 rename_tracks.py ./ausgabe --meta metadata.json --dry-run   # Vorschau
python3 rename_tracks.py ./ausgabe --meta metadata.json              # ausführen
```

**Variante: HTTP-API**

```bash
export OCR_URL="http://openclaw:8080/ocr"   # POST Body = Bilddatei
python3 ocr_metadata.py cover.jpg --json > metadata.json
```

**OpenClaw:** Wenn DeepSeek OCR auf dem OpenClaw-Rechner läuft, ein kleines Wrapper-Skript bereitstellen, das z.B. per SSH ein Bild übergibt und die OCR-Ausgabe zurückgibt. Siehe `ocr_openclaw.example.sh`.

### Komplette Pipeline

```bash
# Nur Split
./run_pipeline.sh /pfad/zu/seite_a.mp3

# Mit OCR (Bilder im gleichen Ordner wie die MP3 oder absoluter Pfad)
VINYL_OCR_IMAGES="cover.jpg label.jpg" OCR_CMD=/pfad/zu/ocr_wrapper.sh ./run_pipeline.sh seite_a.mp3 ./ausgabe

# Optional Spleeter (langsam, pro Track 2 Stems)
VINYL_USE_SPLEETER=1 ./run_pipeline.sh seite_a.mp3
```

## Konfiguration (Umgebung)

| Variable | Bedeutung |
|----------|-----------|
| `OCR_CMD` / `VINYL_OCR_CMD` | Befehl für OCR; erhält Bildpfad als 1. Argument, liefert Text auf stdout |
| `OCR_URL` / `VINYL_OCR_URL` | URL für OCR-API; POST mit Bild-Body |
| `VINYL_OCR_IMAGES` | Leerzeichen-getrennte Liste von Bildern für die Pipeline (Cover/Label) |
| `VINYL_USE_SPLEETER` | `1` = nach dem Split Spleeter 2stems pro Track ausführen |
| `OPENCLAW_HOST` | Hostname für Beispiel-Wrapper (z.B. `openclaw`) |
| `REMOTE_OCR_CMD` | OCR-Befehl auf OpenClaw im Beispiel-Wrapper |

## Spleeter (optional)

Spleeter trennt z.B. Gesang und Begleitung (2stems). Nützlich, wenn du pro Track nur die Instrumentals oder nur die Vocals brauchst.

```bash
pip install spleeter
spleeter separate -p spleeter:2stems -o output_dir eingabe.mp3
# → output_dir/eingabe/vocals.wav, accompaniment.wav
```

In dieser Toolchain: `VINYL_USE_SPLEETER=1` in `run_pipeline.sh` erzeugt pro getrenntem Track einen Unterordner mit `vocals.wav` und `accompaniment.wav`.

## Dateien

| Datei | Beschreibung |
|-------|--------------|
| `split_by_silence.py` | Track-Split per ffmpeg silencedetect |
| `ocr_metadata.py` | OCR aufrufen, Album/Trackliste parsen, JSON oder Umbenennungsvorschlag |
| `rename_tracks.py` | Tracks aus Ordner + metadata.json umbenennen und ID3 setzen |
| `run_pipeline.sh` | Kombinierte Pipeline (Split → optional OCR → optional Spleeter) |
| `ocr_openclaw.example.sh` | Beispiel-Wrapper für OCR auf OpenClaw (SSH + Remote-Befehl) |
