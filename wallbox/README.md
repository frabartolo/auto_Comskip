# Wallbox – ESPHome & Hardware

Steuerung der Wallbox-Ladung (DS3502) und Umgebungssensorik (BME280) per ESP8266, angebunden an Home Assistant.

## Struktur

```
wallbox/
├── README.md                 ← dieses Dokument
├── esphome/                  ← Firmware (ESPHome Device Builder, wilhelmshome:/config/esphome/)
│   ├── wb-mit-wetter.yaml    ← Kombi: Wallbox + Wetter (D1 Mini Pro)
│   ├── wallbox.yaml          ← nur Wallbox
│   ├── wetterstation.yaml    ← nur BME280 (Legacy)
│   └── components/
│       └── ds3502_potentiometer/   ← nativer I2C-Treiber DS3502
└── hardware/
    ├── mounting.md           ← Verdrahtung, Hutschiene, externer BME
    └── 3d-print/README.md    ← Verweis auf ~/Workspace/Wallbox/hardware/3d-print/
```

3D-Druck (BME-Gehäuse, STL): [`~/Workspace/Wallbox`](~/Workspace/Wallbox) → `hardware/3d-print/BME_Sensor_Pod/`

## Deployment

**Produktion:** Home Assistant `wilhelmshome`, ESPHome unter `/config/esphome/`.

Lokale Kopie in `esphome/` dient als Spiegel / Versionskontrolle (Git-Repo auf dem HA-Host).

```bash
# Beispiel: geänderte Datei auf wilhelmshome kopieren
scp esphome/wb-mit-wetter.yaml root@wilhelmshome:/config/esphome/
```

`secrets.yaml` liegt nur auf dem HA-Host (nicht im Repo).

## Geräte

| YAML | ESP | Status |
|------|-----|--------|
| `wb-mit-wetter.yaml` | D1 Mini Pro (Kombi) | **aktiv** |
| `wallbox.yaml` | D1 Mini (Wallbox allein) | Legacy / Ersatz |
| `wetterstation.yaml` | D1 Mini Lite | Legacy |

## Hardware

- D1 Mini Pro auf **Hutschiene**: [Printables DIN mount (Basti)](https://www.printables.com/model/1530436-d1mini-din-rail-mount)
- BME280 **extern**: `~/Workspace/Wallbox/hardware/3d-print/BME_Sensor_Pod/bme_sensor_pod.stl`
- Details: [`hardware/mounting.md`](hardware/mounting.md)

## Home Assistant

Automationen und Regelung: `packages/wallbox.yaml` auf wilhelmshome (nicht in diesem Ordner).
