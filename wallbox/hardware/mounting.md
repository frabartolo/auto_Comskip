# Montage im Wallbox-Schaltkasten

## Komponenten

| Teil | Funktion | Befestigung |
|------|----------|-------------|
| D1 Mini Pro | ESP8266, WiFi, Antenne | [DIN-Halter](https://www.printables.com/model/1530436-d1mini-din-rail-mount) |
| DS3502-Breakout | Ladestrom-Ansteuerung | Jumper zum D1, Kabel zur Wallbox |
| BME280-Breakout | Temperatur, Druck, Feuchte | Externes [Sensorköpfchen](3d-print/bme_sensor_pod.scad) |

## Verdrahtung I2C

| Breakout | SDA | SCL | Adresse |
|----------|-----|-----|---------|
| DS3502 (Wallbox) | **D1** | **D2** | 0x28 |
| BME280 (Wetter) | **D3** | **D4** | 0x76 |

Gemeinsame **3,3 V** und **GND** pro Breakout. SDA/SCL nicht vertauschen.

## Firmware

Siehe [`../esphome/`](../esphome/):

- **`wb-mit-wetter.yaml`** – Kombi-Gerät (Produktion am D1 Mini Pro)
- **`wallbox.yaml`** – nur Wallbox (separater ESP, falls noch vorhanden)
- **`wetterstation.yaml`** – nur BME280 (Legacy)

Build und Flash über ESPHome Device Builder auf wilhelmshome (`/config/esphome/`).

## BME280 extern

1. BME280 in gedrucktes Sensorköpfchen setzen
2. 4-adriges Kabel durch Kabeldurchführung im Schaltkasten
3. Pod **außerhalb** montieren (freie Luft, nicht über Wärmequellen)
4. Kabel zum D1 (D3/D4)

## Stromversorgung

- Micro-USB am D1 (5 V) oder
- 5 V aus Schaltkasten über DIN-Halter-Deckelöffnung

## Maße D1 Mini Pro (gemessen)

34,5 × 25,6 mm (Platinenabmessung)
