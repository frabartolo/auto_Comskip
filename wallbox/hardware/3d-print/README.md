# 3D-Druck – Wallbox-Elektronik

## D1 Mini Pro auf Hutschiene

Fertiges Modell (empfohlen):

**[D1mini DIN Rail mount – Basti](https://www.printables.com/model/1530436-d1mini-din-rail-mount)** (Printables)

- Variante **D1 mini Pro** wählen (Antenne + USB)
- Material: **PETG**, 0,2 mm, 3–4 Perimeter
- Stromversorgung per USB oder 5 V über Deckelöffnung

## Externer BME280-Sensor

Datei: `bme_sensor_pod.scad`

Der BME280 sitzt **außerhalb** des Schaltkastens, damit Temperatur/Luftfeuchte die Umgebung widerspiegeln.

```bash
openscad -o bme_sensor_pod.stl bme_sensor_pod.scad
```

- Material: PETG
- Schlitze nach oben drucken
- Montage: M3-Schraube oder Klett außerhalb des Kastens
- 4-adriges Kabel zum D1 (Bus `bus_weather`, D3/D4)

## Optional: Volles Kombi-Gehäuse

Falls du später D1 + DS3502 in **einem** Gehäuse ohne fertigen DIN-Halter drucken willst, liegt ein älterer Entwurf im Git-Verlauf (`wallbox_enclosure.scad`, archiviert). Aktuell nicht nötig.
