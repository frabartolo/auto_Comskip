// Externes Sensorköpfchen für BME280 (Pimoroni/Adafruit-Breakout)
// Der D1 Mini Pro sitzt im fertigen DIN-Halter:
// https://www.printables.com/model/1530436-d1mini-din-rail-mount
//
// STL: openscad -o bme_sensor_pod.stl bme_sensor_pod.scad

bme_l = 18.0;
bme_w = 14.0;
bme_h = 8.0;
bme_cable_d = 5.5;
pod_wall = 1.8;

$fn = 32;

module ventilation_slots(x, y, w, l, count = 5) {
  slot_w = 1.2;
  gap = (l - count * slot_w) / (count + 1);
  for (i = [0 : count - 1]) {
    translate([x + gap + i * (slot_w + gap), y, -0.1])
      cube([slot_w, w, pod_wall + 0.4]);
  }
}

difference() {
  union() {
    cube([bme_l + 2 * pod_wall, bme_w + 2 * pod_wall, bme_h + pod_wall]);
    translate([pod_wall + 3, bme_w / 2 + pod_wall, bme_h / 2 + pod_wall])
      rotate([0, 90, 0])
        cylinder(h = 8, d = bme_cable_d + 1.5, center = true, $fn = 32);
  }
  translate([pod_wall, pod_wall, pod_wall])
    cube([bme_l, bme_w, bme_h + 0.2]);
  ventilation_slots(pod_wall + 2, pod_wall + 2, bme_w - 4, bme_l - 4, 4);
  translate([-0.1, bme_w / 2 + pod_wall, bme_h / 2 + pod_wall])
    rotate([0, 90, 0])
      cylinder(h = pod_wall + 0.2, d = bme_cable_d, $fn = 32);
}

translate([bme_l / 2 + pod_wall, -0.1, bme_h / 2 + pod_wall])
  rotate([-90, 0, 0])
    cylinder(h = pod_wall + 0.2, d = 3.2, $fn = 24);
