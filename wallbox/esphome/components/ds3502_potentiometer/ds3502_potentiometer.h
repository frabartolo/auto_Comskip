#pragma once

#include "esphome/components/i2c/i2c.h"
#include "esphome/core/component.h"
#include "esphome/core/log.h"

namespace esphome {
namespace ds3502_potentiometer {

static const char *const TAG = "ds3502_potentiometer";

// DS3502 register map (Adafruit breakout, address 0x28)
static const uint8_t REG_WIPER = 0x00;
static const uint8_t REG_MODE = 0x02;
// MODE=1: Schreibzugriffe auf 0x00 aendern nur WR, nicht IVR (EEPROM)
static const uint8_t MODE_WRITE_WIPER_ONLY = 0x80;

class DS3502Potentiometer : public Component, public i2c::I2CDevice {
 public:
  void setup() override {
    ESP_LOGCONFIG(TAG, "Setting up DS3502 at 0x%02X...", this->address_);
    if (!this->write_byte(REG_MODE, MODE_WRITE_WIPER_ONLY)) {
      ESP_LOGE(TAG, "DS3502 not reachable / mode write failed");
      this->mark_failed();
      return;
    }
    // Sicherer Start: 0 A bis HA/ESPHome den Sollwert setzt (kein Sprung auf 12 A nach Reboot)
    if (!this->set_wiper(0)) {
      this->mark_failed();
      return;
    }
    ESP_LOGCONFIG(TAG, "DS3502 ready.");
  }

  /// Gleiche Abbildung wie wallbox.yaml: 0..64 A -> 0..127 (ganzzahlig)
  static int amps_to_wiper(int amps) {
    if (amps < 0) {
      amps = 0;
    }
    if (amps > 64) {
      amps = 64;
    }
    return (amps * 127) / 64;
  }

  bool set_wiper(int value) {
    if (value < 0 || value > 127) {
      ESP_LOGW(TAG, "Wiper %d out of range", value);
      return false;
    }
    if (!this->write_byte(REG_WIPER, static_cast<uint8_t>(value))) {
      ESP_LOGW(TAG, "I2C write failed for wiper %d", value);
      return false;
    }
    this->last_wiper_ = value;
    ESP_LOGV(TAG, "Wiper -> %d", value);
    return true;
  }

  int get_wiper_value() const { return this->last_wiper_; }

 protected:
  int last_wiper_{0};
};

}  // namespace ds3502_potentiometer
}  // namespace esphome
