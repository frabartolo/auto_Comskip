import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.components import i2c
from esphome.const import CONF_ADDRESS, CONF_ID

DEPENDENCIES = ["i2c"]
MULTI_CONF = True

ds3502_potentiometer_ns = cg.esphome_ns.namespace("ds3502_potentiometer")
DS3502Potentiometer = ds3502_potentiometer_ns.class_(
    "DS3502Potentiometer", cg.Component, i2c.I2CDevice
)

CONFIG_SCHEMA = (
    cv.Schema(
        {
            cv.GenerateID(): cv.declare_id(DS3502Potentiometer),
            cv.Optional(CONF_ADDRESS, default=0x28): cv.i2c_address,
        }
    )
    .extend(cv.COMPONENT_SCHEMA)
    .extend(i2c.i2c_device_schema(0x28))
)


async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)
    await i2c.register_i2c_device(var, config)
