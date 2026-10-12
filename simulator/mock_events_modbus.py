"""
This script simulates a Modbus TCP device that provides mock data for testing.
Requires the `pymodbus` library. You can install it via pip:
    pip install pymodbus
"""

import math
import random
import time

from pymodbus.client import ModbusTcpClient
from pymodbus.server import StartTcpServer
from pymodbus.simulator import DataType, SimData, SimDevice

HOST = "127.0.0.1"
PORT = 5020
DEVICE_ID = 1

UPDATE_INTERVAL = 1.0


# ---------------------------------------------------------------------------
# Simulated device state
# ---------------------------------------------------------------------------

last_update = 0.0
simulation_time = 0.0

values = {
    "temperature": 25.0,   # °C
    "pressure": 10.0,      # bar
    "power": 1000.0,       # W
    "frequency": 50.0,     # Hz
    "status": 0,           # 0 = OK, 1 = warning
}


# ---------------------------------------------------------------------------
# Generate new device values
# ---------------------------------------------------------------------------

def update_simulation():
    global last_update
    global simulation_time

    now = time.monotonic()

    if now - last_update < UPDATE_INTERVAL:
        return

    last_update = now
    simulation_time += UPDATE_INTERVAL

    values["temperature"] = (
        25.0
        + 3.0 * math.sin(simulation_time / 10.0)
        + random.uniform(-0.2, 0.2)
    )

    values["pressure"] = (
        10.0
        + 0.5 * math.sin(simulation_time / 15.0)
        + random.uniform(-0.05, 0.05)
    )

    values["power"] = (
        1000.0
        + 300.0 * math.sin(simulation_time / 8.0)
        + random.uniform(-30.0, 30.0)
    )

    values["frequency"] = (
        50.0
        + 0.05 * math.sin(simulation_time / 5.0)
        + random.uniform(-0.01, 0.01)
    )

    values["status"] = 1 if random.random() < 0.02 else 0


# ---------------------------------------------------------------------------
# Convert engineering values to Modbus registers
#
# Register layout:
#
#   0: temperature  INT16  / 10
#   1: pressure     UINT16 / 10
#   2: power        UINT16
#   3: frequency    UINT16 / 100
#   4: status       UINT16
# ---------------------------------------------------------------------------

def to_registers():
    temperature = round(values["temperature"] * 10)

    # Signed INT16 is represented as its 16-bit unsigned register value.
    if temperature < 0:
        temperature += 65536

    pressure = round(values["pressure"] * 10)
    power = round(values["power"])
    frequency = round(values["frequency"] * 100)
    status = int(values["status"])

    return [
        temperature,
        pressure,
        power,
        frequency,
        status,
    ]


# ---------------------------------------------------------------------------
# SimDevice action
# ---------------------------------------------------------------------------

async def device_action(
    function_code,
    start_address,
    address,
    count,
    current_registers,
    set_values,
):
    """
    Called by PyModbus when the registers are accessed.

    current_registers contains the registers that will be returned
    to the Modbus client.

    We update the simulated device once per second and modify
    current_registers in-place.
    """

    # Only update on read requests.
    if set_values is None:

        update_simulation()

        registers = to_registers()

        # current_registers corresponds to the requested address range.
        for i in range(len(current_registers)):
            register_index = start_address + i

            if 0 <= register_index < len(registers):
                current_registers[i] = registers[register_index]

    return None


# ---------------------------------------------------------------------------
# Create device
# ---------------------------------------------------------------------------

def create_device():
    """
    Five holding registers at addresses 0..4.

    SimData defines the register space.
    SimDevice represents the Modbus device.
    """

    registers = SimData(
        address=0,
        count=5,
        values=[0, 0, 0, 0, 0],
        datatype=DataType.REGISTERS,
    )

    return SimDevice(
        id=DEVICE_ID,
        simdata=[registers],
        action=device_action,
    )


# ---------------------------------------------------------------------------
# Decode registers
# ---------------------------------------------------------------------------

def decode_registers(registers):
    # INT16 temperature
    temperature_raw = registers[0]

    if temperature_raw >= 32768:
        temperature_raw -= 65536

    temperature = temperature_raw / 10.0

    # UINT16
    pressure = registers[1] / 10.0
    power = float(registers[2])
    frequency = registers[3] / 100.0
    status = registers[4]

    return {
        "temperature": temperature,
        "pressure": pressure,
        "power": power,
        "frequency": frequency,
        "status": status,
    }


# ---------------------------------------------------------------------------
# Modbus client
# ---------------------------------------------------------------------------

def read_device():
    client = ModbusTcpClient(
        HOST,
        port=PORT,
    )

    if not client.connect():
        raise RuntimeError(
            f"Could not connect to {HOST}:{PORT}"
        )

    print(f"[CLIENT] Connected to {HOST}:{PORT}")

    try:
        while True:

            result = client.read_holding_registers(
                address=0,
                count=5,
                device_id=DEVICE_ID,
            )

            if result.isError():
                print(f"[CLIENT] Modbus error: {result}")
                time.sleep(UPDATE_INTERVAL)
                continue

            decoded = decode_registers(result.registers)

            print(
                "[CLIENT] "
                f"temperature={decoded['temperature']:.1f} °C | "
                f"pressure={decoded['pressure']:.1f} bar | "
                f"power={decoded['power']:.0f} W | "
                f"frequency={decoded['frequency']:.2f} Hz | "
                f"status={decoded['status']}"
            )

            time.sleep(UPDATE_INTERVAL)

    finally:
        client.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    device = create_device()

    print(
        f"[SERVER] Starting Modbus TCP device "
        f"{DEVICE_ID} on {HOST}:{PORT}"
    )

    # StartTcpServer blocks, so run it in a background thread.
    import threading

    server_thread = threading.Thread(
        target=StartTcpServer,
        kwargs={
            "context": device,
            "address": (HOST, PORT),
        },
        daemon=True,
    )

    server_thread.start()

    # Give the server time to bind.
    time.sleep(0.5)

    # The main thread acts as the Modbus client.
    read_device()


if __name__ == "__main__":
    main()