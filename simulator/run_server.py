"""
Run only the Modbus TCP server part of the upstream simulator.

`mock_events_modbus.py` is vendored unchanged from
https://github.com/rajat-bhardwaj/mock-modbus-sim (commit dd1712b, MIT).
Upstream binds to 127.0.0.1 and runs a client in the same process; inside
Docker the server must listen on 0.0.0.0 and the client is our collector
service, so we reuse `create_device()` and start the server ourselves.
"""

import logging
import os

from pymodbus.server import StartTcpServer

from mock_events_modbus import create_device

HOST = os.getenv("MODBUS_BIND_HOST", "0.0.0.0")
PORT = int(os.getenv("MODBUS_PORT", "5020"))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s simulator %(message)s")
    logging.info("starting Modbus TCP simulator on %s:%s", HOST, PORT)
    StartTcpServer(context=create_device(), address=(HOST, PORT))


if __name__ == "__main__":
    main()
