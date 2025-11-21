#!/usr/bin/env python3
"""
heater_slower_socket.py

UNIX-socket daemon for HCalory heater control.
- Polling runs internally and updates heater.heater_response each interval.
- CLI connects to socket: /tmp/hcalory-control-<mac>.sock
- Debug mode prints fragments and polling responses.
- Adds cache age tracking and pump_data_force behavior.
"""
from __future__ import annotations

import argparse
import asyncio
import enum
import json
import logging
import os
import signal
import sys
import time
from typing import Any

import bleak
import bleak_retry_connector

# --- Logging setup ---
logger = logging.getLogger("hcalory-control")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

# --- Protocol constants ---
command_header = bytes.fromhex("000200010001000e040000090000000000000000")


class ListableEnum(enum.Enum):
    @classmethod
    def list(cls) -> list[str]:
        return list(cls.__members__.keys())


class Command(bytes, ListableEnum):
    stop_heat = command_header + bytes.fromhex("010e")
    start_heat = command_header + bytes.fromhex("020f")
    up = command_header + bytes.fromhex("0310")
    down = command_header + bytes.fromhex("0411")
    gear = command_header + bytes.fromhex("0714")
    thermostat = command_header + bytes.fromhex("0613")
    pump_data = command_header + bytes.fromhex("000d")


class HeaterState(int, ListableEnum):
    off = 0
    cooldown = 65
    cooldown_starting = 67
    cooldown_received = 69
    ignition_received = 128
    ignition_starting = 129
    igniting = 131
    running = 133
    heating = 135
    error = 255


class HeaterMode(int, ListableEnum):
    off = 0
    thermostat = 1
    gear = 2
    ignition_failed = 8
    unknown = 92


class HeaterResponse:
    def __init__(self, raw: bytes):
        # Expect at least 40 bytes (indexes 0..39)
        # Parsing identical to your working code
        self._header = raw[0:20]
        self.heater_state = HeaterState(raw[20])
        try:
            self.heater_mode = HeaterMode(raw[21])
        except Exception:
            self.heater_mode = HeaterMode.unknown
        self.heater_setting = raw[22]
        self._mystery = raw[23]
        self._voltage = raw[25]
        self._body_temperature = raw[27:29]
        self._ambient_temperature = raw[30:32]
        self._end_junk = raw[33:40]

    @property
    def voltage(self) -> int:
        return self._voltage // 10

    @property
    def body_temperature(self) -> int:
        return int(self._body_temperature.hex(), 16) // 10

    @property
    def ambient_temperature(self) -> int:
        return int(self._ambient_temperature.hex(), 16) // 10

    @property
    def cooldown(self) -> bool:
        return self.heater_state in (
            HeaterState.cooldown_received,
            HeaterState.cooldown_starting,
            HeaterState.cooldown,
        )

    @property
    def preheating(self) -> bool:
        return self.heater_state in (
            HeaterState.ignition_received,
            HeaterState.ignition_starting,
            HeaterState.igniting,
            HeaterState.heating,
        )

    @property
    def running(self) -> bool:
        return self.heater_state in (
            HeaterState.ignition_received,
            HeaterState.ignition_starting,
            HeaterState.igniting,
            HeaterState.running,
            HeaterState.heating,
        )

    def asdict(self) -> dict[str, Any]:
        return {
            "heater_state": self.heater_state.name,
            "heater_mode": self.heater_mode.name,
            "heater_setting": self.heater_setting,
            "voltage": self.voltage,
            "body_temperature": self.body_temperature,
            "ambient_temperature": self.ambient_temperature,
            "running": self.running,
            "cooldown": self.cooldown,
            "preheating": self.preheating,
        }


# --- HCaloryHeater class (kept behavior from working code) ---
class HCaloryHeater:
    write_characteristic_id = "0000fff2-0000-1000-8000-00805f9b34fb"
    read_characteristic_id = "0000fff1-0000-1000-8000-00805f9b34fb"

    def __init__(self, address: str, bluetooth_timeout: float = 30.0, max_bluetooth_retry_attempts: int = 20):
        self.address = address
        self.bluetooth_timeout = bluetooth_timeout
        self.max_bluetooth_retry_attempts = max_bluetooth_retry_attempts
        self.bleak_client: bleak.BleakClient | None = None
        self._write_characteristic: bleak.BleakGATTCharacteristic | None = None
        self._read_characteristic: bleak.BleakGATTCharacteristic | None = None
        # queue to receive notifications (keeps original working behavior)
        self._data_queue: asyncio.Queue[bytearray] = asyncio.Queue()
        self._connect_lock = asyncio.Lock()
        self._command_lock = asyncio.Lock()
        self._intentional_disconnect = False
        # last response cached by poll loop or _data_handler
        self.heater_response: HeaterResponse | None = None
        # last update timestamp (epoch seconds)
        self._last_update: float = 0.0

    async def _ensure_connection(self, reason: str = "") -> None:
        # identical to your working code (keeps behavior)
        if self.bleak_client and self.bleak_client.is_connected:
            return  # already connected

        async with self._connect_lock:
            if self.bleak_client and self.bleak_client.is_connected:
                return
            logger.info("Connecting to heater %s %s", self.address, reason)
            device = await bleak.BleakScanner.find_device_by_address(self.address, timeout=10.0)
            if device is None:
                raise RuntimeError(f"Heater {self.address} not found")

            try:
                self.bleak_client = await bleak_retry_connector.establish_connection(
                    bleak.BleakClient,
                    device,
                    self.address,
                    self.handle_disconnect,
                    use_services_cache=True,
                    max_attempts=self.max_bluetooth_retry_attempts,
                    timeout=self.bluetooth_timeout,
                )
                await self.bleak_client.start_notify(self.read_characteristic, self._data_handler)
                logger.info("Connected to heater %s", self.address)
            except Exception as e:
                logger.warning("BLE connect failed: %s", e)
                raise

    @property
    def read_characteristic(self) -> bleak.BleakGATTCharacteristic:
        if self._read_characteristic is None:
            assert self.bleak_client is not None
            self._read_characteristic = self.bleak_client.services.get_characteristic(self.read_characteristic_id)
        return self._read_characteristic

    @property
    def write_characteristic(self) -> bleak.BleakGATTCharacteristic:
        if self._write_characteristic is None:
            assert self.bleak_client is not None
            self._write_characteristic = self.bleak_client.services.get_characteristic(self.write_characteristic_id)
        return self._write_characteristic

    def handle_disconnect(self, client: bleak.BleakClient) -> None:
        if not self._intentional_disconnect:
            logger.warning("Encountered unintentional disconnect from %s", self.address)
        self._read_characteristic = None
        self._write_characteristic = None

    async def _data_handler(self, _: bleak.BleakGATTCharacteristic, data: bytearray):
        # queue notifications for consumers and update cached last response if parsable
        # Log fragment in debug mode
        logger.debug("Received fragment (len=%d): %s", len(data), data.hex())
        await self._data_queue.put(data)
        # try parsing and updating cache (defensive)
        try:
            resp = HeaterResponse(bytes(data))
            self.heater_response = resp
            self._last_update = time.time()
        except Exception:
            # ignore parse errors for non-standard notifications
            pass

    async def send_command(self, cmd: Command):
        async with self._command_lock:
            await self._ensure_connection(f"(send {cmd.name})")
            await self.bleak_client.write_gatt_char(self.write_characteristic, cmd)

    # convenience command methods used by socket handler mapping
    async def start_heat(self):
        await self.send_command(Command.start_heat)

    async def stop_heat(self):
        await self.send_command(Command.stop_heat)

    async def change_setting_up(self):
        await self.send_command(Command.up)

    async def change_setting_down(self):
        await self.send_command(Command.down)

    async def set_gear_mode(self):
        await self.send_command(Command.gear)

    async def set_thermostat_mode(self):
        await self.send_command(Command.thermostat)

    async def get_data(self) -> HeaterResponse:
        # send pump_data and read one notification from queue (original working behavior)
        await self.send_command(Command.pump_data)
        raw = await self._data_queue.get()
        resp = HeaterResponse(raw)
        self.heater_response = resp
        self._last_update = time.time()
        return resp

    async def disconnect(self):
        self._intentional_disconnect = True
        if self.bleak_client:
            try:
                await self.bleak_client.disconnect()
            except Exception as e:
                logger.debug("Disconnect error: %s", e)
            self.bleak_client = None
            self._read_characteristic = None
            self._write_characteristic = None


# --- Socket helpers ---
def socket_path_for_address(address: str) -> str:
    clean = address.lower().replace(":", "_")
    return f"/tmp/hcalory-control-{clean}.sock"


# --- Daemon: polling + unix socket server ---
async def run_daemon(address: str, interval: float, debug: bool, max_age: float):
    socket_path = socket_path_for_address(address)

    # ensure no leftover socket
    if os.path.exists(socket_path):
        try:
            os.remove(socket_path)
        except OSError:
            logger.debug("Could not remove existing socket %s", socket_path)

    heater = HCaloryHeater(address)

    # try initial connect (will raise if not found)
    try:
        await heater._ensure_connection("daemon initial connect")
    except Exception as e:
        logger.warning("Initial connect failed: %s (daemon will still start and retry on demand)", e)

    # polling task (updates heater.heater_response)
    async def poll_loop():
        while True:
            try:
                resp = None
                try:
                    resp = await heater.get_data()
                except Exception as e:
                    logger.debug("Polling get_data error: %s", e)
                if debug and resp:
                    # only print if debug is enabled
                    print(json.dumps(resp.asdict(), indent=4, sort_keys=True))
            except Exception as e:
                logger.warning("Unexpected error in poll loop: %s", e)
            await asyncio.sleep(interval)

    poll_task = asyncio.create_task(poll_loop())

    # socket server handler uses same heater instance (no new connections)
    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            data = await reader.read(1024)
            if not data:
                writer.close()
                await writer.wait_closed()
                return
            cmd = data.decode().strip()
            logger.debug("Socket received command: %s", cmd)

            # Accept both regular commands and 'pump_data_force' special-case
            if cmd not in Command.list() and cmd != "pump_data_force":
                writer.write(f"ERROR: unknown command: {cmd}\n".encode())
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            # process commands using the existing heater object
            try:
                if cmd in ("pump_data", "pump_data_force"):
                    force = (cmd == "pump_data_force")
                    now = time.time()
                    have_cache = heater.heater_response is not None
                    age = (now - heater._last_update) if have_cache else None

                    if not force and have_cache and age is not None and age <= max_age:
                        # cache is fresh enough -> return it
                        out = heater.heater_response.asdict()
                    else:
                        # either forced or cache missing/stale -> try to get fresh data
                        try:
                            resp = await heater.get_data()
                            out = resp.asdict()
                        except Exception as e:
                            # if cannot fetch fresh, but we have stale cache -> return it with warning
                            if have_cache:
                                logger.warning("Failed to refresh pump_data (age=%.1fs): %s — returning stale cache", age if age else 9999, e)
                                out = heater.heater_response.asdict()
                            else:
                                writer.write(f"ERROR: failed to get data: {e}\n".encode())
                                await writer.drain()
                                writer.close()
                                await writer.wait_closed()
                                return
                    writer.write((json.dumps(out) + "\n").encode())
                else:
                    # mapping of names to methods (keeps same API)
                    mapping = {
                        "start_heat": heater.start_heat,
                        "stop_heat": heater.stop_heat,
                        "up": heater.change_setting_up,
                        "down": heater.change_setting_down,
                        "gear": heater.set_gear_mode,
                        "thermostat": heater.set_thermostat_mode,
                    }
                    func = mapping[cmd]
                    await func()
                    writer.write(f"OK: {cmd}\n".encode())
            except Exception as e:
                logger.exception("Error executing command %s", cmd)
                writer.write(f"ERROR: {e}\n".encode())

            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    # create unix server
    server = await asyncio.start_unix_server(handle_client, path=socket_path)
    logger.info("Daemon listening on %s", socket_path)

    # graceful shutdown handling
    stop = asyncio.Event()

    def _stop(*_):
        stop.set()

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, _stop)
        except NotImplementedError:
            # not available on Windows
            pass

    async with server:
        await stop.wait()
        logger.info("Daemon shutting down...")
        server.close()
        await server.wait_closed()
        poll_task.cancel()
        try:
            await poll_task
        except asyncio.CancelledError:
            pass
        await heater.disconnect()
        # remove socket
        try:
            if os.path.exists(socket_path):
                os.remove(socket_path)
        except Exception:
            pass
        logger.info("Daemon stopped.")


# --- CLI client that talks to the daemon socket ---
async def cli_send(address: str, command: str) -> int:
    socket_path = socket_path_for_address(address)
    if not os.path.exists(socket_path):
        print("ERROR: daemon not running (socket not found)", file=sys.stderr)
        return 2
    try:
        reader, writer = await asyncio.open_unix_connection(socket_path)
    except Exception as e:
        print(f"ERROR: cannot connect to daemon: {e}", file=sys.stderr)
        return 2
    try:
        writer.write((command + "\n").encode())
        await writer.drain()
        resp = await reader.read(-1)
        if not resp:
            print("ERROR: empty response", file=sys.stderr)
            return 2
        print(resp.decode().strip())
        return 0
    finally:
        writer.close()
        await writer.wait_closed()


# --- main ---
def parse_args():
    p = argparse.ArgumentParser(prog="hcalory-control")
    p.add_argument("--address", required=True, help="Bluetooth MAC address of heater")
    p.add_argument("--daemon", action="store_true", help="Run as daemon and poll data continuously")
    p.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds")
    p.add_argument("--debug", action="store_true", help="Enable debug printing for polling and fragments")
    p.add_argument("--max-age", type=float, default=5.0, help="Maximum allowed cache age in seconds before forcing refresh")
    p.add_argument("command", nargs="?", choices=Command.list(), help="Command to send (CLI mode)")
    return p.parse_args()


def main():
    args = parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    if args.daemon:
        try:
            asyncio.run(run_daemon(args.address, args.interval, args.debug, args.max_age))
        except KeyboardInterrupt:
            pass
        return

    # CLI mode -> forward to daemon via socket
    if not args.command:
        print("ERROR: command required in CLI mode", file=sys.stderr)
        sys.exit(2)
    code = asyncio.run(cli_send(args.address, args.command))
    sys.exit(code)


if __name__ == "__main__":
    main()
