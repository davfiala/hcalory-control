#!/usr/bin/env python3
import argparse
import asyncio
import dataclasses
import enum
import json
import logging
from typing import Any

import bleak
import bleak_retry_connector
import datastruct
from bleak import BleakError

logger = logging.getLogger(__name__)


class ListableEnum(enum.Enum):
    @classmethod
    def list(cls) -> list[str]:
        return list(cls.__members__.keys())


command_header = bytes.fromhex("000200010001000e040000090000000000000000")


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


@dataclasses.dataclass
class HeaterResponse(datastruct.DataStruct):
    _header: bytes = datastruct.fields.field("20s")
    heater_state: HeaterState = datastruct.fields.field("B")
    heater_mode: HeaterMode = datastruct.fields.field("B")
    heater_setting: int = datastruct.fields.field("B")
    _mystery: int = datastruct.fields.field("B")
    _1: ... = datastruct.fields.padding(1)
    _voltage: int = datastruct.fields.field("B")
    _2: ... = datastruct.fields.padding(1)
    _body_temperature: bytes = datastruct.fields.field("2s")
    _3: ... = datastruct.fields.padding(1)
    _ambient_temperature: bytes = datastruct.fields.field("2s")
    _end_junk: bytes = datastruct.fields.field("7s")

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


class HCaloryHeater:
    write_characteristic_id = "0000fff2-0000-1000-8000-00805f9b34fb"
    read_characteristic_id = "0000fff1-0000-1000-8000-00805f9b34fb"

    def __init__(
        self,
        device: bleak.BLEDevice,
        bluetooth_timeout: float = 30.0,
        max_bluetooth_retry_attempts: int = 20,
    ):
        self.device: bleak.BLEDevice = device
        self.bluetooth_timeout: float = bluetooth_timeout
        self.max_bluetooth_retry_attempts = max_bluetooth_retry_attempts
        self._data_pump_queue: asyncio.Queue[bytearray] = asyncio.Queue()
        self.bleak_client: bleak.BleakClient | None = None
        self._write_characteristic: str | None = None
        self._read_characteristic: str | None = None
        self._intentional_disconnect: bool = False
        self._reconnect_event: asyncio.Event = asyncio.Event()
        self._reconnect_event.clear()
        self._connect_lock = asyncio.Lock()
        self._command_lock = asyncio.Lock()

    async def start_heat(self):
        await self.send_command(Command.start_heat)

    async def stop_heat(self):
        await self.send_command(Command.stop_heat)

    async def change_setting_up(self):
        await self.send_command(Command.up)

    async def change_setting_down(self):
        await self.send_command(Command.down)

    async def set_thermostat_mode(self):
        await self.send_command(Command.thermostat)

    async def set_gear_mode(self):
        await self.send_command(Command.gear)

    async def _ensure_connection(self, connection_reason: str = "") -> None:
        if connection_reason:
            connection_reason = f" Reason: {connection_reason}"
        if self._connect_lock.locked():
            logger.debug(
                "Connection to heater %s already ongoing. You'll have to wait!%s",
                self.device.address,
                connection_reason,
            )
        if self.bleak_client and self.bleak_client.is_connected:
            return
        async with self._connect_lock:
            if self.bleak_client and self.bleak_client.is_connected:
                return
            try:
                client = await bleak_retry_connector.establish_connection(
                    bleak.BleakClient,
                    self.device,
                    disconnected_callback=self.handle_disconnect,
                    use_services_cache=True,
                    max_attempts=self.max_bluetooth_retry_attempts,
                    timeout=self.bluetooth_timeout,
                )
                self.bleak_client = client
                self._intentional_disconnect = False
                self._reconnect_event.set()
                await self.bleak_client.start_notify(
                    self.read_characteristic_id, self.data_pump_handler
                )
            except (asyncio.TimeoutError, BleakError):
                logger.exception(
                    "Failed to connect to heater %s. This will be retried.%s",
                    self.device.address,
                    connection_reason,
                )
                raise

    @property
    def is_connected(self) -> bool:
        return self.bleak_client is not None and self.bleak_client.is_connected

    def handle_disconnect(self, _: bleak.BleakClient) -> None:
        self._reconnect_event.clear()
        if not self._intentional_disconnect:
            logger.warning(
                "Encountered unintentional disconnect from %s.", self.device.address
            )

    async def data_pump_handler(
        self, _: str, data: bytearray
    ) -> None:
        await self._data_pump_queue.put(data)

    async def get_data(self) -> HeaterResponse:
        await self.send_command(Command.pump_data)
        raw = await self._data_pump_queue.get()
        return HeaterResponse.unpack(raw)

    async def disconnect(self) -> None:
        self._intentional_disconnect = True
        if self.bleak_client:
            await self.bleak_client.disconnect()

    async def send_command(self, command: Command):
        async with self._command_lock:
            await self._ensure_connection(f"Sending command {command.name}")
            assert self.bleak_client is not None
            await self.bleak_client.write_gatt_char(self.write_characteristic_id, command)

    async def wait_for_reconnect(self, timeout: float = 30.0) -> None:
        try:
            await asyncio.wait_for(self._reconnect_event.wait(), timeout)
        except asyncio.TimeoutError:
            pass


async def run_command(command: Command, address: str) -> None:
    device = await bleak.BleakScanner.find_device_by_address(address, timeout=30.0)
    assert device is not None
    heater = HCaloryHeater(device)
    pre_command_data = await heater.get_data()
    if command == Command.pump_data:
        print(json.dumps(pre_command_data.asdict(), sort_keys=True, indent=4))
        return
    await heater.send_command(command)
    await asyncio.sleep(1)
    post_command_data = await heater.get_data()
    print(
        f"Before command:\n{json.dumps(pre_command_data.asdict(), sort_keys=True, indent=4)}"
    )
    print(
        f"After command:\n{json.dumps(post_command_data.asdict(), sort_keys=True, indent=4)}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", type=str, choices=Command.list())
    parser.add_argument("--address", type=str, required=True)
    args = parser.parse_args()
    command = Command[args.command]
    address: str = args.address
    asyncio.run(run_command(command, address))


if __name__ == "__main__":
    main()
