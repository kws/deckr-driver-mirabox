from __future__ import annotations

import pytest
from deckr.hardware.descriptors import DeviceDescriptor

from deckr.drivers.mirabox._device import MiraBoxDockDevice


class _FakeTransport:
    def __init__(self) -> None:
        self.hid = "0B00:1001:0300D0785616"
        self.descriptor = {"product_string": "MSD_TWO", "serial_number": "0300D0785616"}


class _FakeLayout:
    name = "MSD_TWO"

    def get_controls(self):
        return []


class _FakeOutputTransport(_FakeTransport):
    def __init__(self) -> None:
        super().__init__()
        self.writes: list[list[bytes]] = []

    async def write_chunks(self, chunks: list[bytes]) -> None:
        self.writes.append(chunks)


class _FakeProtocol:
    def __init__(self) -> None:
        self.commands: list[tuple[str, dict]] = []

    def encode_command(self, command: str, **kwargs) -> list[bytes]:
        self.commands.append((command, kwargs))
        return [command.encode("ascii")]


class _FakeDisplay:
    id = 7


class _FakeControl:
    name = "0,0"
    display = _FakeDisplay()


class _FakeRasterLayout(_FakeLayout):
    def get_control_for_name(self, name: str):
        if name == "0,0":
            return _FakeControl()
        return None


def test_mirabox_device_exposes_hid_for_hw_device_contract():
    device = MiraBoxDockDevice(
        transport=_FakeTransport(),
        protocol=object(),
        layout=_FakeLayout(),
    )

    assert device.id == "0B00:1001:0300D0785616"
    assert device.hid == "0B00:1001:0300D0785616"

    info = device.device_descriptor
    assert isinstance(info, DeviceDescriptor)
    assert info.device_id == "0B00:1001:0300D0785616"
    assert info.fingerprint == "0B00:1001:0300D0785616"


@pytest.mark.asyncio
async def test_set_raster_frame_refreshes_after_sending_image() -> None:
    protocol = _FakeProtocol()
    transport = _FakeOutputTransport()
    device = MiraBoxDockDevice(
        transport=transport,
        protocol=protocol,
        layout=_FakeRasterLayout(),
    )

    await device.set_raster_frame("0,0", b"jpeg")

    assert protocol.commands == [
        ("set_key_image", {"key": 7, "image": b"jpeg"}),
        ("refresh", {}),
    ]
    assert transport.writes == [[b"set_key_image"], [b"refresh"]]


@pytest.mark.asyncio
async def test_clear_raster_refreshes_after_clearing_key() -> None:
    protocol = _FakeProtocol()
    transport = _FakeOutputTransport()
    device = MiraBoxDockDevice(
        transport=transport,
        protocol=protocol,
        layout=_FakeRasterLayout(),
    )

    await device.clear_raster("0,0")

    assert protocol.commands == [
        ("clear_key", {"target": 7}),
        ("refresh", {}),
    ]
    assert transport.writes == [[b"clear_key"], [b"refresh"]]
