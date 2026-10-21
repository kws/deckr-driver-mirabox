"""Exercise Deckr messages through real layouts/protocols to the HID boundary."""

import base64
from contextlib import asynccontextmanager

import anyio
import pytest
from deckr.hardware import HardwareCommandContext
from deckr.hardware import messages as hw

from deckr.drivers.mirabox._device import MiraBoxDockDevice, run_command_sequence
from deckr.drivers.mirabox._discovery import (
    ResetDeviceCommand,
    _apply_device_commands,
    _forward_device_events,
)
from deckr.drivers.mirabox._protocol import MiraBoxProtocol
from deckr.drivers.mirabox.layouts import BUILD_IN_LAYOUT_PATH, parse_layout_file
from deckr.drivers.mirabox.layouts._data import Layout

pytestmark = pytest.mark.asyncio


class RecordingHid:
    hid = "test-panel"
    descriptor = {"serial_number": "test-panel"}

    def __init__(self, reports=()):
        self.reports = reports
        self.writes = []

    async def write_chunks(self, chunks, *, context=None):
        if context is not None:
            context.authorize()
        self.writes.extend(chunks)

    @asynccontextmanager
    async def subscribe(self):
        async def reports():
            for report in self.reports:
                yield report

        yield reports()


def _device(transport, filename="device-msd-two.yml"):
    layout = Layout.model_validate(parse_layout_file(BUILD_IN_LAYOUT_PATH / filename))
    return MiraBoxDockDevice(
        transport=transport,
        layout=layout,
        protocol=MiraBoxProtocol(protocol_version=layout.protocol_version),
    )


def _command(
    *,
    control_id="0,0",
    capability_id="raster.bitmap",
    command_type="clear",
    params=None,
    manager_id="manager",
    device_id="test-panel",
):
    return hw.control_command_message(
        controller_id="controller",
        sender_session_id="controller-session",
        manager_id=manager_id,
        device_id=device_id,
        control_id=control_id,
        capability_id=capability_id,
        command_type=command_type,
        params=params,
    )


async def _apply(device, commands):
    send, receive = anyio.create_memory_object_stream(len(commands))
    async with receive:
        async with send:
            for command in commands:
                if not isinstance(command, ResetDeviceCommand):
                    command = HardwareCommandContext(
                        command, device.device_descriptor, lambda: None
                    )
                await send.send(command)
        await _apply_device_commands(device, receive, "manager")


@pytest.mark.parametrize(
    "filename", sorted(path.name for path in BUILD_IN_LAYOUT_PATH.glob("*.yml"))
)
async def test_builtin_layout_initialization_and_teardown_write_hid_commands(filename):
    transport = RecordingHid()
    device = _device(transport, filename)
    await run_command_sequence(transport, device.protocol, device.layout.init_sequence)
    await run_command_sequence(
        transport, device.protocol, device.layout.teardown_sequence
    )

    size = 513 if filename == "device-akp153-protocol-v1.yml" else 1025
    assert transport.writes == [
        command.ljust(size, b"\x00")
        for command in (
            b"\x00CRT\x00\x00DIS",
            b"\x00CRT\x00\x00CLE\x00\x00\x00\xff",
            b"\x00CRT\x00\x00LIG\x00\x00\x64",
            b"\x00CRT\x00\x00STP",
            b"\x00CRT\x00\x00CLE\x00\x00\x00\xff",
            b"\x00CRT\x00\x00STP",
        )
    ]


async def test_deckr_raster_power_and_reset_commands_write_expected_hid_reports():
    transport = RecordingHid()
    device = _device(transport)
    image = bytes(range(256)) * 5  # Cross a report boundary with nonzero data.
    await _apply(
        device,
        [
            _command(
                command_type="set_frame",
                params={
                    "image": base64.b64encode(image).decode("ascii"),
                    "encoding": "jpeg",
                },
            ),
            _command(),
            _command(
                control_id=None, capability_id="device.power", command_type="sleep"
            ),
            _command(
                control_id=None, capability_id="device.power", command_type="wake"
            ),
            ResetDeviceCommand(),
        ],
    )

    assert transport.writes == [
        report.ljust(1025, b"\x00")
        for report in (
            b"\x00CRT\x00\x00BAT\x00\x00\x05\x00\x01",
            b"\x00" + image[:1024],
            b"\x00" + image[1024:],
            b"\x00CRT\x00\x00STP",
            b"\x00CRT\x00\x00CLE\x00\x00\x00\x01",
            b"\x00CRT\x00\x00STP",
            b"\x00CRT\x00\x00HAN",
            b"\x00CRT\x00\x00DIS",
            b"\x00CRT\x00\x00CLE\x00\x00\x00\xff",
            b"\x00CRT\x00\x00STP",
        )
    ]


@pytest.mark.parametrize(
    "command",
    [
        _command(manager_id="other-manager"),
        _command(device_id="other-device"),
        _command(capability_id="unknown"),
        _command(command_type="set_frame", params={"image": "!", "encoding": "jpeg"}),
        _command(command_type="set_frame", params={}),
    ],
)
async def test_misdirected_or_malformed_commands_do_not_write_hid(command):
    transport = RecordingHid()
    await _apply(_device(transport), [command])
    assert transport.writes == []


@pytest.mark.parametrize(
    ("filename", "events"),
    [
        (
            "device-msd-two.yml",
            [("down", {"eventType": "down"}), ("up", {"eventType": "up"})],
        ),
        (
            "device-akp03-protocol-v2.yml",
            [
                ("down", {"eventType": "down"}),
                ("up", {"eventType": "up"}),
                ("down", {"eventType": "down"}),
                ("up", {"eventType": "up"}),
            ],
        ),
    ],
)
async def test_hid_reports_become_deckr_input_with_device_identity(filename, events):
    def report(payload):
        data = bytearray(64)
        data[:3] = b"ACK"
        data[8:10] = (1).to_bytes(2, "big")
        data[10] = payload
        return bytes(data)

    transport = RecordingHid([b"short", bytes(64), report(1), report(0)])
    send, receive = anyio.create_memory_object_stream(10)
    async with receive:
        async with send:
            await _forward_device_events(
                _device(transport, filename), send, "manager", "session"
            )
        messages = [message async for message in receive]

    bodies = [hw.hardware_body_from_message(message) for message in messages]
    assert [(body.event_type, body.value) for body in bodies] == events
    for message, body in zip(messages, bodies, strict=True):
        assert message.sender.endpoint_id == "manager"
        assert message.sender_session_id == "session"
        assert body.control_id == "0,0"
        assert body.capability_id == "button.momentary"
        ref = hw.hardware_device_ref_from_message(message)
        assert ref.device_id == "test-panel"
        assert ref.fingerprint == "test-panel"
