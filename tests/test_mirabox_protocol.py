from __future__ import annotations

import pytest

from deckr.drivers.mirabox._protocol import MiraBoxProtocol


@pytest.mark.parametrize(("version", "report_size"), [(1, 513), (2, 1025), (3, 1025)])
def test_image_reports_preserve_payload_across_packet_boundaries(version, report_size):
    protocol = MiraBoxProtocol(protocol_version=version)
    image = bytes(range(256)) * 5
    payloads = protocol.encode_command("set_key_image", key=7, image=image)

    assert len(payloads) == (4 if version == 1 else 3)
    assert all(len(payload) == report_size and payload[0] == 0 for payload in payloads)
    assert payloads[0] == b"\x00CRT\x00\x00BAT\x00\x00\x05\x00\x07".ljust(
        report_size, b"\x00"
    )
    assembled = b"".join(payload[1:] for payload in payloads[1:])
    assert assembled[: len(image)] == image
    assert assembled[len(image) :] == bytes(len(assembled) - len(image))


def test_compact_bat_image_header_uses_wire_display_id() -> None:
    protocol = MiraBoxProtocol(protocol_version=3)

    payloads = protocol.encode_command("set_key_image", key=7, image=b"abc")

    assert len(payloads) == 2
    assert payloads[0][0:14] == b"\x00CRT\x00\x00BAT\x00\x00\x00\x03\x07"
    assert payloads[1][1:4] == b"abc"


def test_brightness_percent_encoding() -> None:
    protocol = MiraBoxProtocol(protocol_version=3)

    payloads = protocol.encode_command("set_brightness", value=50)

    assert payloads[0][0:12] == b"\x00CRT\x00\x00LIG\x00\x002"


def test_mode_command_encoding() -> None:
    protocol = MiraBoxProtocol(protocol_version=3)

    payloads = protocol.encode_command("set_mode", mode=2)

    assert payloads[0][0:12] == b"\x00CRT\x00\x00MOD\x00\x002"


def test_led_command_encoding() -> None:
    protocol = MiraBoxProtocol(protocol_version=3)

    brightness = protocol.encode_command("set_led_brightness", value=40)
    colors = protocol.encode_command(
        "set_led_colors",
        colors=[(1, 2, 3), (4, 5, 6)],
    )

    assert brightness[0][0:12] == b"\x00CRT\x00\x00LBLIG("
    assert colors[0][0:17] == b"\x00CRT\x00\x00SETLB\x01\x02\x03\x04\x05\x06"


def test_shutdown_clear_command_encoding() -> None:
    protocol = MiraBoxProtocol(protocol_version=3)

    payloads = protocol.encode_command("shutdown_clear")

    assert payloads[0][0:13] == b"\x00CRT\x00\x00CLE\x00\x00DC"


def test_protocol_v2_synthesizes_press_payload_for_no_release_input_reports() -> None:
    protocol = MiraBoxProtocol(protocol_version=2)
    report = bytearray(64)
    report[0:3] = b"ACK"
    report[8:10] = (81).to_bytes(2, "big")
    report[10] = 0

    event = protocol.parse_event(bytes(report))

    assert event is not None
    assert event.button_id == 81
    assert event.payload == 1
    assert event.supports_release is False


def test_protocol_v3_uses_release_payload_from_input_reports() -> None:
    protocol = MiraBoxProtocol(protocol_version=3)
    report = bytearray(64)
    report[0:3] = b"ACK"
    report[8:10] = (81).to_bytes(2, "big")
    report[10] = 0

    event = protocol.parse_event(bytes(report))

    assert event is not None
    assert event.button_id == 81
    assert event.payload == 0
    assert event.supports_release is True
