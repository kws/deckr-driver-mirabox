from __future__ import annotations

from deckr.drivers.mirabox._protocol import MiraBoxProtocol


def test_protocol_v1_uses_512_byte_hid_reports() -> None:
    protocol = MiraBoxProtocol(protocol_version=1)

    payloads = protocol.encode_command("set_key_image", key=1, image=bytes(600))

    assert protocol.packet_size == 512
    assert [len(payload) for payload in payloads] == [513, 513, 513]


def test_protocol_v2_uses_1024_byte_hid_reports() -> None:
    protocol = MiraBoxProtocol(protocol_version=2)

    payloads = protocol.encode_command("set_key_image", key=1, image=bytes(600))

    assert protocol.packet_size == 1024
    assert [len(payload) for payload in payloads] == [1025, 1025]


def test_protocol_v3_uses_1024_byte_hid_reports() -> None:
    protocol = MiraBoxProtocol(protocol_version=3)

    payloads = protocol.encode_command("set_key_image", key=1, image=bytes(600))

    assert protocol.packet_size == 1024
    assert [len(payload) for payload in payloads] == [1025, 1025]


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
