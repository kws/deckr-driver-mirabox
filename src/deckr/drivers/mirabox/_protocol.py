"""MiraBox StreamDock protocol implementation."""

import logging
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class InteractionEvent:
    button_id: int
    payload: int
    supports_release: bool = True


@runtime_checkable
class DeviceProtocol(Protocol):
    read_size: int

    def get_read_size(self) -> int: ...
    def encode_command(self, command: str, *args, **kwargs) -> list[bytes]: ...
    def parse_event(
        self, report: bytes, hid: str, uid: str
    ) -> InteractionEvent | None: ...


CMD_PREFIX = b"CRT\x00\x00"


class MiraBoxProtocol:
    """Protocol implementation for MiraBox StreamDock devices.

    This protocol uses:
    - Report ID: 0x00
    - Packet size: 512 bytes for protocol v1, 1024 bytes for v2/v3
    - Command prefix: "CRT" + 2 null bytes
    - Event parsing: button_id at bytes 8:10, payload at byte 10
    """

    def __init__(
        self,
        *,
        protocol_version: int,
        report_id: int = 0x00,
        read_size: int = 64,
        cmd_prefix: bytes = CMD_PREFIX,
    ):
        if protocol_version < 1 or protocol_version > 3:
            raise ValueError("MiraBox protocol_version must be 1, 2, or 3")
        self._report_id = report_id
        self._protocol_version = protocol_version
        self._packet_size = 1024 if protocol_version >= 2 else 512
        self._read_size = read_size
        self._cmd_prefix = cmd_prefix

    @property
    def read_size(self) -> int:
        return self._read_size

    @property
    def packet_size(self) -> int:
        return self._packet_size

    @property
    def protocol_version(self) -> int:
        return self._protocol_version

    @property
    def supports_release_events(self) -> bool:
        return self._protocol_version >= 3

    def _crt(self, command: str) -> bytes:
        """Create a CRT-prefixed command."""
        return self._cmd_prefix + command.encode()

    def _byte_arg(self, name: str, value: int) -> int:
        if value < 0 or value > 0xFF:
            raise ValueError(f"{name} must fit in one byte")
        return value

    def _percent_arg(self, name: str, value: int) -> int:
        if value < 0 or value > 100:
            raise ValueError(f"{name} must be a 0-100 percent value")
        return value

    def _to_report_chunks(self, data: bytes) -> list[bytes]:
        report_prefix = bytes([self._report_id])
        offset = 0
        chunk_num = 0

        chunks = []
        while offset < len(data):
            max_offset = min(offset + self._packet_size, len(data))
            chunk = data[offset:max_offset]

            if len(chunk) < self._packet_size:
                chunk = chunk + bytes(self._packet_size - len(chunk))

            payload = report_prefix + chunk
            chunks.append(payload)
            offset += self._packet_size
            chunk_num += 1

        return chunks

    def encode_command(self, command: str, *args, **kwargs) -> list[bytes]:
        """Encode a command into MiraBox protocol format.

        Supported commands:
        - "wake_display": Wake the display (DIS)
        - "sleep_display": Sleep the display (HAN)
        - "clear_key": Clear a key or all keys (CLE + target)
        - "refresh": Refresh the screen (STP)
        - "connect": Connect to the screen (CONNECT)
        - "set_brightness": Set display brightness percent (LIG + value)
        - "set_mode": Set device mode (MOD + ASCII digit)
        - "set_led_brightness": Set LED brightness percent (LBLIG + value)
        - "set_led_colors": Set LED RGB colors (SETLB + RGB triples)
        - "shutdown_clear": Clear display before shutdown (CLE + DC)
        - "set_key_image": Set key image (BAT + len_hi + len_lo + display id)
        - "set_logo": Set logo (LOG + len)
        - "set_background_image": Set background (BGPIC + len + x + y + width + height + frame_buffer)
        """
        logger.debug(f"MiraBox encode_command: {command} args={args} kwargs={kwargs}")
        if command == "wake_display":
            return self._to_report_chunks(self._crt("DIS"))

        elif command == "sleep_display":
            return self._to_report_chunks(self._crt("HAN"))

        elif command == "clear_key":
            target = kwargs.get("target", 0xFF)
            return self._to_report_chunks(self._crt("CLE") + target.to_bytes(4, "big"))

        elif command == "refresh":
            return self._to_report_chunks(self._crt("STP"))

        elif command == "connect":
            return self._to_report_chunks(self._crt("CONNECT"))

        elif command == "set_brightness":
            value = self._percent_arg("value", kwargs.get("value", 100))
            return self._to_report_chunks(self._crt("LIG") + value.to_bytes(3, "big"))

        elif command == "set_mode":
            mode = self._byte_arg("mode", kwargs["mode"])
            if mode > 9:
                raise ValueError("mode must be 0-9")
            return self._to_report_chunks(self._crt("MOD") + b"\x00\x00" + bytes([0x30 + mode]))

        elif command == "set_led_brightness":
            value = self._percent_arg("value", kwargs.get("value", 100))
            return self._to_report_chunks(self._crt("LBLIG") + bytes([value]))

        elif command == "set_led_colors":
            colors = kwargs["colors"]
            color_bytes = bytearray()
            for index, color in enumerate(colors):
                if len(color) != 3:
                    raise ValueError(f"colors[{index}] must contain red, green, and blue")
                color_bytes.extend(self._byte_arg("color component", int(component)) for component in color)
            return self._to_report_chunks(self._crt("SETLB") + bytes(color_bytes))

        elif command == "shutdown_clear":
            return self._to_report_chunks(self._crt("CLE") + b"\x00\x00DC")

        elif command == "set_key_image":
            key = kwargs["key"]
            image = kwargs["image"]
            if len(image) > 0xFFFF:
                raise ValueError("set_key_image image payload must be at most 65535 bytes")
            cmd = (
                self._crt("BAT")
                + b"\x00\x00"
                + len(image).to_bytes(2, "big")
                + self._byte_arg("key", key).to_bytes(1, "big")
            )
            return self._to_report_chunks(cmd) + self._to_report_chunks(image)

        elif command == "set_logo":
            image = kwargs["image"]
            cmd = self._crt("LOG") + len(image).to_bytes(4, "big")
            return self._to_report_chunks(cmd) + self._to_report_chunks(image)

        elif command == "set_background_image":
            image = kwargs["image"]
            x = kwargs["x"]
            y = kwargs["y"]
            width = kwargs["width"]
            height = kwargs["height"]
            frame_buffer = kwargs["frame_buffer"]
            cmd = (
                self._crt("BGPIC")
                + len(image).to_bytes(4, "big")
                + x.to_bytes(2, "big")
                + y.to_bytes(2, "big")
                + width.to_bytes(2, "big")
                + height.to_bytes(2, "big")
                + frame_buffer.to_bytes(2, "big")
            )
            return self._to_report_chunks(cmd) + self._to_report_chunks(image)

        else:
            raise ValueError(f"Unknown command: {command}")

    def parse_event(self, report: bytes) -> InteractionEvent | None:
        """Parse a MiraBox HID report into (button_id, payload).

        MiraBox format:
        - button_id: bytes 8:10 (big-endian unsigned int)
        - payload: byte 10
        """
        if len(report) < 11:
            logger.warning(
                f"MiraBox parse_event: report too short ({len(report)} bytes)"
            )
            return None

        if report[0:3] != b"ACK":
            logger.warning(f"MiraBox parse_event: invalid ACK prefix ({report[0:3]!r})")
            return None

        button_id = int.from_bytes(report[8:10], byteorder="big", signed=False)
        payload = report[10] if self.supports_release_events else 1

        event = InteractionEvent(
            button_id=button_id,
            payload=payload,
            supports_release=self.supports_release_events,
        )

        logger.debug("MiraBox parse_event: %s", event)
        return event
