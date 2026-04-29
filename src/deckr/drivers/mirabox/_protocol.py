"""MiraBox StreamDock protocol implementation."""

import logging
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class InteractionEvent:
    button_id: int
    payload: int


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
    - Packet size: 1024 bytes
    - Command prefix: "CRT" + 2 null bytes
    - Event parsing: button_id at bytes 8:10, payload at byte 10
    """

    def __init__(
        self,
        *,
        report_id: int = 0x00,
        packet_size: int = 1024,
        read_size: int = 64,
        cmd_prefix: bytes = CMD_PREFIX,
    ):
        self._report_id = report_id
        self._packet_size = packet_size
        self._read_size = read_size
        self._cmd_prefix = cmd_prefix

    @property
    def read_size(self) -> int:
        return self._read_size

    def _crt(self, command: str) -> bytes:
        """Create a CRT-prefixed command."""
        return self._cmd_prefix + command.encode()

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
        - "wake_screen": Wake the screen (DIS)
        - "sleep_screen": Sleep the screen (HAN)
        - "clear_key": Clear a key or all keys (CLE + target)
        - "refresh": Refresh the screen (STP)
        - "connect": Connect to the screen (CONNECT)
        - "set_brightness": Set brightness (LIG + value)
        - "set_key_image": Set key image (BAT + len + key + x + y)
        - "set_logo": Set logo (LOG + len)
        - "set_background_image": Set background (BGPIC + len + x + y + width + height + frame_buffer)
        """
        logger.debug(f"MiraBox encode_command: {command} args={args} kwargs={kwargs}")
        if command == "wake_screen":
            return self._to_report_chunks(self._crt("DIS"))

        elif command == "sleep_screen":
            return self._to_report_chunks(self._crt("HAN"))

        elif command == "clear_key":
            target = kwargs.get("target", 0xFF)
            return self._to_report_chunks(self._crt("CLE") + target.to_bytes(4, "big"))

        elif command == "refresh":
            return self._to_report_chunks(self._crt("STP"))

        elif command == "connect":
            return self._to_report_chunks(self._crt("CONNECT"))

        elif command == "set_brightness":
            value = kwargs.get("value", 0xFF)
            return self._to_report_chunks(self._crt("LIG") + value.to_bytes(3, "big"))

        elif command == "set_key_image":
            key = kwargs["key"]
            image = kwargs["image"]
            x = kwargs.get("x", 0)
            y = kwargs.get("y", 0)
            cmd = (
                self._crt("BAT")
                + len(image).to_bytes(4, "big")
                + key.to_bytes(1, "big")
                + x.to_bytes(2, "big")
                + y.to_bytes(2, "big")
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
        payload = report[10]

        event = InteractionEvent(button_id=button_id, payload=payload)

        logger.debug("MiraBox parse_event: %s", event)
        return event
