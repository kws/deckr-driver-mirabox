"""Device session for MiraBox setup wizard: discovery, init, send_control_image."""

import hid

from deckr.drivers.mirabox._protocol import MiraBoxProtocol
from deckr.drivers.mirabox._transport import BlockingHidTransport, HidIoError


class DeviceSessionError(Exception):
    """Raised when device discovery or init fails."""


class DeviceSession:
    """Context manager for a MiraBox device session during setup.

    Discovers first compatible device, opens transport, runs init sequence.
    Exposes send_control_image and refresh.
    """

    def __init__(self) -> None:
        self._transport: BlockingHidTransport | None = None
        self._protocol: MiraBoxProtocol | None = None
        self._device: dict | None = None
        self._start_offset = 0

    def _write(self, chunks: list[bytes]) -> None:
        if self._transport is None:
            raise DeviceSessionError("device not open")
        for chunk in chunks:
            self._transport.write(chunk)

    @property
    def start_offset(self) -> int:
        """Protocol key offset (5 for Mbox_N4E, 0 otherwise)."""
        return self._start_offset

    @property
    def firmware_version(self) -> str:
        """Firmware version string from device."""
        if self._device is None:
            return ""
        return getattr(self, "_firmware_version", "")

    @property
    def device_info(self) -> str:
        """Scan-style device info: vid, pid, serial, firmware."""
        if self._device is None:
            return ""
        vid = self._device.get("vendor_id", 0)
        pid = self._device.get("product_id", 0)
        serial = self._device.get("serial_number")
        if serial is not None:
            if isinstance(serial, bytes):
                serial = serial.decode("utf-8", errors="replace")
        else:
            serial = ""
        fw = getattr(self, "_firmware_version", "")
        return (
            f"vendor_id={vid} product_id={pid} serial_number={serial} firmware={fw!r}"
        )

    def _device_key(self, d: dict) -> tuple:
        """Key for deduplication: same physical device exposes multiple HID endpoints."""
        vid = d.get("vendor_id", 0)
        pid = d.get("product_id", 0)
        serial = d.get("serial_number")
        if serial is not None and serial != "":
            if isinstance(serial, bytes):
                serial = serial.decode("utf-8", errors="replace")
            return (vid, pid, serial)
        return (vid, pid, d.get("path", ""))

    def __enter__(self) -> "DeviceSession":
        devices = list(hid.enumerate())
        compatible = [d for d in devices if d.get("usage_page") == 65440]
        # Deduplicate: each physical device exposes 2 HID endpoints
        seen: dict[tuple, dict] = {}
        for d in compatible:
            key = self._device_key(d)
            if key not in seen:
                seen[key] = d
        unique = list(seen.values())
        if not unique:
            raise DeviceSessionError(
                "No compatible MiraBox device found (usage_page 65440)"
            )
        if len(unique) > 1:
            raise DeviceSessionError(
                f"Multiple compatible devices found ({len(unique)}). "
                "Please disconnect all but one."
            )
        self._device = unique[0]
        self._transport = BlockingHidTransport(self._device["path"])
        self._transport.open()
        self._protocol = MiraBoxProtocol()

        report = self._transport.get_input_report(0)
        self._firmware_version = report[1:-1].decode("ascii", errors="replace")
        self._start_offset = 5 if "Mbox_N4E" in self._firmware_version else 0

        self._write(self._protocol.encode_command("wake_display"))
        self._write(self._protocol.encode_command("clear_key", target=0xFF))
        self._write(self._protocol.encode_command("set_brightness", value=100))
        self._write(self._protocol.encode_command("refresh"))
        return self

    def __exit__(self, *args: object) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        self._protocol = None
        self._device = None

    def send_control_image(self, key_id: int, jpeg_bytes: bytes) -> None:
        """Send JPEG image to the given protocol key control."""
        cmds = self._protocol.encode_command(
            "set_key_image", key=key_id, image=jpeg_bytes, x=0, y=0
        )
        self._write(cmds)

    def refresh(self) -> None:
        """Refresh the device display."""
        self._write(self._protocol.encode_command("refresh"))

    def clear_key(self, key_id: int) -> None:
        """Clear the given protocol key control (blank display)."""
        self._write(self._protocol.encode_command("clear_key", target=key_id))
        self._write(self._protocol.encode_command("refresh"))

    def read_report(self, timeout_ms: int = 100, read_size: int = 512) -> bytes | None:
        """Read one HID report from the device. Returns None on timeout or empty read."""
        if self._transport is None:
            return None
        try:
            data = self._transport.read(timeout_ms, read_size=read_size)
            return data if len(data) > 0 else None
        except HidIoError:
            return None

    def decode_event(self, report: bytes) -> str:
        """Decode HID report to display string. Shows key_id/payload or raw hex on failure."""
        if self._protocol is None or len(report) == 0:
            return f"raw: {report[:64].hex(' ')}"
        event = self._protocol.parse_event(report)
        if event is not None:
            return f"key_id={event.button_id} payload={event.payload}"
        return f"raw: {report[:64].hex(' ')}"
