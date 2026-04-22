"""MiraBox StreamDock device implementation."""

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import anyio

import deckr.hardware.events as hw_events

from deckr.drivers.mirabox._transport import AsyncHidTransport, descriptors_for_path
from deckr.drivers.mirabox.layouts import search_candidates
from deckr.drivers.mirabox.layouts._data import Heartbeat
from deckr.drivers.mirabox.layouts._evaluator import eval_policy
from deckr.drivers.mirabox._protocol import DeviceProtocol, MiraBoxProtocol
from deckr.drivers.mirabox.layouts import Layout
import logging

logger = logging.getLogger(__name__)


class MiraBoxDockDevice:
    """MiraBox StreamDock device implementation."""

    def __init__(
        self,
        *,
        transport: AsyncHidTransport,
        protocol: DeviceProtocol,
        layout: Layout,
    ):
        self.transport = transport
        self.protocol = protocol
        self.layout = layout

    @property
    def id(self) -> str:
        """This is a hardware identifier for the device. It is unique for the device and does not change."""
        return self.transport.hid

    @property
    def hid(self) -> str:
        """Return the HID identifier used for stable hardware identity."""
        return self.transport.hid

    @property
    def descriptor(self) -> dict[str, Any]:
        return self.transport.descriptor

    async def send_payloads(self, payloads: list[bytes]) -> None:
        """Send multiple payloads to the device."""
        await self.transport.write_chunks(payloads)

    async def wake_screen(self) -> None:
        """Wake the screen."""
        payloads = self.protocol.encode_command("wake_screen")
        await self.send_payloads(payloads)

    async def sleep_screen(self) -> None:
        """Sleep the screen."""
        payloads = self.protocol.encode_command("sleep_screen")
        await self.send_payloads(payloads)

    async def clear_key(self, target: int = 0xFF) -> None:
        """Clear a key, or all keys if target is 0xFF."""
        payloads = self.protocol.encode_command("clear_key", target=target)
        await self.send_payloads(payloads)

    async def refresh(self) -> None:
        """Refresh the screen."""
        payloads = self.protocol.encode_command("refresh")
        await self.send_payloads(payloads)

    async def connect(self) -> None:
        """Connect to the screen."""
        payloads = self.protocol.encode_command("connect")
        await self.send_payloads(payloads)

    async def set_brightness(self, value: int) -> None:
        """Set screen brightness.

        Args:
            value: Brightness percentage (0-100), converted to device range (0-255)
        """
        # Convert 0-100 percentage to 0-255 device range
        device_value = int((value / 100) * 255)
        payloads = self.protocol.encode_command("set_brightness", value=device_value)
        await self.send_payloads(payloads)

    async def set_image(self, slot_id: str, image: bytes) -> None:
        """Set a slot image (HWDevice protocol method)."""
        await self.set_key_image(slot_id, image)

    async def clear_slot(self, slot_id: str) -> None:
        """Clear a slot (HWDevice protocol method)."""
        control = self.layout.get_control_for_name(slot_id)
        if control is None:
            logger.error(f"Slot not found: {slot_id}")
            return
        if not hasattr(control, "display"):
            logger.error(f"Control {control.name} does not have a display")
            return
        await self.clear_key(target=control.display.id)

    async def set_key_image(
        self, key: str | int, image: bytes, x: int = 0, y: int = 0
    ) -> None:
        """Set a key image.

        Args:
            key: Slot ID (string) or key ID (int)
            image: Image bytes
            x: X offset (default: 0)
            y: Y offset (default: 0)
        """
        key_str = str(key)
        logger.debug(f"Setting key image for key: {key_str}")
        control = self.layout.get_control_for_name(key_str)
        if control is None:
            logger.error(f"Slot not found for key: {key_str}")
            return
        elif not hasattr(control, "display"):
            logger.error(f"Control {control.name} does not have a display")
            return
        payloads = self.protocol.encode_command(
            "set_key_image", key=control.display.id, image=image, x=x, y=y
        )
        try:
            await self.send_payloads(payloads)
        except Exception as e:
            logger.error(f"Error setting key image for key: {key_str}: {e}")
            return

    async def set_logo(self, image: bytes) -> None:
        """Set the logo.

        Args:
            image: Image bytes
        """
        payloads = self.protocol.encode_command("set_logo", image=image)
        await self.send_payloads(payloads)

    async def set_background_image(
        self, image: bytes, x: int, y: int, width: int, height: int, frame_buffer: int
    ) -> None:
        """Set the background image.

        Args:
            image: Image bytes
            x: X position
            y: Y position
            width: Image width
            height: Image height
            frame_buffer: Frame buffer ID
        """
        payloads = self.protocol.encode_command(
            "set_background_image",
            image=image,
            x=x,
            y=y,
            width=width,
            height=height,
            frame_buffer=frame_buffer,
        )
        await self.send_payloads(payloads)

    async def subscribe(self) -> AsyncIterator[Any]:
        async with self.transport.subscribe() as stream:
            async for report in stream:
                event = self.protocol.parse_event(report)
                for hw_event in self.layout.to_hardware_event(event, self):
                    yield hw_event

    @property
    def slots(self) -> list[hw_events.HWSlot]:
        return self.layout.get_slots()


@asynccontextmanager
async def launch_device(path: str, *, teardown_control: dict[str, bool] | None = None):
    descriptors = descriptors_for_path(path)
    if len(descriptors) == 0:
        logger.error(f"No descriptors found for device {path}")
        yield None
        return

    desc = descriptors[0]

    layouts = list(search_candidates(desc))

    if len(layouts) == 0:
        logger.debug("Device is not a candidate for any layouts: %s", desc)
        yield None
        return

    layout_names = [layout.get("name", "unknown") for layout in layouts]
    logger.info(
        f"Attempting to open device {path} with candidate layouts: {', '.join(layout_names)}"
    )

    async with AsyncHidTransport(path) as transport:
        report = await transport.get_input_report(
            0
        )  # TODO: Do we assume report id 0 for all devices? That seems a bit risky...
        if len(report) < 1:
            raise Exception("Failed to read firmware version")
        firmware_version = report[1:-1].decode("ascii")
        matched_layouts = []
        for layout in layouts:
            if "match" not in layout:
                continue

            device_descriptor = {**desc, "firmware": firmware_version}
            if eval_policy(layout["match"], device_descriptor):
                matched_layouts.append(layout)

        if len(matched_layouts) == 0:
            logger.warning(
                f"No layout found for device {path}. Tried the following configurations: {', '.join(layout_names)}"
            )
            yield None
            return
        elif len(matched_layouts) > 1:
            logger.warning(
                f"Multiple layouts found for device {path}. Tried the following configurations: {', '.join(layout_names)}. Will use the first one."
            )

        layout = matched_layouts[0]
        layout = Layout.model_validate(layout)
        logger.info(f"Using layout {layout.name} for device {path}")

        protocol = MiraBoxProtocol()
        device = MiraBoxDockDevice(
            transport=transport, protocol=protocol, layout=layout
        )

        async with anyio.create_task_group() as tg:
            if layout.heartbeats:
                for heartbeat in layout.heartbeats:
                    tg.start_soon(heartbeat_loop, transport, protocol, heartbeat)
            await initialize_device(transport, protocol, layout)
            logger.info("Device initialized")

            yield device

            # Duplicate HID interfaces share one display; clearing from the secondary
            # session blanks keys drawn by the primary. Discovery sets suppress_clear.
            if teardown_control is not None and teardown_control.get("suppress_clear"):
                logger.debug(
                    "Closing HID path without clear_key/refresh (duplicate interface)"
                )
            else:
                logger.info("Stopping device")
                await device.clear_key()
                await device.refresh()

    logger.info("Device terminated")


async def initialize_device(
    transport: AsyncHidTransport, protocol: DeviceProtocol, layout: Layout
):
    for command in layout.init_sequence:
        payloads = protocol.encode_command(command.cmd, **command.args)
        await transport.write_chunks(payloads)
        await anyio.sleep(0.1)


async def heartbeat_loop(
    transport: AsyncHidTransport, protocol: DeviceProtocol, heartbeat: Heartbeat
):
    while True:
        for command in heartbeat.commands:
            payloads = protocol.encode_command(command.cmd, **command.args)
            await transport.write_chunks(payloads)
            await anyio.sleep(0.1)
        await anyio.sleep(heartbeat.period)
