import logging
from contextlib import asynccontextmanager
from typing import Any

import anyio
import hid
from deckr.hardware import messages as hw_messages
from deckr.transports.bus import EventBus

from deckr.drivers.mirabox._device import launch_device

logger = logging.getLogger(__name__)


def _physical_hid_key(d: dict[str, Any]) -> tuple[Any, ...]:
    """Group HID enumerate rows that share one USB device."""
    serial = d.get("serial_number")
    if serial is None:
        serial = ""
    return (d.get("vendor_id"), d.get("product_id"), serial)


def _hid_interface_sort_key(d: dict[str, Any]) -> tuple[Any, Any]:
    """Prefer lower interface_number (primary / key display is usually interface 0)."""
    iface = d.get("interface_number")
    if iface is None:
        iface = 999
    return (iface, d.get("path") or b"")


@asynccontextmanager
async def discover_mirabox_devices(event_bus: EventBus, *, manager_id: str):
    """
    The discovery loop manages USB connections. It stores 'non' candidate paths,
    so that these are not re-attempted on each pass of the loop. If a path disappears,
    it is removed from the register.

    If a candidate path is found, and attempt is made to open it. If successfully opened, then
    a connection event is sent to the event stream.

    The component is managed, and when the device is stopped, a disconnected event is sent and it
    is removed from the register.
    """
    send_stream, receive_stream = anyio.create_memory_object_stream[str](
        max_buffer_size=100
    )
    discovery_send, discovery_receive = anyio.create_memory_object_stream[Any](
        max_buffer_size=100
    )

    async with anyio.create_task_group() as tg:
        tg.start_soon(discover_loop, discovery_send)
        tg.start_soon(
            launcher_loop,
            discovery_receive,
            send_stream,
            event_bus,
            manager_id,
        )
        yield receive_stream


async def discover_loop(send_stream: anyio.abc.ByteStream):
    path_register: set[bytes] = set()
    while True:
        rows = list(hid.enumerate())
        devices = {d["path"]: d for d in rows}
        seen_paths = set(devices.keys())

        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for d in rows:
            groups.setdefault(_physical_hid_key(d), []).append(d)
        canonical_path = {
            k: min(members, key=_hid_interface_sort_key)["path"]
            for k, members in groups.items()
        }

        for path, device in devices.items():
            if path in path_register:
                continue
            if canonical_path[_physical_hid_key(device)] != path:
                logger.info(
                    "Skipping HID path %s (canonical interface for this device is %s)",
                    path,
                    canonical_path[_physical_hid_key(device)],
                )
                continue
            await send_stream.send(device)
        path_register = seen_paths
        await anyio.sleep(1)


async def launcher_loop(
    receive_stream: anyio.abc.ByteStream,
    send_stream: anyio.abc.ObjectSendStream[Any],
    event_bus: EventBus,
    manager_id: str,
):
    connected_device_ids: set[str] = set()

    async with anyio.create_task_group() as tg:
        async for device in receive_stream:
            tg.start_soon(
                device_loop,
                device,
                send_stream,
                event_bus,
                connected_device_ids,
                manager_id,
            )


async def device_loop(
    device: dict[str, Any],
    send_stream: anyio.abc.ObjectSendStream[Any],
    event_bus: EventBus,
    connected_device_ids: set[str],
    manager_id: str,
):
    cancelled = anyio.get_cancelled_exc_class()
    device_id = None

    try:
        teardown_control: dict[str, bool] = {}
        async with launch_device(
            device["path"], teardown_control=teardown_control
        ) as my_device:
            if my_device is None:
                return

            device_id = my_device.id

            if device_id in connected_device_ids:
                logger.info(
                    "Device %s already connected (duplicate HID interface %s), skipping",
                    device_id,
                    device["path"],
                )
                # Avoid clear_key/refresh on exit: same physical device as primary session.
                teardown_control["suppress_clear"] = True
                return

            connected_device_ids.add(device_id)
            logger.info("Device connected: %s", device_id)
            await send_stream.send(
                hw_messages.hardware_input_message(
                    manager_id=manager_id,
                    device_id=device_id,
                    body=hw_messages.DeviceConnectedMessage(
                        device=hw_messages.HardwareDevice(
                            id=my_device.id,
                            fingerprint=my_device.hid,
                            hid=my_device.hid,
                            slots=list(my_device.slots),
                            name=getattr(my_device, "name", None),
                        ),
                    ),
                )
            )
            async with anyio.create_task_group() as tg:
                tg.start_soon(
                    _run_until_complete,
                    tg.cancel_scope,
                    _forward_device_events,
                    my_device,
                    send_stream,
                    manager_id,
                )
                tg.start_soon(
                    _run_until_complete,
                    tg.cancel_scope,
                    _apply_device_commands,
                    my_device,
                    event_bus,
                    manager_id,
                )

    except cancelled as e:
        raise e
    except Exception as e:
        logger.info("Device error: %s", e)
        if logger.isEnabledFor(logging.DEBUG):
            logger.exception("Device error: %s", e, exc_info=True)

    if device_id is not None:
        connected_device_ids.discard(device_id)
        await send_stream.send(
            hw_messages.hardware_input_message(
                manager_id=manager_id,
                device_id=device_id,
                body=hw_messages.DeviceDisconnectedMessage(),
            )
        )


async def _forward_device_events(
    device: Any,
    send_stream: anyio.abc.ObjectSendStream[Any],
    manager_id: str,
) -> None:
    async for event in device.subscribe():
        await send_stream.send(
            hw_messages.hardware_input_message(
                manager_id=manager_id,
                device_id=device.id,
                body=event,
            )
        )


async def _run_until_complete(cancel_scope, func, *args) -> None:
    try:
        await func(*args)
    finally:
        cancel_scope.cancel()


async def _apply_device_commands(
    device: Any,
    event_bus: EventBus,
    manager_id: str,
) -> None:
    async with event_bus.subscribe() as stream:
        async for envelope in stream:
            ref = hw_messages.hardware_device_ref_from_message(envelope)
            if ref != hw_messages.HardwareDeviceRef(
                manager_id=manager_id,
                device_id=device.id,
            ):
                continue
            message = hw_messages.hardware_body_from_message(envelope)
            if not isinstance(message, hw_messages.HARDWARE_COMMAND_MESSAGE_TYPES):
                continue
            if isinstance(message, hw_messages.SetImageMessage):
                await device.set_image(message.slot_id, message.image)
            elif isinstance(message, hw_messages.ClearSlotMessage):
                await device.clear_slot(message.slot_id)
            elif isinstance(message, hw_messages.SleepScreenMessage):
                await device.sleep_screen()
            elif isinstance(message, hw_messages.WakeScreenMessage):
                await device.wake_screen()
