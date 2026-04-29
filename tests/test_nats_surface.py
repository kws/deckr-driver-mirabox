from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import anyio
import pytest
from deckr.contracts.lanes import CORE_LANE_CONTRACTS, LaneContractRegistry
from deckr.contracts.messages import (
    DeckrMessage,
    controller_address,
    endpoint_target,
    hardware_manager_address,
)
from deckr.hardware import messages as hw_messages
from deckr.runtime import Deckr
from deckr.state import (
    DeviceClaim,
    HardwareInventory,
    StateUnavailable,
    hardware_inventory_key,
)
from memory_lane_substrate import MemoryLaneSubstrate

from deckr.drivers.mirabox._discovery import (
    _apply_device_commands,
    _manager_command_subscription,
)
from deckr.drivers.mirabox._factory import MiraboxDeviceFactory


def _deckr() -> Deckr:
    lane_contracts = LaneContractRegistry(CORE_LANE_CONTRACTS.values())
    return Deckr(
        lane_contracts=lane_contracts,
        substrate=MemoryLaneSubstrate(lane_contracts=lane_contracts),
    )


def _device() -> hw_messages.HardwareDevice:
    return hw_messages.HardwareDevice(
        id="deck",
        name="MiraBox",
        hid="hid:deck",
        fingerprint="fingerprint:deck",
        slots=[
            hw_messages.HardwareSlot(
                id="0,0",
                coordinates=hw_messages.HardwareCoordinates(column=0, row=0),
                image_format=hw_messages.HardwareImageFormat(width=72, height=72),
                gestures=("key_down", "key_up"),
            )
        ],
    )


def _factory(deckr: Deckr) -> MiraboxDeviceFactory:
    manager = MiraboxDeviceFactory(
        deckr.lane("hardware_messages"),
        deckr.state(),
        manager_id="mirabox-main",
    )
    manager._endpoint = deckr.lane("hardware_messages").endpoint(
        hardware_manager_address("mirabox-main")
    )
    return manager


def _claim(controller_id: str = "main", session_id: str = "controller-session"):
    return DeviceClaim(
        claimedByEndpoint=controller_address(controller_id),
        claimedBySessionId=session_id,
        timestamp=datetime.now(UTC),
        ttlSeconds=15,
    )


@pytest.mark.asyncio
async def test_connect_and_disconnect_rewrite_aggregate_inventory() -> None:
    async with _deckr() as deckr:
        manager = _factory(deckr)
        await manager._handle_device_message(
            hw_messages.hardware_input_message(
                manager_id="mirabox-main",
                device_id="deck",
                body=hw_messages.DeviceConnectedMessage(device=_device()),
            )
        )
        entry = await deckr.state().get(hardware_inventory_key("mirabox-main"))
        assert entry is not None
        inventory = HardwareInventory.model_validate(entry.value)
        assert set(inventory.devices) == {"deck"}
        assert inventory.devices["deck"].descriptor["id"] == "deck"

        await manager._handle_device_message(
            hw_messages.hardware_input_message(
                manager_id="mirabox-main",
                device_id="deck",
                body=hw_messages.DeviceDisconnectedMessage(),
            )
        )
        entry = await deckr.state().get(hardware_inventory_key("mirabox-main"))
        assert entry is not None
        inventory = HardwareInventory.model_validate(entry.value)
        assert inventory.devices == {}


@pytest.mark.asyncio
async def test_inventory_state_unavailable_keeps_local_device_state() -> None:
    class UnavailableState:
        async def put(self, *args, **kwargs):
            raise StateUnavailable("temporary substrate outage")

    async with _deckr() as deckr:
        manager = MiraboxDeviceFactory(
            deckr.lane("hardware_messages"),
            UnavailableState(),
            manager_id="mirabox-main",
        )
        manager._endpoint = deckr.lane("hardware_messages").endpoint(
            hardware_manager_address("mirabox-main")
        )

        await manager._handle_device_message(
            hw_messages.hardware_input_message(
                manager_id="mirabox-main",
                device_id="deck",
                body=hw_messages.DeviceConnectedMessage(device=_device()),
            )
        )

    assert "deck" in manager._devices
    assert manager._inventory_revision is None


@pytest.mark.asyncio
async def test_presence_heartbeat_refreshes_aggregate_inventory() -> None:
    async with _deckr() as deckr:
        manager = _factory(deckr)
        manager._devices["deck"] = _device()

        async with anyio.create_task_group() as tg:
            tg.start_soon(manager._presence_loop)
            with anyio.fail_after(1):
                while True:
                    entry = await deckr.state().get(
                        hardware_inventory_key("mirabox-main")
                    )
                    if entry is not None:
                        break
                    await anyio.sleep(0.01)
            tg.cancel_scope.cancel()

    inventory = HardwareInventory.model_validate(entry.value)
    assert set(inventory.devices) == {"deck"}


@pytest.mark.asyncio
async def test_claimed_input_is_sent_only_to_claiming_controller() -> None:
    async with _deckr() as deckr:
        manager = _factory(deckr)
        manager._claims["deck"] = _claim()
        manager._controller_presence_sessions[controller_address("main")] = (
            "controller-session"
        )
        main = deckr.lane("hardware_messages").endpoint(controller_address("main"))
        other = deckr.lane("hardware_messages").endpoint(controller_address("other"))

        async with main.subscribe() as main_stream, other.subscribe() as other_stream:
            await manager._handle_device_message(
                hw_messages.hardware_input_message(
                    manager_id="mirabox-main",
                    device_id="deck",
                    body=hw_messages.KeyDownMessage(key_id="0,0"),
                )
            )
            received = await main_stream.receive()
            with anyio.move_on_after(0.05) as scope:
                await other_stream.receive()

    assert received.recipient.endpoint == controller_address("main")
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_claim_delete_resets_device_and_drops_input() -> None:
    class FakeDevice:
        id = "deck"

        def __init__(self) -> None:
            self.clear_key = AsyncMock()
            self.refresh = AsyncMock()

    async with _deckr() as deckr:
        manager = _factory(deckr)
        device = FakeDevice()
        command_send, command_receive = anyio.create_memory_object_stream(max_buffer_size=100)
        manager._command_streams["deck"] = command_send
        manager._controller_presence_sessions[controller_address("main")] = (
            "controller-session"
        )
        claim_key = "claim.device.mirabox-main.deck"
        main = deckr.lane("hardware_messages").endpoint(controller_address("main"))

        async with (
            command_send,
            command_receive,
            main.subscribe() as main_stream,
            anyio.create_task_group() as tg,
        ):
            tg.start_soon(manager._claim_watch_loop)
            tg.start_soon(
                _apply_device_commands,
                device,
                command_receive,
                "mirabox-main",
            )
            await deckr.state().create(claim_key, _claim())
            with anyio.fail_after(1):
                while "deck" not in manager._claims:
                    await anyio.sleep(0.01)

            await deckr.state().delete(claim_key)
            with anyio.fail_after(1):
                while device.clear_key.await_count < 1:
                    await anyio.sleep(0.01)

            await manager._handle_device_message(
                hw_messages.hardware_input_message(
                    manager_id="mirabox-main",
                    device_id="deck",
                    body=hw_messages.KeyDownMessage(key_id="0,0"),
                )
            )
            with anyio.move_on_after(0.05) as scope:
                await main_stream.receive()
            tg.cancel_scope.cancel()

    device.clear_key.assert_awaited_once()
    device.refresh.assert_awaited_once()
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_claim_without_matching_controller_presence_resets_and_is_unroutable() -> None:
    class FakeDevice:
        id = "deck"

        def __init__(self) -> None:
            self.clear_key = AsyncMock()
            self.refresh = AsyncMock()

    async with _deckr() as deckr:
        manager = _factory(deckr)
        device = FakeDevice()
        command_send, command_receive = anyio.create_memory_object_stream(max_buffer_size=100)
        manager._command_streams["deck"] = command_send
        manager._controller_presence_sessions[controller_address("main")] = (
            "different-session"
        )
        main = deckr.lane("hardware_messages").endpoint(controller_address("main"))

        async with (
            command_send,
            command_receive,
            main.subscribe() as main_stream,
            anyio.create_task_group() as tg,
        ):
            tg.start_soon(manager._claim_watch_loop)
            tg.start_soon(
                _apply_device_commands,
                device,
                command_receive,
                "mirabox-main",
            )
            await deckr.state().create("claim.device.mirabox-main.deck", _claim())
            with anyio.fail_after(1):
                while device.clear_key.await_count < 1:
                    await anyio.sleep(0.01)

            await manager._handle_device_message(
                hw_messages.hardware_input_message(
                    manager_id="mirabox-main",
                    device_id="deck",
                    body=hw_messages.KeyDownMessage(key_id="0,0"),
                )
            )
            with anyio.move_on_after(0.05) as scope:
                await main_stream.receive()
            tg.cancel_scope.cancel()

    device.clear_key.assert_awaited_once()
    device.refresh.assert_awaited_once()
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_direct_commands_are_applied_only_when_addressed_to_manager() -> None:
    class FakeDevice:
        id = "deck"

        def __init__(self) -> None:
            self.set_image = AsyncMock()
            self.clear_slot = AsyncMock()
            self.sleep_screen = AsyncMock()
            self.wake_screen = AsyncMock()

    async with _deckr() as deckr:
        device = FakeDevice()
        manager = deckr.lane("hardware_messages").endpoint(
            hardware_manager_address("mirabox-main")
        )
        controller = deckr.lane("hardware_messages").endpoint(controller_address("main"))
        command_send, command_receive = (
            anyio.create_memory_object_stream[DeckrMessage](max_buffer_size=100)
        )
        command_streams = {"deck": command_send}

        async with command_send, command_receive, anyio.create_task_group() as tg:
            tg.start_soon(
                _manager_command_subscription,
                manager,
                "mirabox-main",
                command_streams,
            )
            tg.start_soon(
                _apply_device_commands,
                device,
                command_receive,
                "mirabox-main",
            )
            await anyio.sleep(0.01)
            await controller.publish(
                hw_messages.hardware_command_for_control(
                    controller_id="main",
                    ref=hw_messages.HardwareControlRef(
                        manager_id="other",
                        device_id="deck",
                        control_id="0,0",
                        control_kind="slot",
                    ),
                    message_type=hw_messages.SET_IMAGE,
                    body=hw_messages.SetImageMessage(slot_id="0,0", image=b"wrong"),
                )
            )
            await anyio.sleep(0.05)
            device.set_image.assert_not_awaited()

            await controller.publish(
                hw_messages.hardware_message(
                    sender=controller.endpoint,
                    recipient=endpoint_target(manager.endpoint),
                    subject=hw_messages.hardware_subject(
                        manager_id="other",
                        device_id="deck",
                        control_id="0,0",
                        control_kind="slot",
                    ),
                    message_type=hw_messages.SET_IMAGE,
                    body=hw_messages.SetImageMessage(slot_id="0,0", image=b"wrong"),
                )
            )
            await anyio.sleep(0.05)
            device.set_image.assert_not_awaited()

            await controller.publish(
                hw_messages.hardware_command_for_control(
                    controller_id="main",
                    ref=hw_messages.HardwareControlRef(
                        manager_id="mirabox-main",
                        device_id="deck",
                        control_id="0,0",
                        control_kind="slot",
                    ),
                    message_type=hw_messages.SET_IMAGE,
                    body=hw_messages.SetImageMessage(slot_id="0,0", image=b"ok"),
                )
            )
            with anyio.fail_after(1):
                while device.set_image.await_count < 1:
                    await anyio.sleep(0.01)
            tg.cancel_scope.cancel()

    device.set_image.assert_awaited_once_with("0,0", b"ok")
