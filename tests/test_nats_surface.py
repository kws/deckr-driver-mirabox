from __future__ import annotations

import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import anyio
import pytest
from deckr.contracts.lanes import CORE_LANE_CONTRACTS, LaneContractRegistry
from deckr.contracts.messages import (
    DeckrMessage,
    EndpointAddress,
    controller_address,
    endpoint_target,
    hardware_manager_address,
)
from deckr.hardware import messages as hw_messages
from deckr.hardware.descriptors import (
    DECKR_INPUT_BUTTON,
    DECKR_OUTPUT_RASTER,
    CapabilityDescriptor,
    CapabilityRef,
    ControlDescriptor,
    ControlGeometry,
    DeviceDescriptor,
    DeviceRef,
)
from deckr.lanes import RegisteredEndpointLane
from deckr.runtime import Deckr
from deckr.state import (
    DeviceClaim,
    EndpointPresence,
    HardwareInventory,
    StateUnavailable,
    hardware_inventory_key,
    presence_endpoint_key,
)
from memory_lane_substrate import MemoryLaneSubstrate

from deckr.drivers.mirabox._discovery import (
    _apply_device_commands,
    _manager_command_subscription,
)
from deckr.drivers.mirabox._factory import (
    MiraboxDeviceFactory,
    default_manager_id,
    resolve_manager_id,
)

MANAGER_SESSION = "manager-session"
CONTROLLER_SESSION = "controller-session"


class EndpointHarness:
    def __init__(
        self,
        deckr: Deckr,
        endpoint: EndpointAddress,
        *,
        session_id: str,
    ) -> None:
        self._state = deckr.state()
        self._registered = RegisteredEndpointLane(
            lane=deckr.lane("hardware_messages"),
            endpoint=endpoint,
            session_id=session_id,
            state=self._state,
            metadata={"runtime": "test"},
        )

    @property
    def lane(self):
        return self._registered.lane

    @property
    def endpoint(self) -> EndpointAddress:
        return self._registered.endpoint

    @property
    def session_id(self) -> str:
        return self._registered.session_id

    async def _ensure_presence(self) -> None:
        await self._state.put(
            presence_endpoint_key(lane=self.lane.name, endpoint=self.endpoint),
            EndpointPresence(
                endpoint=self.endpoint,
                lane=self.lane.name,
                sessionId=self.session_id,
                timestamp=datetime.now(UTC),
                ttlSeconds=15,
            ),
            ttl=15,
        )

    async def publish(self, message: DeckrMessage) -> DeckrMessage:
        await self._ensure_presence()
        return await self._registered.publish(message)

    async def reply_to(self, request: DeckrMessage, **kwargs) -> DeckrMessage:
        await self._ensure_presence()
        return await self._registered.reply_to(request, **kwargs)

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator:
        await self._ensure_presence()
        async with self._registered.subscribe() as stream:
            yield stream


def _endpoint(
    deckr: Deckr,
    endpoint: EndpointAddress,
    *,
    session_id: str = CONTROLLER_SESSION,
) -> EndpointHarness:
    return EndpointHarness(deckr, endpoint, session_id=session_id)


def _deckr() -> Deckr:
    lane_contracts = LaneContractRegistry(CORE_LANE_CONTRACTS.values())
    return Deckr(
        lane_contracts=lane_contracts,
        substrate=MemoryLaneSubstrate(lane_contracts=lane_contracts),
    )


def _control() -> ControlDescriptor:
    return ControlDescriptor(
        controlId="0,0",
        kind="bitmap_key",
        geometry=ControlGeometry(x=0, y=0, width=1, height=1, unit="grid"),
        inputCapabilities=(
            CapabilityDescriptor(
                capabilityId="button.momentary",
                family=DECKR_INPUT_BUTTON,
                type="momentary",
                direction="input",
                access=("emits",),
                eventTypes=("down", "up"),
            ),
            CapabilityDescriptor(
                capabilityId="button.press",
                family=DECKR_INPUT_BUTTON,
                type="activation",
                direction="input",
                access=("emits",),
                eventTypes=("press",),
            ),
        ),
        outputCapabilities=(
            CapabilityDescriptor.model_validate(
                {
                    "capabilityId": "raster.bitmap",
                    "family": DECKR_OUTPUT_RASTER,
                    "type": "bitmap",
                    "direction": "output",
                    "access": ["settable"],
                    "commandTypes": ["set_frame", "clear"],
                    "constraints": [
                        {"type": "fixed", "subject": "width", "value": 72},
                        {"type": "fixed", "subject": "height", "value": 72},
                    ],
                }
            ),
        ),
    )


def _device() -> DeviceDescriptor:
    return DeviceDescriptor(
        deviceId="deck",
        displayName="MiraBox",
        fingerprint="fingerprint:deck",
        controls=(_control(),),
    )


def _available_message() -> DeckrMessage:
    return hw_messages.device_available_message(
        manager_id="mirabox-main",
        sender_session_id=MANAGER_SESSION,
        descriptor=_device(),
    )


def _unavailable_message() -> DeckrMessage:
    return hw_messages.device_unavailable_message(
        manager_id="mirabox-main",
        sender_session_id=MANAGER_SESSION,
        device_id="deck",
        reason="test",
    )


def _input_message() -> DeckrMessage:
    return hw_messages.control_input_message(
        manager_id="mirabox-main",
        sender_session_id=MANAGER_SESSION,
        device_id="deck",
        fingerprint="fingerprint:deck",
        control_id="0,0",
        capability_id="button.momentary",
        event_type="down",
        value={"eventType": "down"},
    )


def _command_message(
    controller_id: str,
    image: bytes,
    *,
    manager_id: str = "mirabox-main",
) -> DeckrMessage:
    return hw_messages.control_command_for_capability(
        controller_id=controller_id,
        sender_session_id=CONTROLLER_SESSION,
        ref=CapabilityRef(
            deviceRef=DeviceRef(managerId=manager_id, deviceId="deck"),
            controlId="0,0",
            capabilityId="raster.bitmap",
        ),
        command_type="set_frame",
        params={
            "image": base64.b64encode(image).decode("ascii"),
            "encoding": "jpeg",
        },
    )


def _power_command_message(controller_id: str, command_type: str) -> DeckrMessage:
    return hw_messages.control_command_for_capability(
        controller_id=controller_id,
        sender_session_id=CONTROLLER_SESSION,
        ref=CapabilityRef(
            deviceRef=DeviceRef(managerId="mirabox-main", deviceId="deck"),
            capabilityId="device.power",
        ),
        command_type=command_type,
        params={},
    )


def _factory(deckr: Deckr) -> MiraboxDeviceFactory:
    manager = MiraboxDeviceFactory(
        deckr.lane("hardware_messages"),
        deckr.state(),
        manager_id="mirabox-main",
    )
    manager._endpoint = _endpoint(
        deckr,
        hardware_manager_address("mirabox-main"),
        session_id=MANAGER_SESSION,
    )
    return manager


def _claim(controller_id: str = "main", session_id: str = "controller-session"):
    return DeviceClaim(
        claimedByEndpoint=controller_address(controller_id),
        claimedBySessionId=session_id,
        timestamp=datetime.now(UTC),
        ttlSeconds=15,
    )


async def _put_controller_presence(
    deckr: Deckr,
    *,
    controller_id: str = "main",
    session_id: str = "controller-session",
) -> None:
    endpoint = controller_address(controller_id)
    await deckr.state().put(
        presence_endpoint_key(lane="hardware_messages", endpoint=endpoint),
        EndpointPresence(
            endpoint=endpoint,
            lane="hardware_messages",
            sessionId=session_id,
            timestamp=datetime.now(UTC),
            ttlSeconds=15,
            metadata={},
        ),
    )


def test_default_manager_id_uses_python_prefix_and_hostname() -> None:
    assert (
        default_manager_id(hostname="deckr-box.local")
        == "mirabox-python-deckr-box.local"
    )


def test_default_manager_id_normalizes_unfriendly_hostname() -> None:
    assert default_manager_id(hostname=" deckr box!! ") == "mirabox-python-deckr-box"
    assert default_manager_id(hostname=":::") == "mirabox-python-local"


def test_resolve_manager_id_keeps_explicit_override() -> None:
    assert resolve_manager_id("kitchen") == "kitchen"


@pytest.mark.asyncio
async def test_connect_and_disconnect_rewrite_aggregate_inventory() -> None:
    async with _deckr() as deckr:
        manager = _factory(deckr)
        await manager._handle_device_message(_available_message())
        entry = await deckr.state().get(hardware_inventory_key("mirabox-main"))
        assert entry is not None
        inventory = HardwareInventory.model_validate(entry.value)
        assert set(inventory.devices) == {"deck"}
        assert inventory.devices["deck"].descriptor.device_id == "deck"

        await manager._handle_device_message(_unavailable_message())
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
        manager._endpoint = _endpoint(
            deckr,
            hardware_manager_address("mirabox-main"),
            session_id=MANAGER_SESSION,
        )

        await manager._handle_device_message(_available_message())

    assert "deck" in manager._devices
    assert manager._inventory_revision is None


@pytest.mark.asyncio
async def test_inventory_publish_writes_aggregate_inventory() -> None:
    async with _deckr() as deckr:
        manager = _factory(deckr)
        manager._devices["deck"] = _device()

        await manager._publish_inventory_safely()
        entry = await deckr.state().get(hardware_inventory_key("mirabox-main"))
        assert entry is not None

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
        main = _endpoint(deckr, controller_address("main"))
        other = _endpoint(deckr, controller_address("other"), session_id="other-session")

        async with main.subscribe() as main_stream, other.subscribe() as other_stream:
            await manager._handle_device_message(_input_message())
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
        command_send, command_receive = anyio.create_memory_object_stream(
            max_buffer_size=100
        )
        manager._command_streams["deck"] = command_send
        await _put_controller_presence(deckr)
        claim_key = "claim.device.mirabox-main.deck"
        main = _endpoint(deckr, controller_address("main"))

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

            await manager._handle_device_message(_input_message())
            with anyio.move_on_after(0.05) as scope:
                await main_stream.receive()
            tg.cancel_scope.cancel()

    device.clear_key.assert_awaited_once()
    device.refresh.assert_awaited_once()
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_broker_snapshot_claim_delete_resets_device_and_drops_input() -> None:
    class FakeDevice:
        id = "deck"

        def __init__(self) -> None:
            self.clear_key = AsyncMock()
            self.refresh = AsyncMock()

    async with _deckr() as deckr:
        manager = _factory(deckr)
        device = FakeDevice()
        command_send, command_receive = anyio.create_memory_object_stream(
            max_buffer_size=100
        )
        manager._command_streams["deck"] = command_send
        await _put_controller_presence(deckr)
        claim_key = "claim.device.mirabox-main.deck"
        await deckr.state().create(claim_key, _claim())
        await manager._reconcile_routing_current_state(reason="test snapshot")
        main = _endpoint(
            deckr,
            controller_address("main"),
            session_id="different-session",
        )

        async with (
            command_send,
            command_receive,
            main.subscribe() as main_stream,
            anyio.create_task_group() as tg,
        ):
            tg.start_soon(
                _apply_device_commands,
                device,
                command_receive,
                "mirabox-main",
            )
            await deckr.state().delete(claim_key)
            await manager._reconcile_routing_current_state(reason="test snapshot")
            with anyio.fail_after(1):
                while device.clear_key.await_count < 1:
                    await anyio.sleep(0.01)

            await manager._handle_device_message(_input_message())
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
        command_send, command_receive = anyio.create_memory_object_stream(
            max_buffer_size=100
        )
        manager._command_streams["deck"] = command_send
        manager._controller_presence_sessions[controller_address("main")] = (
            "different-session"
        )
        main = _endpoint(
            deckr,
            controller_address("main"),
            session_id="different-session",
        )

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

            await manager._handle_device_message(_input_message())
            with anyio.move_on_after(0.05) as scope:
                await main_stream.receive()
            tg.cancel_scope.cancel()

    device.clear_key.assert_awaited_once()
    device.refresh.assert_awaited_once()
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_controller_presence_restore_makes_current_claim_routable() -> None:
    async with _deckr() as deckr:
        manager = _factory(deckr)
        claim_key = "claim.device.mirabox-main.deck"
        await deckr.state().create(claim_key, _claim())
        await manager._reconcile_routing_current_state(reason="test snapshot")
        assert manager._claim_recipient("deck") is None

        await _put_controller_presence(deckr)
        await manager._reconcile_routing_current_state(reason="test snapshot")
        assert manager._claim_recipient("deck") == controller_address("main")

        main = _endpoint(deckr, controller_address("main"))
        async with main.subscribe() as main_stream:
            await manager._handle_device_message(_input_message())
            received = await main_stream.receive()

    assert received.recipient.endpoint == controller_address("main")


@pytest.mark.asyncio
async def test_invalid_claim_payload_is_not_routable() -> None:
    class FakeDevice:
        id = "deck"

        def __init__(self) -> None:
            self.clear_key = AsyncMock()
            self.refresh = AsyncMock()

    async with _deckr() as deckr:
        manager = _factory(deckr)
        device = FakeDevice()
        command_send, command_receive = anyio.create_memory_object_stream(
            max_buffer_size=100
        )
        manager._command_streams["deck"] = command_send
        await deckr.state().put(
            "claim.device.mirabox-main.deck",
            {
                "claimedByEndpoint": "controller:main",
                "timestamp": datetime.now(UTC).isoformat(),
                "ttlSeconds": 15,
            },
        )
        await _put_controller_presence(deckr)
        main = _endpoint(deckr, controller_address("main"))

        async with (
            command_send,
            command_receive,
            main.subscribe() as main_stream,
            anyio.create_task_group() as tg,
        ):
            tg.start_soon(
                _apply_device_commands,
                device,
                command_receive,
                "mirabox-main",
            )
            await manager._reconcile_routing_current_state(reason="test snapshot")
            with anyio.fail_after(1):
                while device.clear_key.await_count < 1:
                    await anyio.sleep(0.01)

            await manager._handle_device_message(_input_message())
            with anyio.move_on_after(0.05) as scope:
                await main_stream.receive()
            tg.cancel_scope.cancel()

    assert "deck" not in manager._claims
    device.clear_key.assert_awaited_once()
    device.refresh.assert_awaited_once()
    assert scope.cancel_called


@pytest.mark.asyncio
async def test_direct_commands_are_applied_only_when_addressed_to_manager() -> None:
    class FakeDevice:
        id = "deck"

        def __init__(self) -> None:
            self.set_raster_frame = AsyncMock()
            self.clear_raster = AsyncMock()
            self.sleep_device = AsyncMock()
            self.wake_device = AsyncMock()

    async with _deckr() as deckr:
        device = FakeDevice()
        manager = _endpoint(
            deckr,
            hardware_manager_address("mirabox-main"),
            session_id=MANAGER_SESSION,
        )
        controller = _endpoint(deckr, controller_address("main"))
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
            await controller.publish(_command_message("main", b"wrong", manager_id="other"))
            await anyio.sleep(0.05)
            device.set_raster_frame.assert_not_awaited()

            other_ref = DeviceRef(managerId="other", deviceId="deck")
            await controller.publish(
                hw_messages.hardware_message(
                    sender=controller.endpoint,
                    sender_session_id=controller.session_id,
                    recipient=endpoint_target(manager.endpoint),
                    subject=hw_messages.hardware_subject_for_capability(
                        CapabilityRef(
                            deviceRef=other_ref,
                            controlId="0,0",
                            capabilityId="raster.bitmap",
                        )
                    ),
                    message_type=hw_messages.CONTROL_COMMAND,
                    body=hw_messages.ControlCommandMessage(
                        deviceRef=other_ref,
                        controlId="0,0",
                        capabilityId="raster.bitmap",
                        commandType="set_frame",
                        params={
                            "image": base64.b64encode(b"wrong").decode("ascii"),
                            "encoding": "jpeg",
                        },
                    ),
                )
            )
            await anyio.sleep(0.05)
            device.set_raster_frame.assert_not_awaited()

            await controller.publish(_command_message("main", b"ok"))
            with anyio.fail_after(1):
                while device.set_raster_frame.await_count < 1:
                    await anyio.sleep(0.01)
            await controller.publish(_power_command_message("main", "sleep"))
            with anyio.fail_after(1):
                while device.sleep_device.await_count < 1:
                    await anyio.sleep(0.01)
            tg.cancel_scope.cancel()

    device.set_raster_frame.assert_awaited_once_with("0,0", b"ok")
    device.sleep_device.assert_awaited_once_with()
