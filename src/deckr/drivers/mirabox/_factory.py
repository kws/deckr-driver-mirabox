from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime

import anyio
from deckr.components import (
    BaseComponent,
    ComponentContext,
    ComponentDefinition,
    ComponentManifest,
    RunContext,
)
from deckr.contracts.messages import (
    EndpointAddress,
    endpoint_target,
    hardware_manager_address,
)
from deckr.hardware import messages as hw_messages
from deckr.lanes import EndpointLane, Lane
from deckr.state import (
    DeviceClaim,
    EndpointPresence,
    HardwareInventory,
    HardwareInventoryDevice,
    StateConflict,
    StateStore,
    encode_key_token,
    hardware_inventory_key,
    parse_device_claim_key,
    parse_presence_endpoint_key,
    presence_endpoint_key,
)

from deckr.drivers.mirabox._discovery import (
    DeviceCommand,
    ResetDeviceCommand,
    discover_mirabox_devices,
)

logger = logging.getLogger(__name__)

PRESENCE_HEARTBEAT_SECONDS = 5.0
PRESENCE_TTL_SECONDS = 15


class MiraboxDeviceFactory(BaseComponent):
    def __init__(self, hardware_lane: Lane, state: StateStore, *, manager_id: str):
        super().__init__("mirabox_device_factory")
        self._hardware_lane = hardware_lane
        self._state = state
        self.manager_id = manager_id
        self._session_id = str(uuid.uuid4())
        self._cancel_scope: anyio.CancelScope | None = None
        self._endpoint: EndpointLane | None = None
        self._devices: dict[str, hw_messages.HardwareDevice] = {}
        self._claims: dict[str, DeviceClaim] = {}
        self._controller_presence_sessions: dict[EndpointAddress, str] = {}
        self._unroutable_devices: set[str] = set()
        self._command_streams: dict[str, anyio.abc.ObjectSendStream[DeviceCommand]] = {}
        self._presence_revision: int | None = None
        self._inventory_revision: int | None = None

    async def start(self, ctx: RunContext) -> None:
        self._endpoint = self._hardware_lane.endpoint(
            hardware_manager_address(self.manager_id)
        )
        self._cancel_scope = ctx.tg.cancel_scope
        ctx.tg.start_soon(self._presence_loop)
        ctx.tg.start_soon(self._claim_watch_loop)
        ctx.tg.start_soon(self._controller_presence_loop)
        ctx.tg.start_soon(self._discovery_loop)

    async def stop(self) -> None:
        with anyio.CancelScope(shield=True):
            if self._cancel_scope is not None:
                self._cancel_scope.cancel()
            self._devices.clear()
            self._claims.clear()
            self._unroutable_devices.clear()
            await self._withdraw_presence()
            await self._withdraw_inventory()

    async def _presence_loop(self) -> None:
        if self._endpoint is None:
            return
        key = presence_endpoint_key(
            lane=self._endpoint.lane.name,
            endpoint=self._endpoint.endpoint,
        )
        try:
            while True:
                entry = await self._state.put(
                    key,
                    EndpointPresence(
                        endpoint=self._endpoint.endpoint,
                        lane=self._endpoint.lane.name,
                        sessionId=self._session_id,
                        timestamp=datetime.now(UTC),
                        ttlSeconds=PRESENCE_TTL_SECONDS,
                        metadata={"runtime": "deckr-driver-mirabox-python"},
                    ),
                    ttl=PRESENCE_TTL_SECONDS,
                )
                self._presence_revision = entry.revision
                await self._publish_inventory()
                await anyio.sleep(PRESENCE_HEARTBEAT_SECONDS)
        finally:
            await self._withdraw_presence()

    async def _withdraw_presence(self) -> None:
        if self._endpoint is None:
            return
        key = presence_endpoint_key(
            lane=self._endpoint.lane.name,
            endpoint=self._endpoint.endpoint,
        )
        with anyio.CancelScope(shield=True):
            revision = self._presence_revision
            if revision is None:
                return
            try:
                await self._state.delete(key, revision=revision)
                self._presence_revision = None
            except StateConflict:
                logger.debug("MiraBox manager presence changed before withdrawal")
            except Exception:
                logger.debug("Failed to withdraw MiraBox manager presence", exc_info=True)

    async def _discovery_loop(self) -> None:
        if self._endpoint is None:
            return
        async with discover_mirabox_devices(
            self._endpoint,
            manager_id=self.manager_id,
            command_streams=self._command_streams,
        ) as stream:
            async for message in stream:
                await self._handle_device_message(message)

    async def _handle_device_message(self, message) -> None:
        if self._endpoint is None:
            return
        event = hw_messages.hardware_body_from_message(message)
        ref = hw_messages.hardware_device_ref_from_message(message)
        if ref is None:
            return
        if isinstance(event, hw_messages.DeviceConnectedMessage):
            self._devices[ref.device_id] = event.device
            await self._publish_inventory()
            return
        if isinstance(event, hw_messages.DeviceDisconnectedMessage):
            self._devices.pop(ref.device_id, None)
            self._claims.pop(ref.device_id, None)
            self._unroutable_devices.discard(ref.device_id)
            await self._publish_inventory()
            return
        if not isinstance(event, hw_messages.HARDWARE_INPUT_MESSAGE_TYPES):
            return
        recipient = self._claim_recipient(ref.device_id)
        if recipient is None:
            logger.debug(
                "Dropping unclaimed MiraBox input for %s/%s",
                ref.manager_id,
                ref.device_id,
            )
            return
        await self._endpoint.publish(
            hw_messages.hardware_message(
                sender=self._endpoint.endpoint,
                recipient=endpoint_target(recipient),
                message_type=message.message_type,
                body=event,
                subject=message.subject,
                causation_id=message.causation_id,
            )
        )

    async def _publish_inventory(self) -> None:
        if self._endpoint is None:
            return
        entry = await self._state.put(
            hardware_inventory_key(self.manager_id),
            HardwareInventory(
                managerId=self.manager_id,
                managerEndpoint=self._endpoint.endpoint,
                sessionId=self._session_id,
                timestamp=datetime.now(UTC),
                ttlSeconds=PRESENCE_TTL_SECONDS,
                devices={
                    device_id: HardwareInventoryDevice(
                        deviceId=device_id,
                        hardwareType=device.name or "mirabox",
                        fingerprint=device.fingerprint,
                        descriptor=device.model_dump(
                            by_alias=True,
                            exclude_none=True,
                            mode="json",
                        ),
                    )
                    for device_id, device in sorted(self._devices.items())
                },
            ),
            ttl=PRESENCE_TTL_SECONDS,
        )
        self._inventory_revision = entry.revision

    async def _withdraw_inventory(self) -> None:
        revision = self._inventory_revision
        if revision is None:
            return
        with anyio.CancelScope(shield=True):
            try:
                await self._state.delete(
                    hardware_inventory_key(self.manager_id),
                    revision=revision,
                )
                self._inventory_revision = None
            except StateConflict:
                logger.debug("MiraBox inventory changed before withdrawal")
            except Exception:
                logger.debug("Failed to withdraw MiraBox inventory", exc_info=True)

    async def _claim_watch_loop(self) -> None:
        prefix = f"claim.device.{encode_key_token(self.manager_id)}."
        async with self._state.watch(prefix) as stream:
            async for change in stream:
                parsed = parse_device_claim_key(change.key)
                if parsed is None:
                    continue
                manager_id, device_id = parsed
                if manager_id != self.manager_id:
                    continue
                if change.entry is None:
                    await self._remove_claim(device_id, reset=True)
                    continue
                try:
                    claim = DeviceClaim.model_validate(change.entry.value)
                except ValueError:
                    await self._remove_claim(device_id, reset=True)
                    continue
                self._claims[device_id] = claim
                if self._claim_recipient(device_id) is None:
                    await self._mark_unroutable(device_id)
                else:
                    self._unroutable_devices.discard(device_id)

    async def _controller_presence_loop(self) -> None:
        prefix = ".".join(
            (
                "presence",
                "endpoint",
                encode_key_token("hardware_messages"),
                encode_key_token("controller"),
                "",
            )
        )
        async with self._state.watch(prefix) as stream:
            async for change in stream:
                parsed = parse_presence_endpoint_key(change.key)
                if parsed is None:
                    continue
                lane, endpoint = parsed
                if lane != "hardware_messages" or endpoint.family != "controller":
                    continue
                if change.entry is None:
                    self._controller_presence_sessions.pop(endpoint, None)
                    await self._reset_claims_for_controller(endpoint)
                    continue
                try:
                    presence = EndpointPresence.model_validate(change.entry.value)
                except ValueError:
                    self._controller_presence_sessions.pop(endpoint, None)
                    await self._reset_claims_for_controller(endpoint)
                    continue
                self._controller_presence_sessions[endpoint] = presence.session_id
                await self._reset_unroutable_claims_for_controller(endpoint)

    def _claim_recipient(self, device_id: str) -> EndpointAddress | None:
        claim = self._claims.get(device_id)
        if claim is None:
            return None
        session_id = self._controller_presence_sessions.get(claim.claimed_by_endpoint)
        if session_id != claim.claimed_by_session_id:
            return None
        return claim.claimed_by_endpoint

    async def _remove_claim(self, device_id: str, *, reset: bool) -> None:
        self._claims.pop(device_id, None)
        self._unroutable_devices.discard(device_id)
        if reset:
            await self._reset_device(device_id)

    async def _mark_unroutable(self, device_id: str) -> None:
        if device_id in self._unroutable_devices:
            return
        self._unroutable_devices.add(device_id)
        await self._reset_device(device_id)

    async def _reset_claims_for_controller(self, endpoint: EndpointAddress) -> None:
        for device_id, claim in tuple(self._claims.items()):
            if claim.claimed_by_endpoint == endpoint:
                await self._mark_unroutable(device_id)

    async def _reset_unroutable_claims_for_controller(
        self,
        endpoint: EndpointAddress,
    ) -> None:
        for device_id, claim in tuple(self._claims.items()):
            if claim.claimed_by_endpoint != endpoint:
                continue
            if self._claim_recipient(device_id) is None:
                await self._mark_unroutable(device_id)
            else:
                self._unroutable_devices.discard(device_id)

    async def _reset_device(self, device_id: str) -> None:
        stream = self._command_streams.get(device_id)
        if stream is None:
            return
        try:
            await stream.send(ResetDeviceCommand())
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            logger.debug("Could not reset closed MiraBox device session %s", device_id)


def driver_factory(
    hardware_lane: Lane,
    state: StateStore,
    *,
    manager_id: str,
) -> MiraboxDeviceFactory:
    return MiraboxDeviceFactory(
        hardware_lane=hardware_lane,
        state=state,
        manager_id=manager_id,
    )


def component_factory(context: ComponentContext) -> MiraboxDeviceFactory:
    source = dict(context.raw_config)
    manager_id = str(source.get("manager_id", "")).strip()
    if not manager_id:
        raise ValueError("deckr.drivers.mirabox requires manager_id")
    return driver_factory(
        context.require_lane("hardware_messages"),
        context.state(),
        manager_id=manager_id,
    )


component = ComponentDefinition(
    manifest=ComponentManifest(
        component_id="deckr.drivers.mirabox",
        config_prefix="deckr.drivers.mirabox",
        consumes=("hardware_messages",),
        publishes=("hardware_messages",),
    ),
    factory=component_factory,
)
