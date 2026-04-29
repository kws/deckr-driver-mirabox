from __future__ import annotations

import logging
import re
import socket
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
from deckr.hardware.descriptors import DeviceDescriptor, DeviceRef
from deckr.lanes import EndpointLane, Lane
from deckr.state import (
    DeviceClaim,
    EndpointPresence,
    HardwareInventory,
    HardwareInventoryDevice,
    StateConflict,
    StateStore,
    StateUnavailable,
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
_STATE_RECONCILE_SECONDS = 1.0
_WATCH_RETRY_SECONDS = 1.0
_DEFAULT_MANAGER_PREFIX = "mirabox-python"
_INVALID_MANAGER_ID_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_CONTROLLER_PRESENCE_PREFIX = ".".join(
    (
        "presence",
        "endpoint",
        encode_key_token("hardware_messages"),
        encode_key_token("controller"),
        "",
    )
)


def _normalize_manager_id_part(value: str) -> str:
    normalized = _INVALID_MANAGER_ID_CHARS.sub("-", value.strip())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-._")
    return normalized or "local"


def default_manager_id(*, hostname: str | None = None) -> str:
    host = socket.gethostname() if hostname is None else hostname
    return f"{_DEFAULT_MANAGER_PREFIX}-{_normalize_manager_id_part(host)}"


def resolve_manager_id(value: str | None = None) -> str:
    manager_id = str(value).strip() if value is not None else ""
    return manager_id or default_manager_id()


class MiraboxDeviceFactory(BaseComponent):
    def __init__(self, hardware_lane: Lane, state: StateStore, *, manager_id: str):
        super().__init__("mirabox_device_factory")
        self._hardware_lane = hardware_lane
        self._state = state
        self.manager_id = manager_id
        self._session_id = str(uuid.uuid4())
        self._cancel_scope: anyio.CancelScope | None = None
        self._endpoint: EndpointLane | None = None
        self._devices: dict[str, DeviceDescriptor] = {}
        self._claims: dict[str, DeviceClaim] = {}
        self._controller_presence_sessions: dict[EndpointAddress, str] = {}
        self._unroutable_devices: set[str] = set()
        self._command_streams: dict[str, anyio.abc.ObjectSendStream[DeviceCommand]] = {}
        self._presence_revision: int | None = None
        self._inventory_revision: int | None = None
        self._routing_reconcile_lock = anyio.Lock()

    async def start(self, ctx: RunContext) -> None:
        self._endpoint = self._hardware_lane.endpoint(
            hardware_manager_address(self.manager_id)
        )
        self._cancel_scope = ctx.tg.cancel_scope
        ctx.tg.start_soon(self._presence_loop)
        ctx.tg.start_soon(self._claim_watch_loop)
        ctx.tg.start_soon(self._controller_presence_loop)
        ctx.tg.start_soon(self._routing_reconciliation_loop)
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
        while True:
            try:
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
                await self._publish_inventory_safely()
            except StateUnavailable:
                logger.warning(
                    "MiraBox manager current state is unavailable; heartbeat will retry",
                    exc_info=True,
                )
            await anyio.sleep(PRESENCE_HEARTBEAT_SECONDS)

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
            except StateUnavailable:
                logger.warning(
                    "Failed to withdraw MiraBox manager presence",
                    exc_info=True,
                )

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
        if isinstance(event, hw_messages.DeviceAvailableMessage):
            self._devices[ref.device_id] = event.descriptor
            await self._publish_inventory_safely()
            await self._endpoint.publish(message)
            return
        if isinstance(event, hw_messages.DeviceDescriptorChangedMessage):
            self._devices[ref.device_id] = event.descriptor
            await self._publish_inventory_safely()
            await self._endpoint.publish(message)
            return
        if isinstance(event, hw_messages.DeviceUnavailableMessage):
            self._devices.pop(ref.device_id, None)
            self._claims.pop(ref.device_id, None)
            self._unroutable_devices.discard(ref.device_id)
            await self._publish_inventory_safely()
            await self._endpoint.publish(message)
            return
        if not isinstance(
            event,
            hw_messages.ControlInputMessage | hw_messages.CapabilityStateChangedMessage,
        ):
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
                        deviceRef=DeviceRef(
                            managerId=self.manager_id,
                            deviceId=device_id,
                            fingerprint=device.fingerprint,
                        ),
                        descriptor=device,
                    )
                    for device_id, device in sorted(self._devices.items())
                },
            ),
            ttl=PRESENCE_TTL_SECONDS,
        )
        self._inventory_revision = entry.revision

    async def _publish_inventory_safely(self) -> None:
        try:
            await self._publish_inventory()
        except StateUnavailable:
            logger.warning(
                "MiraBox inventory current state is unavailable; heartbeat will retry",
                exc_info=True,
            )

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
            except StateUnavailable:
                logger.warning("Failed to withdraw MiraBox inventory", exc_info=True)

    async def _claim_watch_loop(self) -> None:
        prefix = f"claim.device.{encode_key_token(self.manager_id)}."
        while True:
            try:
                async with self._state.watch(prefix) as stream:
                    async for change in stream:
                        parsed = parse_device_claim_key(change.key)
                        if parsed is None:
                            continue
                        manager_id, device_id = parsed
                        if manager_id != self.manager_id:
                            continue
                        await self._reconcile_routing_current_state(
                            reason="device claim watch"
                        )
            except StateUnavailable:
                logger.warning(
                    "MiraBox device claim state is unavailable; watch will retry",
                    exc_info=True,
                )
                await anyio.sleep(_WATCH_RETRY_SECONDS)

    async def _controller_presence_loop(self) -> None:
        while True:
            try:
                async with self._state.watch(_CONTROLLER_PRESENCE_PREFIX) as stream:
                    async for change in stream:
                        parsed = parse_presence_endpoint_key(change.key)
                        if parsed is None:
                            continue
                        lane, endpoint = parsed
                        if lane != "hardware_messages" or endpoint.family != "controller":
                            continue
                        await self._reconcile_routing_current_state(
                            reason="controller presence watch"
                        )
            except StateUnavailable:
                logger.warning(
                    "Controller endpoint presence state is unavailable; watch will retry",
                    exc_info=True,
                )
                await anyio.sleep(_WATCH_RETRY_SECONDS)

    async def _routing_reconciliation_loop(self) -> None:
        while True:
            try:
                await self._reconcile_routing_current_state(reason="broker snapshot")
            except StateUnavailable:
                logger.warning(
                    "MiraBox routing current state unavailable; reconciliation will retry",
                    exc_info=True,
                )
            await anyio.sleep(_STATE_RECONCILE_SECONDS)

    async def _reconcile_routing_current_state(self, *, reason: str) -> None:
        async with self._routing_reconcile_lock:
            await self._reconcile_routing_current_state_locked(reason=reason)

    async def _reconcile_routing_current_state_locked(self, *, reason: str) -> None:
        claim_prefix = f"claim.device.{encode_key_token(self.manager_id)}."
        claim_entries = await self._state.items(claim_prefix)
        presence_entries = await self._state.items(_CONTROLLER_PRESENCE_PREFIX)

        next_claims: dict[str, DeviceClaim] = {}
        invalid_claim_devices: set[str] = set()
        next_controller_sessions: dict[EndpointAddress, str] = {}

        for entry in claim_entries:
            parsed = parse_device_claim_key(entry.key)
            if parsed is None:
                continue
            manager_id, device_id = parsed
            if manager_id != self.manager_id:
                continue
            try:
                next_claims[device_id] = DeviceClaim.model_validate(entry.value)
            except ValueError:
                logger.warning("Ignoring invalid MiraBox device claim %s", entry.key)
                invalid_claim_devices.add(device_id)

        for entry in presence_entries:
            parsed = parse_presence_endpoint_key(entry.key)
            if parsed is None:
                continue
            lane, endpoint = parsed
            if lane != "hardware_messages" or endpoint.family != "controller":
                continue
            try:
                presence = EndpointPresence.model_validate(entry.value)
            except ValueError:
                logger.warning("Ignoring invalid controller presence %s", entry.key)
                continue
            if presence.endpoint != endpoint or presence.lane != lane:
                logger.warning(
                    "Ignoring controller presence %s with mismatched payload",
                    entry.key,
                )
                continue
            next_controller_sessions[endpoint] = presence.session_id

        logger.debug("Reconciling MiraBox routing current state via %s", reason)
        devices_to_reset = self._devices_to_reset_for_routing_snapshot(
            next_claims,
            next_controller_sessions,
            invalid_claim_devices,
        )
        self._claims = next_claims
        self._controller_presence_sessions = next_controller_sessions
        self._unroutable_devices = {
            device_id
            for device_id, claim in next_claims.items()
            if _claim_recipient(claim, next_controller_sessions) is None
        }
        for device_id in sorted(devices_to_reset):
            await self._reset_device(device_id)

    def _devices_to_reset_for_routing_snapshot(
        self,
        next_claims: dict[str, DeviceClaim],
        next_controller_sessions: dict[EndpointAddress, str],
        invalid_claim_devices: set[str],
    ) -> set[str]:
        devices_to_reset = set(invalid_claim_devices)
        for device_id, old_claim in self._claims.items():
            next_claim = next_claims.get(device_id)
            if next_claim is None:
                devices_to_reset.add(device_id)
                continue
            if _claim_route_identity(old_claim) != _claim_route_identity(next_claim):
                devices_to_reset.add(device_id)
                continue
            if (
                _claim_recipient(old_claim, self._controller_presence_sessions)
                is not None
                and _claim_recipient(next_claim, next_controller_sessions) is None
            ):
                devices_to_reset.add(device_id)

        for device_id, next_claim in next_claims.items():
            if (
                device_id not in self._claims
                and _claim_recipient(next_claim, next_controller_sessions) is None
            ):
                devices_to_reset.add(device_id)
        return devices_to_reset

    def _claim_recipient(self, device_id: str) -> EndpointAddress | None:
        claim = self._claims.get(device_id)
        if claim is None:
            return None
        return _claim_recipient(claim, self._controller_presence_sessions)

    async def _reset_device(self, device_id: str) -> None:
        stream = self._command_streams.get(device_id)
        if stream is None:
            return
        try:
            await stream.send(ResetDeviceCommand())
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            logger.debug("Could not reset closed MiraBox device session %s", device_id)


def _claim_route_identity(claim: DeviceClaim) -> tuple[EndpointAddress, str]:
    return claim.claimed_by_endpoint, claim.claimed_by_session_id


def _claim_recipient(
    claim: DeviceClaim,
    controller_presence_sessions: dict[EndpointAddress, str],
) -> EndpointAddress | None:
    session_id = controller_presence_sessions.get(claim.claimed_by_endpoint)
    if session_id != claim.claimed_by_session_id:
        return None
    return claim.claimed_by_endpoint


def driver_factory(
    hardware_lane: Lane,
    state: StateStore,
    *,
    manager_id: str | None = None,
) -> MiraboxDeviceFactory:
    return MiraboxDeviceFactory(
        hardware_lane=hardware_lane,
        state=state,
        manager_id=resolve_manager_id(manager_id),
    )


def component_factory(context: ComponentContext) -> MiraboxDeviceFactory:
    source = dict(context.raw_config)
    return driver_factory(
        context.require_lane("hardware_messages"),
        context.state(),
        manager_id=source.get("manager_id"),
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
