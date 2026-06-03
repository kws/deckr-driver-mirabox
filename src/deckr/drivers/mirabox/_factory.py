from __future__ import annotations

import logging
import re
import socket
from collections.abc import Mapping

import anyio
import deckr.hardware.messages as hw_messages
from deckr.components import (
    BaseComponent,
    ComponentContext,
    ComponentDefinition,
    ComponentManifest,
    ReadinessState,
    RunContext,
)
from deckr.contracts.messages import DeckrMessage
from deckr.hardware.runtime import HardwareManagerRuntime

from deckr.drivers.mirabox._discovery import (
    DeviceCommand,
    DeviceConnected,
    DeviceDisconnected,
    DeviceDiscoveryEvent,
    ResetDeviceCommand,
    discover_mirabox_devices,
)

logger = logging.getLogger(__name__)

_DEFAULT_MANAGER_PREFIX = "mirabox-python"
_INVALID_MANAGER_ID_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


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


def _labels_from_config(config: Mapping[str, object] | None) -> dict[str, str]:
    raw = dict(config or {}).get("labels", {})
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError("MiraBox manager config.labels must be a table")
    labels: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("MiraBox manager config.labels keys must be strings")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"MiraBox manager config.labels.{key} must be a non-empty string"
            )
        labels[key.strip()] = value.strip()
    return labels


class MiraboxDeviceFactory(BaseComponent):
    def __init__(
        self,
        context: ComponentContext,
        *,
        labels: Mapping[str, str] | None = None,
    ):
        super().__init__(context.runtime_name)
        self._context = context
        self.manager_id = context.require_endpoint_id("hardware_manager")
        self._labels = dict(labels or {})
        self._runtime: HardwareManagerRuntime | None = None
        self._command_streams: dict[str, anyio.abc.ObjectSendStream[DeviceCommand]] = {}
        self._stopping: anyio.Event | None = None

    async def start(self, ctx: RunContext) -> None:
        self._stopping = ctx.stopping
        ctx.start_task(self._run, ctx, name=f"{self.name}.mirabox")

    async def _run(self, ctx: RunContext) -> None:
        async with self._context.open_endpoint(
            "hardware_manager",
            metadata={"runtime": "deckr-driver-mirabox-python"},
        ) as endpoint:
            self.manager_id = endpoint.address.endpoint_id
            runtime = HardwareManagerRuntime(
                endpoint=endpoint,
                beacon=self._context.require_beacon(),
                concord=self._context.require_concord(),
                manager_id=self.manager_id,
                labels=self._labels,
                command_handler=self._send_device_command,
                reset_handler=self._reset_device,
            )
            self._runtime = runtime
            try:
                await runtime.start(ctx.tg)
                ctx.start_task(
                    self._discovery_loop,
                    endpoint.session_id,
                    name=f"{self.name}.discovery",
                )
                await ctx.report_status(
                    ReadinessState.READY,
                    diagnostics={"manager_id": self.manager_id},
                )
                await ctx.stopping.wait()
            finally:
                self._command_streams.clear()
                await self._stop_runtime()
                await ctx.report_status(
                    ReadinessState.UNREADY,
                    reasons=("stopped",),
                    diagnostics={"manager_id": self.manager_id},
                )

    async def stop(self) -> None:
        stopping = self._stopping
        if stopping is not None:
            stopping.set()
        with anyio.CancelScope(shield=True):
            self._command_streams.clear()
            await self._stop_runtime()

    async def _stop_runtime(self) -> None:
        runtime = self._runtime
        self._runtime = None
        if runtime is not None:
            await runtime.stop()

    async def _discovery_loop(self, sender_session_id: str) -> None:
        async with discover_mirabox_devices(
            manager_id=self.manager_id,
            sender_session_id=sender_session_id,
            command_streams=self._command_streams,
        ) as stream:
            async for event in stream:
                await self._handle_device_event(event)

    async def _handle_device_event(self, event: DeviceDiscoveryEvent) -> None:
        if self._runtime is None:
            return
        if isinstance(event, DeviceConnected):
            await self._runtime.set_device(event.descriptor)
        elif isinstance(event, DeviceDisconnected):
            await self._runtime.remove_device(event.device_id, reason=event.reason)
        else:
            await self._runtime.handle_hardware_message(event)

    async def _send_device_command(self, envelope: DeckrMessage) -> bool | None:
        ref = hw_messages.hardware_device_ref_from_message(envelope)
        if ref is None or ref.manager_id != self.manager_id:
            return None
        message = hw_messages.hardware_body_from_message(envelope)
        if not isinstance(message, hw_messages.ControlCommandMessage):
            return None
        command_stream = self._command_streams.get(ref.device_id)
        if command_stream is None:
            logger.debug(
                "Dropping command for closed MiraBox device %s/%s",
                ref.manager_id,
                ref.device_id,
            )
            return False
        await command_stream.send(envelope)
        return True

    async def _reset_device(self, device_id: str) -> None:
        stream = self._command_streams.get(device_id)
        if stream is None:
            return
        await stream.send(ResetDeviceCommand())


def driver_factory(context: ComponentContext) -> MiraboxDeviceFactory:
    return MiraboxDeviceFactory(
        context=context,
        labels=_labels_from_config(context.config),
    )


def component_factory(context: ComponentContext) -> MiraboxDeviceFactory:
    return driver_factory(context)


component = ComponentDefinition(
    manifest=ComponentManifest(
        component_id="dev.deckr.hardware.mirabox",
        consumes=("hardware_messages",),
        publishes=("hardware_messages",),
        endpoint_slots=("hardware_manager",),
        role="hardware_manager",
    ),
    factory=component_factory,
)
