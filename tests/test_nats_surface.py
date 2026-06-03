from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import anyio
import deckr.hardware.messages as hw_messages
import pytest
from deckr.components import (
    ComponentState,
    resolve_component_host_plan,
    start_components,
)
from deckr.concord import ContractValidityStatus
from deckr.contracts.messages import controller_address, hardware_manager_address
from deckr.core.config import ConfigDocument
from deckr.hardware import (
    HARDWARE_CLAIM_PROFILE_ID,
    HARDWARE_FEATURE_ID,
    CapabilityDescriptor,
    ControlDescriptor,
    DeviceDescriptor,
    DeviceRef,
    HardwareBeaconPayload,
    HardwareClaimDevice,
    HardwareClaimTerms,
)
from message_bus_mocks import mock_deckr

from deckr.drivers.mirabox import _factory as factory_module
from deckr.drivers.mirabox._discovery import DeviceConnected, ResetDeviceCommand

pytestmark = pytest.mark.asyncio


def _document(*, labels: dict[str, str] | None = None) -> ConfigDocument:
    config: dict[str, Any] = {}
    if labels is not None:
        config["labels"] = labels
    return ConfigDocument(
        raw={
            "deckr": {
                "components": {
                    "instances": {
                        "mirabox": {
                            "component": "dev.deckr.hardware.mirabox",
                            "instance_id": "main",
                            "endpoints": {"hardware_manager": "mirabox-main"},
                            "config": config,
                        }
                    }
                }
            }
        },
        source_path=None,
        base_dir=Path.cwd(),
    )


def _device() -> DeviceDescriptor:
    return DeviceDescriptor(
        deviceId="deck",
        displayName="Test MiraBox",
        fingerprint="serial-a",
        controls=(
            ControlDescriptor(
                controlId="0,0",
                kind="key",
                outputCapabilities=(
                    CapabilityDescriptor(
                        capabilityId="raster.bitmap",
                        family="dev.deckr.output.raster",
                        type="bitmap",
                        direction="output",
                        access=("settable",),
                        commandTypes=("set_frame", "clear"),
                    ),
                ),
            ),
        ),
    )


class FakeDiscovery:
    def __init__(self) -> None:
        self.send, self.receive = anyio.create_memory_object_stream(20)
        self.kwargs: dict[str, object] = {}

    @asynccontextmanager
    async def __call__(self, **kwargs):
        self.kwargs = kwargs
        async with self.receive:
            yield self.receive


@asynccontextmanager
async def _running_component(monkeypatch, *, labels: dict[str, str] | None = None):
    fake_discovery = FakeDiscovery()
    monkeypatch.setattr(factory_module, "discover_mirabox_devices", fake_discovery)
    plan = resolve_component_host_plan(
        _document(labels=labels),
        definitions={"dev.deckr.hardware.mirabox": factory_module.component},
    )
    deckr_cm = mock_deckr(
        lane_contracts=plan.lane_contracts,
        lanes=plan.lane_names,
    )
    deckr = await deckr_cm.__aenter__()
    component_cm = start_components(deckr, plan)
    component_host = await component_cm.__aenter__()
    factory = component_host.components[0]
    try:
        await component_host.component_manager.wait_for_state(
            "dev.deckr.hardware.mirabox:main",
            ComponentState.RUNNING,
        )
        with anyio.fail_after(1):
            while factory._runtime is None:
                await anyio.sleep(0.01)
        with anyio.fail_after(1):
            while "manager_id" not in fake_discovery.kwargs:
                await anyio.sleep(0.01)
        yield deckr, component_host, factory, fake_discovery
    finally:
        await component_cm.__aexit__(None, None, None)
        await deckr_cm.__aexit__(None, None, None)


async def _claim(factory, concord, controller_endpoint):
    runtime = factory._runtime
    assert runtime is not None
    terms = HardwareClaimTerms(
        claimId="claim-1",
        controllerEndpoint=controller_endpoint.address,
        managerEndpoint=hardware_manager_address("mirabox-main"),
        devices=(
            HardwareClaimDevice(
                deviceRef=DeviceRef(
                    managerId="mirabox-main",
                    deviceId="deck",
                    fingerprint="serial-a",
                ),
                instanceCount=1,
            ),
        ),
    )
    contract = await concord._create_contract(
        (controller_endpoint.address, hardware_manager_address("mirabox-main")),
        contract_id="claim-1",
        profile=HARDWARE_CLAIM_PROFILE_ID,
        terms=terms,
        created_by=controller_endpoint.address,
    )
    await concord._attach(
        contract,
        controller_endpoint.address,
        controller_endpoint.session_id,
    )
    await concord.wait_current()
    await runtime.reconcile_claims(reason="test")
    return contract


async def test_mirabox_advertises_hardware_and_routes_claimed_input(monkeypatch):
    async with _running_component(
        monkeypatch,
        labels={"room": "office"},
    ) as (deckr, _host, factory, fake_discovery):
        runtime = factory._runtime
        assert runtime is not None
        assert fake_discovery.kwargs["manager_id"] == "mirabox-main"
        assert fake_discovery.kwargs["sender_session_id"] == runtime.endpoint.session_id

        await fake_discovery.send.send(DeviceConnected(_device()))
        with anyio.fail_after(1):
            while "deck" not in runtime.devices:
                await anyio.sleep(0.01)

        await deckr.beacon.wait_current()
        candidates = deckr.beacon.candidates(HARDWARE_FEATURE_ID)
        payload = HardwareBeaconPayload.model_validate(
            candidates[0].advertisement.payload
        )
        assert payload.labels == {"room": "office"}
        assert payload.devices["deck"].descriptor == _device()

        async with deckr.endpoint(controller_address("controller-main")) as controller:
            contract = await _claim(factory, deckr.concord, controller)
            assert (await deckr.concord._validate(contract)).status == (
                ContractValidityStatus.VALID
            )

            deckr._message_bus.publish.reset_mock()
            await fake_discovery.send.send(
                hw_messages.control_input_message(
                    manager_id="mirabox-main",
                    sender_session_id=runtime.endpoint.session_id,
                    device_id="deck",
                    fingerprint="serial-a",
                    control_id="0,0",
                    capability_id="raster.bitmap",
                    event_type="press",
                    value={"eventType": "press"},
                )
            )
            with anyio.fail_after(1):
                while not deckr._message_bus.publish.called:
                    await anyio.sleep(0.01)
            routed = deckr._message_bus.publish.call_args.args[0]
            assert routed.recipient.endpoint == controller.address
            assert routed.recipient_session_id == controller.session_id


async def test_mirabox_authorized_commands_and_claim_loss_reset(monkeypatch):
    async with _running_component(monkeypatch) as (
        deckr,
        _host,
        factory,
        fake_discovery,
    ):
        runtime = factory._runtime
        assert runtime is not None
        await fake_discovery.send.send(DeviceConnected(_device()))
        with anyio.fail_after(1):
            while "deck" not in runtime.devices:
                await anyio.sleep(0.01)

        command_send, command_receive = anyio.create_memory_object_stream(10)
        async with (
            command_send,
            command_receive,
            deckr.endpoint(controller_address("controller-main")) as controller,
        ):
            factory._command_streams["deck"] = command_send
            contract = await _claim(factory, deckr.concord, controller)

            command = hw_messages.control_command_message(
                controller_id="controller-main",
                sender_session_id=controller.session_id,
                manager_id="mirabox-main",
                device_id="deck",
                control_id="0,0",
                capability_id="raster.bitmap",
                command_type="clear",
            )
            assert await runtime.handle_command(command)
            with anyio.fail_after(1):
                assert await command_receive.receive() == command

            await deckr.concord._cancel(contract, controller.address, reason="test")
            await deckr.concord.wait_current()
            await runtime.reconcile_claims(reason="test cancel")
            with anyio.fail_after(1):
                assert isinstance(await command_receive.receive(), ResetDeviceCommand)
