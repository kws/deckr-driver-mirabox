from __future__ import annotations

from deckr.hardware.descriptors import DeviceDescriptor

from deckr.drivers.mirabox._device import MiraBoxDockDevice


class _FakeTransport:
    def __init__(self) -> None:
        self.hid = "0B00:1001:0300D0785616"
        self.descriptor = {"product_string": "MSD_TWO", "serial_number": "0300D0785616"}


class _FakeLayout:
    name = "MSD_TWO"

    def get_controls(self):
        return []


def test_mirabox_device_exposes_hid_for_hw_device_contract():
    device = MiraBoxDockDevice(
        transport=_FakeTransport(),
        protocol=object(),
        layout=_FakeLayout(),
    )

    assert device.id == "0B00:1001:0300D0785616"
    assert device.hid == "0B00:1001:0300D0785616"

    info = device.device_descriptor
    assert isinstance(info, DeviceDescriptor)
    assert info.device_id == "0B00:1001:0300D0785616"
    assert info.fingerprint == "0B00:1001:0300D0785616"
