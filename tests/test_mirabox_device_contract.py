from __future__ import annotations

from deckr.hardware.events import WireHWDevice

from deckr.drivers.mirabox._device import MiraBoxDockDevice


class _FakeTransport:
    def __init__(self) -> None:
        self.hid = "0B00:1001:0300D0785616"
        self.descriptor = {"product_string": "MSD_TWO"}


class _FakeLayout:
    def get_slots(self):
        return []


def test_mirabox_device_exposes_hid_for_hw_device_contract():
    device = MiraBoxDockDevice(
        transport=_FakeTransport(),
        protocol=object(),
        layout=_FakeLayout(),
    )

    assert device.id == "0B00:1001:0300D0785616"
    assert device.hid == "0B00:1001:0300D0785616"

    info = WireHWDevice(
        id=device.id,
        hid=device.hid,
        slots=list(device.slots),
        name=getattr(device, "name", None),
    )
    assert info.id == "0B00:1001:0300D0785616"
    assert info.hid == "0B00:1001:0300D0785616"
