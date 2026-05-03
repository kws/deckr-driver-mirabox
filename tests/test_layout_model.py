from __future__ import annotations

import pytest
import yaml
from deckr.hardware.capabilities import encoder_relative_value_schema

from deckr.drivers.mirabox._protocol import InteractionEvent
from deckr.drivers.mirabox.layouts import BUILD_IN_LAYOUT_PATH
from deckr.drivers.mirabox.layouts._data import KeyEvents, Layout


def test_key_events_require_exactly_one_field() -> None:
    with pytest.raises(ValueError):
        KeyEvents.model_validate({"key": 1, "press": 2})

    with pytest.raises(ValueError):
        KeyEvents.model_validate({})


def test_builtin_layout_validates_and_exposes_controls() -> None:
    layout_data = yaml.safe_load((BUILD_IN_LAYOUT_PATH / "device-msd-two.yml").read_text())

    layout = Layout.model_validate(layout_data)
    controls = layout.get_controls()

    assert layout.name == "MSD_TWO"
    encoder = next(
        cap
        for control in controls
        for cap in control.input_capabilities
        if cap.capability_id == "encoder.relative"
    )
    assert encoder.value_schema == encoder_relative_value_schema()
    assert any(control.control_id == "0,0" for control in controls)


def test_builtin_layout_emits_signed_encoder_delta() -> None:
    layout_data = yaml.safe_load((BUILD_IN_LAYOUT_PATH / "device-msd-two.yml").read_text())
    layout = Layout.model_validate(layout_data)

    clockwise = list(layout.to_control_input(InteractionEvent(button_id=81, payload=0), None))
    counterclockwise = list(
        layout.to_control_input(InteractionEvent(button_id=80, payload=0), None)
    )

    assert clockwise[0].value == {"delta": 1, "direction": "clockwise"}
    assert counterclockwise[0].value == {
        "delta": -1,
        "direction": "counterclockwise",
    }
