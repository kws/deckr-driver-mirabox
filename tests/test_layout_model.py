from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from deckr.hardware.capabilities import encoder_relative_value_schema

from deckr.drivers.mirabox._protocol import InteractionEvent
from deckr.drivers.mirabox.layouts import (
    BUILD_IN_LAYOUT_PATH,
    parse_layout_file,
    resolve_config_files,
)
from deckr.drivers.mirabox.layouts._data import KeyEvents, Layout
from deckr.drivers.mirabox.layouts._evaluator import eval_policy


def _load_builtin_layouts() -> list[tuple[str, dict]]:
    return [
        (layout_file.name, parse_layout_file(layout_file))
        for layout_file in resolve_config_files()
        if BUILD_IN_LAYOUT_PATH in layout_file.parents
    ]


def _resolve_layout(descriptor: dict, firmware: str) -> Layout:
    matches = []
    for _, layout_data in _load_builtin_layouts():
        if not eval_policy(layout_data["candidate"], descriptor):
            continue
        context = {**descriptor, "firmware": firmware}
        if eval_policy(layout_data["match"], context):
            matches.append(Layout.model_validate(layout_data))
    assert matches
    return matches[0]


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
    key = next(control for control in controls if control.control_id == "0,0")
    assert [
        (capability.capability_id, capability.event_types)
        for capability in key.input_capabilities
    ] == [("button.momentary", ("down", "up"))]


def test_every_builtin_layout_parses_and_declares_protocol_version() -> None:
    for filename, layout_data in _load_builtin_layouts():
        assert "protocol_version" in layout_data, filename
        layout = Layout.model_validate(layout_data)
        assert layout.protocol_version in {1, 2, 3}
        assert layout.teardown_sequence, filename


def test_layout_requires_protocol_version() -> None:
    with pytest.raises(ValueError):
        Layout.model_validate(
            {
                "name": "MISSING_PROTOCOL",
                "candidate": "True",
                "match": "True",
                "image_config": {},
            }
        )


def test_sample_vid_pid_firmware_descriptors_resolve_expected_layouts() -> None:
    samples = [
        (
            {
                "vendor_id": 768,
                "product_id": 4112,
                "usage_page": None,
                "usage": None,
            },
            "V1.AKP153.00.001",
            "AKP153_PROTOCOL_V1",
        ),
        (
            {
                "vendor_id": 768,
                "product_id": 12304,
                "usage_page": 65440,
                "usage": 2,
            },
            "V3.AKP153E.02.009",
            "AKP153",
        ),
        (
            {
                "vendor_id": 768,
                "product_id": 4097,
                "usage_page": None,
                "usage": None,
            },
            "V2.AKP03.01.001",
            "AKP03_PROTOCOL_V2",
        ),
        (
            {
                "vendor_id": 26115,
                "product_id": 4098,
                "usage_page": None,
                "usage": None,
            },
            "V3.N3EN.01.001",
            "AKP03_PROTOCOL_V3",
        ),
        (
            {
                "vendor_id": 2816,
                "product_id": 4097,
                "usage_page": 65440,
                "usage": 2,
            },
            "V25.MSD_TWO.01.005",
            "MSD_TWO",
        ),
    ]

    for descriptor, firmware, expected_name in samples:
        assert _resolve_layout(descriptor, firmware).name == expected_name


def test_akp153_protocol_v1_uses_raster_contract() -> None:
    layout_data = yaml.safe_load(
        (BUILD_IN_LAYOUT_PATH / "device-akp153-protocol-v1.yml").read_text()
    )
    layout = Layout.model_validate(layout_data)
    controls = layout.get_controls()
    key = next(control for control in controls if control.control_id == "0,0")
    raster = key.output_capabilities[0]

    assert layout.protocol_version == 1
    assert [
        (constraint.subject, constraint.value)
        for constraint in raster.constraints
        if constraint.subject in {"width", "height"}
    ] == [("width", 85), ("height", 85)]


def test_layout_image_dimensions_and_rotation_match_contract() -> None:
    akp153 = Layout.model_validate(
        yaml.safe_load((BUILD_IN_LAYOUT_PATH / "device-akp153.yml").read_text())
    )
    akp153_sidebar = akp153.get_control_for_name("5,0")
    assert akp153_sidebar is not None
    assert akp153_sidebar.display.format.width == 82
    assert akp153_sidebar.display.format.height == 82
    assert akp153_sidebar.display.format.rotation == 90

    akp03_v3 = Layout.model_validate(
        yaml.safe_load(
            (BUILD_IN_LAYOUT_PATH / "device-akp03-protocol-v3.yml").read_text()
        )
    )
    key = akp03_v3.get_control_for_name("0,0")
    assert key is not None
    assert key.display.format.width == 60
    assert key.display.format.height == 60
    assert key.display.format.rotation == 90


def test_driver_source_layouts_and_tests_do_not_name_reference_projects() -> None:
    forbidden = [
        "Open" + "Deck",
        "open" + "deck",
        "mira" + "jazz",
        "Mira" + "Jazz",
        "Open " + "Deck",
    ]
    roots = [BUILD_IN_LAYOUT_PATH.parent.parent, Path(__file__).resolve().parent]
    files = [
        path
        for root in roots
        for path in root.rglob("*")
        if path.is_file() and path.suffix in {".py", ".yml", ".yaml"}
    ]

    for path in files:
        text = path.read_text(errors="ignore")
        for name in forbidden:
            assert name not in text, path


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


def test_builtin_layout_key_up_emits_only_momentary_up() -> None:
    layout_data = yaml.safe_load((BUILD_IN_LAYOUT_PATH / "device-msd-two.yml").read_text())
    layout = Layout.model_validate(layout_data)

    events = list(layout.to_control_input(InteractionEvent(button_id=1, payload=0), None))

    assert len(events) == 1
    assert events[0].control_id == "0,0"
    assert events[0].capability_id == "button.momentary"
    assert events[0].event_type == "up"


def test_protocol_without_release_events_synthesizes_momentary_down_up() -> None:
    layout_data = yaml.safe_load((BUILD_IN_LAYOUT_PATH / "device-msd-two.yml").read_text())
    layout = Layout.model_validate(layout_data)

    events = list(
        layout.to_control_input(
            InteractionEvent(button_id=1, payload=1, supports_release=False),
            None,
        )
    )

    assert [(event.event_type, event.value) for event in events] == [
        ("down", {"eventType": "down"}),
        ("up", {"eventType": "up"}),
    ]
