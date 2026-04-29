import logging
from collections import namedtuple
from collections.abc import Generator
from dataclasses import dataclass
from typing import Annotated, Any, Literal

from deckr.hardware.descriptors import (
    DECKR_INPUT_BUTTON,
    DECKR_INPUT_ENCODER,
    DECKR_INPUT_TOUCH,
    DECKR_OUTPUT_RASTER,
    CapabilityDescriptor,
    CapabilitySchema,
    ControlGeometry,
)
from deckr.hardware.descriptors import (
    ControlDescriptor as DeckrControlDescriptor,
)
from pydantic import BaseModel, Field, model_validator

from deckr.drivers.mirabox._protocol import InteractionEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ControlInputEvent:
    control_id: str
    capability_id: str
    event_type: str
    value: dict[str, Any]


class ImageFormat(BaseModel):
    width: int
    height: int
    format: str
    rotation: int = 0


class Display(BaseModel):
    id: int
    format: ImageFormat


class KeyEvents(BaseModel):
    key: int | None = None
    press: int | None = None

    @model_validator(mode="after")
    def exactly_one_field(self):
        if (self.key is None) == (self.press is None):
            raise ValueError("Exactly one of 'key' or 'press' must be specified")
        return self


class DialEvents(BaseModel):
    clockwise: int
    counterclockwise: int
    key: int | None = None
    press: int | None = None

    @model_validator(mode="after")
    def exactly_one_field(self):
        if (self.key is None) == (self.press is None):
            raise ValueError("Exactly one of 'key' or 'press' must be specified")
        return self


class TouchDialEvents(DialEvents):
    tap: int


class TouchStripEvents(BaseModel):
    left_swipe: int
    right_swipe: int


class BaseControl(BaseModel):
    type: str
    row: int = 0
    column: int = 0


class KeyControl(BaseControl):
    name: str
    type: Literal["key"]
    events: KeyEvents
    display: Display


class ButtonControl(BaseControl):
    name: str
    type: Literal["button"]
    events: KeyEvents


class TouchDialControl(BaseControl):
    name: str
    type: Literal["touch_dial"]
    events: TouchDialEvents
    display: Display


class DialControl(BaseControl):
    name: str
    type: Literal["dial"]
    events: DialEvents


class TouchStripControl(BaseControl):
    name: str
    type: Literal["touch_strip"]
    events: TouchStripEvents
    display: Display


class ScreenControl(BaseControl):
    name: str
    type: Literal["screen"]
    display: Display


Control = (
    KeyControl
    | ButtonControl
    | DialControl
    | TouchDialControl
    | TouchStripControl
    | ScreenControl
)

DiscriminatedControl = Annotated[
    Control,
    Field(discriminator="type"),
]


class InitCommand(BaseModel):
    cmd: str
    args: dict[str, Any] = Field(default_factory=dict)


class Heartbeat(BaseModel):
    period: int
    commands: list[InitCommand]


ControlDescriptor = namedtuple("ControlDescriptor", ["event_type", "control"])


def _button_value_schema(events: tuple[str, ...], schema_id: str) -> CapabilitySchema:
    return CapabilitySchema.model_validate(
        {
            "schemaId": schema_id,
            "schema": {
                "type": "object",
                "required": ["eventType"],
                "properties": {"eventType": {"enum": list(events)}},
                "additionalProperties": False,
            },
        }
    )


def _encoder_value_schema() -> CapabilitySchema:
    return CapabilitySchema.model_validate(
        {
            "schemaId": "deckr.value.input.encoder.relative.v1",
            "schema": {
                "type": "object",
                "required": ["delta"],
                "properties": {
                    "delta": {"type": "integer"},
                    "direction": {"enum": ["clockwise", "counterclockwise"]},
                },
                "additionalProperties": False,
            },
        }
    )


def _touch_value_schema() -> CapabilitySchema:
    return CapabilitySchema.model_validate(
        {
            "schemaId": "deckr.value.input.touch.gesture.v1",
            "schema": {
                "type": "object",
                "required": ["eventType"],
                "properties": {
                    "eventType": {"enum": ["tap", "swipe"]},
                    "direction": {"enum": ["left", "right"]},
                },
                "additionalProperties": False,
            },
        }
    )


def _raster_command_schema(width: int, height: int) -> CapabilitySchema:
    return CapabilitySchema.model_validate(
        {
            "schemaId": "deckr.command.output.raster.bitmap.v1",
            "schema": {
                "type": "object",
                "required": ["commandType"],
                "properties": {
                    "commandType": {"enum": ["set_frame", "clear"]},
                    "image": {"type": "string", "contentEncoding": "base64"},
                    "encoding": {"enum": ["jpeg", "png"]},
                    "width": {"const": width},
                    "height": {"const": height},
                },
                "additionalProperties": False,
            },
        }
    )


def _momentary_button_capability() -> CapabilityDescriptor:
    return CapabilityDescriptor(
        capabilityId="button.momentary",
        family=DECKR_INPUT_BUTTON,
        type="momentary",
        direction="input",
        access=("emits",),
        valueSchema=_button_value_schema(
            ("down", "up"),
            "deckr.value.input.button.momentary.v1",
        ),
        eventTypes=("down", "up"),
    )


def _activation_button_capability(
    control_id: str,
    *,
    projected: bool,
) -> CapabilityDescriptor:
    payload: dict[str, Any] = {
        "capabilityId": "button.press",
        "family": DECKR_INPUT_BUTTON,
        "type": "activation",
        "direction": "input",
        "access": ["emits"],
        "valueSchema": _button_value_schema(
            ("press",),
            "deckr.value.input.button.activation.v1",
        ).model_dump(by_alias=True, exclude_none=True, mode="json"),
        "eventTypes": ["press"],
    }
    if projected:
        payload["projection"] = {
            "owner": "hardware_manager",
            "source": {
                "controlId": control_id,
                "capabilityId": "button.momentary",
            },
        }
    return CapabilityDescriptor.model_validate(payload)


def _encoder_capability() -> CapabilityDescriptor:
    return CapabilityDescriptor.model_validate(
        {
            "capabilityId": "encoder.relative",
            "family": DECKR_INPUT_ENCODER,
            "type": "relative",
            "direction": "input",
            "access": ["emits"],
            "valueSchema": _encoder_value_schema().model_dump(
                by_alias=True,
                exclude_none=True,
                mode="json",
            ),
            "eventTypes": ["rotate"],
            "constraints": [
                {
                    "type": "range",
                    "subject": "delta",
                    "minimum": -24,
                    "maximum": 24,
                    "step": 1,
                    "unit": "detent",
                }
            ],
            "units": [{"subject": "delta", "unit": "detent"}],
        }
    )


def _touch_capability() -> CapabilityDescriptor:
    return CapabilityDescriptor(
        capabilityId="touch.gesture",
        family=DECKR_INPUT_TOUCH,
        type="gesture",
        direction="input",
        access=("emits",),
        valueSchema=_touch_value_schema(),
        eventTypes=("tap", "swipe"),
    )


def _raster_capability(format: ImageFormat) -> CapabilityDescriptor:
    return CapabilityDescriptor.model_validate(
        {
            "capabilityId": "raster.bitmap",
            "family": DECKR_OUTPUT_RASTER,
            "type": "bitmap",
            "direction": "output",
            "access": ["settable"],
            "commandSchema": _raster_command_schema(
                format.width,
                format.height,
            ).model_dump(by_alias=True, exclude_none=True, mode="json"),
            "commandTypes": ["set_frame", "clear"],
            "constraints": [
                {
                    "type": "fixed",
                    "subject": "width",
                    "value": format.width,
                    "unit": "pixel",
                },
                {
                    "type": "fixed",
                    "subject": "height",
                    "value": format.height,
                    "unit": "pixel",
                },
                {
                    "type": "fixed",
                    "subject": "rotation",
                    "value": format.rotation,
                    "unit": "degree",
                },
                {"type": "enum", "subject": "encoding", "values": ["jpeg", "png"]},
            ],
            "units": [
                {"subject": "width", "unit": "pixel"},
                {"subject": "height", "unit": "pixel"},
                {"subject": "rotation", "unit": "degree"},
            ],
        }
    )


class Layout(BaseModel):
    name: str
    candidate: str
    match: str

    init_sequence: list[InitCommand] = Field(default_factory=list)
    heartbeats: list[Heartbeat] = Field(default_factory=list)

    controls: list[DiscriminatedControl] = Field(default_factory=list)
    image_config: dict[str, ImageFormat]

    _event_lookup: dict[int, ControlDescriptor]
    _name_lookup: dict[str, Control]

    def model_post_init(self, __context) -> None:
        event_map = {}
        for control in self.controls:
            for event_type, event_id in control.events:
                event_map[event_id] = ControlDescriptor(event_type, control)
        self._event_lookup = event_map

        name_map = {}
        for control in self.controls:
            name_map[control.name] = control
        self._name_lookup = name_map

    def get_control_for_event(self, event_id: int) -> ControlDescriptor | None:
        return self._event_lookup.get(event_id, None)

    def get_control_for_name(self, name: str) -> Control | None:
        return self._name_lookup.get(name, None)

    def _slot_type_and_gestures(self, control: Control) -> tuple[str, list[str]]:
        if isinstance(control, KeyControl):
            return ("key", ["key_down", "key_up"])
        if isinstance(control, ButtonControl):
            return ("button", ["key_down", "key_up"])
        if isinstance(control, DialControl):
            return (
                "encoder",
                ["encoder_down", "encoder_rotate", "encoder_up"],
            )
        if isinstance(control, TouchDialControl):
            return (
                "touch_dial",
                ["encoder_down", "encoder_rotate", "encoder_up", "touch_tap"],
            )
        if isinstance(control, TouchStripControl):
            return ("touch_strip", ["touch_swipe"])
        if isinstance(control, ScreenControl):
            return ("screen", [])
        return ("key", ["key_down", "key_up"])

    def get_controls(self) -> list[DeckrControlDescriptor]:
        result = []
        for control in self.controls:
            input_capabilities: list[CapabilityDescriptor] = []
            if isinstance(control, KeyControl | ButtonControl):
                if control.events.key is not None:
                    input_capabilities.append(_momentary_button_capability())
                    input_capabilities.append(
                        _activation_button_capability(control.name, projected=True)
                    )
                else:
                    input_capabilities.append(
                        _activation_button_capability(control.name, projected=False)
                    )
            if isinstance(control, DialControl | TouchDialControl):
                input_capabilities.append(_encoder_capability())
                if control.events.key is not None:
                    input_capabilities.append(_momentary_button_capability())
                    input_capabilities.append(
                        _activation_button_capability(control.name, projected=True)
                    )
                else:
                    input_capabilities.append(
                        _activation_button_capability(control.name, projected=False)
                    )
            if isinstance(control, TouchDialControl | TouchStripControl):
                input_capabilities.append(_touch_capability())
            output_capabilities: list[CapabilityDescriptor] = []
            if hasattr(control, "display"):
                output_capabilities.append(_raster_capability(control.display.format))
            result.append(
                DeckrControlDescriptor(
                    controlId=control.name,
                    kind=control.type,
                    label=control.name,
                    geometry=ControlGeometry(
                        x=control.column,
                        y=control.row,
                        width=1,
                        height=1,
                        unit="grid",
                    ),
                    inputCapabilities=tuple(input_capabilities),
                    outputCapabilities=tuple(output_capabilities),
                )
            )
        return result

    def to_control_input(
        self, event: InteractionEvent, device
    ) -> Generator[ControlInputEvent, None, None]:
        del device
        control_descriptor = self.get_control_for_event(event.button_id)
        if control_descriptor is None:
            logger.warning(f"Control not found for event: {event}")
            return None

        control_name = control_descriptor.control.name

        if control_descriptor.event_type == "key":
            if event.payload == 0:
                yield ControlInputEvent(
                    control_id=control_name,
                    capability_id="button.momentary",
                    event_type="up",
                    value={"eventType": "up"},
                )
                yield ControlInputEvent(
                    control_id=control_name,
                    capability_id="button.press",
                    event_type="press",
                    value={"eventType": "press"},
                )
            else:
                yield ControlInputEvent(
                    control_id=control_name,
                    capability_id="button.momentary",
                    event_type="down",
                    value={"eventType": "down"},
                )
        elif control_descriptor.event_type == "press":
            yield ControlInputEvent(
                control_id=control_name,
                capability_id="button.press",
                event_type="press",
                value={"eventType": "press"},
            )
        elif control_descriptor.event_type == "clockwise":
            yield ControlInputEvent(
                control_id=control_name,
                capability_id="encoder.relative",
                event_type="rotate",
                value={"delta": 1, "direction": "clockwise"},
            )
        elif control_descriptor.event_type == "counterclockwise":
            yield ControlInputEvent(
                control_id=control_name,
                capability_id="encoder.relative",
                event_type="rotate",
                value={"delta": -1, "direction": "counterclockwise"},
            )
        elif control_descriptor.event_type == "tap":
            yield ControlInputEvent(
                control_id=control_name,
                capability_id="touch.gesture",
                event_type="tap",
                value={"eventType": "tap"},
            )
        elif control_descriptor.event_type == "left_swipe":
            yield ControlInputEvent(
                control_id=control_name,
                capability_id="touch.gesture",
                event_type="swipe",
                value={"eventType": "swipe", "direction": "left"},
            )
        elif control_descriptor.event_type == "right_swipe":
            yield ControlInputEvent(
                control_id=control_name,
                capability_id="touch.gesture",
                event_type="swipe",
                value={"eventType": "swipe", "direction": "right"},
            )
        else:
            logger.warning(f"Unknown event type: {control_descriptor.event_type}")
