import logging
from collections import namedtuple
from collections.abc import Generator
from dataclasses import dataclass
from typing import Annotated, Any, Literal

from deckr.hardware.capabilities import (
    button_activation_value_schema,
    button_momentary_value_schema,
    encoder_relative_value_schema,
    raster_bitmap_command_schema,
    touch_gesture_value_schema,
)
from deckr.hardware.descriptors import (
    DECKR_INPUT_BUTTON,
    DECKR_INPUT_ENCODER,
    DECKR_INPUT_TOUCH,
    DECKR_OUTPUT_RASTER,
    CapabilityDescriptor,
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


def _momentary_button_capability() -> CapabilityDescriptor:
    return CapabilityDescriptor(
        capabilityId="button.momentary",
        family=DECKR_INPUT_BUTTON,
        type="momentary",
        direction="input",
        access=("emits",),
        valueSchema=button_momentary_value_schema(),
        eventTypes=("down", "up"),
    )


def _activation_button_capability() -> CapabilityDescriptor:
    payload: dict[str, Any] = {
        "capabilityId": "button.press",
        "family": DECKR_INPUT_BUTTON,
        "type": "activation",
        "direction": "input",
        "access": ["emits"],
        "valueSchema": button_activation_value_schema().model_dump(
            by_alias=True,
            exclude_none=True,
            mode="json",
        ),
        "eventTypes": ["press"],
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
            "valueSchema": encoder_relative_value_schema().model_dump(
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
        valueSchema=touch_gesture_value_schema(),
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
            "commandSchema": raster_bitmap_command_schema(
                width=format.width,
                height=format.height,
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

    def get_controls(self) -> list[DeckrControlDescriptor]:
        result = []
        for control in self.controls:
            input_capabilities: list[CapabilityDescriptor] = []
            if isinstance(control, KeyControl | ButtonControl):
                if control.events.key is not None:
                    input_capabilities.append(_momentary_button_capability())
                else:
                    input_capabilities.append(_activation_button_capability())
            if isinstance(control, DialControl | TouchDialControl):
                input_capabilities.append(_encoder_capability())
                if control.events.key is not None:
                    input_capabilities.append(_momentary_button_capability())
                else:
                    input_capabilities.append(_activation_button_capability())
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
