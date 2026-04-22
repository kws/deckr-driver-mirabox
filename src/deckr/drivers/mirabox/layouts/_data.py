from collections import namedtuple
from pydantic import BaseModel, Field, model_validator
from typing import Any, Generator, Optional, Union, Literal, Annotated
from deckr.drivers.mirabox._protocol import InteractionEvent
import deckr.hardware.events as hw_events
import logging

logger = logging.getLogger(__name__)


class ImageFormat(BaseModel):
    width: int
    height: int
    format: str
    rotation: int = 0

    def to_hw_image_format(self) -> hw_events.HWSImageFormat:
        return hw_events.HWSImageFormat(
            width=self.width,
            height=self.height,
            format=self.format,
            rotation=self.rotation,
        )


class Display(BaseModel):
    id: int
    format: ImageFormat


class KeyEvents(BaseModel):
    key: Optional[int] = None
    press: Optional[int] = None

    @model_validator(mode="after")
    def exactly_one_field(self):
        if (self.key is None) == (self.press is None):
            raise ValueError("Exactly one of 'key' or 'press' must be specified")
        return self


class DialEvents(BaseModel):
    clockwise: int
    counterclockwise: int
    key: Optional[int] = None
    press: Optional[int] = None

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


Control = Union[
    KeyControl,
    ButtonControl,
    DialControl,
    TouchDialControl,
    TouchStripControl,
    ScreenControl,
]

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

    def _slot_type_and_gestures(self, control: Control) -> tuple[str, frozenset[str]]:
        if isinstance(control, KeyControl):
            return ("key", frozenset({"key_down", "key_up"}))
        if isinstance(control, ButtonControl):
            return ("button", frozenset({"key_down", "key_up"}))
        if isinstance(control, DialControl):
            return (
                "encoder",
                frozenset({"encoder_rotate", "encoder_down", "encoder_up"}),
            )
        if isinstance(control, TouchDialControl):
            return (
                "touch_dial",
                frozenset(
                    {"encoder_rotate", "encoder_down", "encoder_up", "touch_tap"}
                ),
            )
        if isinstance(control, TouchStripControl):
            return ("touch_strip", frozenset({"touch_swipe"}))
        if isinstance(control, ScreenControl):
            return ("screen", frozenset())
        return ("key", frozenset({"key_down", "key_up"}))

    def get_slots(self) -> list[hw_events.HWSlot]:
        result = []
        for control in self.controls:
            slot_type, gestures = self._slot_type_and_gestures(control)
            image_format = None
            if hasattr(control, "display"):
                image_format = control.display.format.to_hw_image_format()
            result.append(
                hw_events.HWSlot(
                    id=control.name,
                    coordinates=hw_events.Coordinates(
                        column=control.column, row=control.row
                    ),
                    image_format=image_format,
                    slot_type=slot_type,
                    gestures=gestures,
                )
            )
        return result

    def to_hardware_event(
        self, event: InteractionEvent, device
    ) -> Generator[hw_events.HardwareEvent, None, None]:
        control_descriptor = self.get_control_for_event(event.button_id)
        if control_descriptor is None:
            logger.warning(f"Control not found for event: {event}")
            return None

        device_id = device.id
        control_name = control_descriptor.control.name

        if control_descriptor.event_type == "key":
            if event.payload == 0:
                yield hw_events.KeyUpEvent(device_id=device_id, key_id=control_name)
            else:
                yield hw_events.KeyDownEvent(device_id=device_id, key_id=control_name)
        elif control_descriptor.event_type == "press":
            yield hw_events.KeyDownEvent(device_id=device_id, key_id=control_name)
            yield hw_events.KeyUpEvent(device_id=device_id, key_id=control_name)
        elif control_descriptor.event_type == "clockwise":
            yield hw_events.DialRotateEvent(
                device_id=device_id, dial_id=control_name, direction="clockwise"
            )
        elif control_descriptor.event_type == "counterclockwise":
            yield hw_events.DialRotateEvent(
                device_id=device_id, dial_id=control_name, direction="counterclockwise"
            )
        elif control_descriptor.event_type == "tap":
            yield hw_events.TouchTapEvent(device_id=device_id, touch_id=control_name)
        elif control_descriptor.event_type == "left_swipe":
            yield hw_events.TouchSwipeEvent(
                device_id=device_id, touch_id=control_name, direction="left"
            )
        elif control_descriptor.event_type == "right_swipe":
            yield hw_events.TouchSwipeEvent(
                device_id=device_id, touch_id=control_name, direction="right"
            )
        else:
            logger.warning(f"Unknown event type: {control_descriptor.event_type}")
