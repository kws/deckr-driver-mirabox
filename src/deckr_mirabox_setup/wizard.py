"""Textual wizard for mapping protocol key IDs to physical slot positions."""

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Vertical
from textual.widgets import Footer, Input, RichLog, Static
from textual.worker import get_current_worker

from deckr_mirabox_setup.device_session import DeviceSession, DeviceSessionError
from deckr_mirabox_setup.slot_render import render_slot_image

NUM_KEYS_DEFAULT = 25
SIZE_DEFAULT = 72
SIZE_STEP = 8
SIZE_MIN = 48
ROTATIONS = (0, 90, 180, 270)


class SlotMappingApp(App[None]):
    """Interactive wizard to map protocol key IDs to physical (row, col) positions."""

    ENABLE_COMMAND_PALETTE = False

    BINDINGS = [
        Binding("n,right", "next_key", "Next", show=True),
        Binding("p,left", "prev_key", "Prev", show=True),
        Binding("plus,shift+equal", "bigger", "Bigger", show=True, key_display="+"),
        Binding("minus,-", "smaller", "Smaller", show=True, key_display="-"),
        Binding("w", "wider", "Wider", show=True),
        Binding("W", "narrower", "Narrower", show=True),
        Binding("h", "taller", "Taller", show=True),
        Binding("H", "shorter", "Shorter", show=True),
        Binding("y", "visible", "Visible", show=True),
        Binding("N", "not_visible", "Not visible", show=True),
        Binding("s", "skip", "Skip", show=True),
        Binding("r", "rotate", "Rotate", show=True),
        Binding("c", "clear_key", "Clear", show=True),
        Binding("escape", "cancel_position", "Cancel", show=True),
        Binding("q", "quit", "Quit", show=True),
    ]

    def __init__(
        self, session: DeviceSession, num_keys: int = NUM_KEYS_DEFAULT
    ) -> None:
        super().__init__()
        self._session = session
        self._num_keys = num_keys
        self._key_index = 0
        self._image_width = SIZE_DEFAULT
        self._image_height = SIZE_DEFAULT
        self._rotation_index = 0
        self._mapping: dict[int, tuple[int, int]] = {}
        self._awaiting_position = False

    def _protocol_key_id(self) -> int:
        return self._key_index + 1 + self._session.start_offset

    def _update_display(self) -> None:
        status = self.query_one("#status", Static)
        rotation = ROTATIONS[self._rotation_index]
        status.update(
            f"Key {self._protocol_key_id()} ({self._key_index + 1}/{self._num_keys})  |  "
            f"Size {self._image_width}x{self._image_height}  |  "
            f"Rotation {rotation}°  |  "
            f"Mapped: {len(self._mapping)}/{self._num_keys}"
        )
        if self._awaiting_position:
            self.query_one("#position-input", Input).focus()

    def _send_current_image(self) -> None:
        """Render and send current slot image to device (runs in worker)."""
        key_id = self._protocol_key_id()
        rotation = ROTATIONS[self._rotation_index]
        jpeg = render_slot_image(
            key_id, self._image_width, self._image_height, rotation
        )
        self._session.send_slot_image(key_id, jpeg)
        self._session.refresh()

    def _on_send_done(self) -> None:
        self._update_display()

    def _append_event(self, text: str) -> None:
        """Append decoded event to log (called from main thread via call_from_thread)."""
        try:
            log = self.query_one("#event-log", RichLog)
            log.write(text)
        except Exception:
            pass

    def _event_log_worker(self) -> None:
        """Background worker: read HID reports, decode, append to log."""
        worker = get_current_worker()
        while True:
            if worker.cancelled_event.is_set():
                break
            try:
                data = self._session.read_report(timeout_ms=100)
                if data:
                    decoded = self._session.decode_event(data)
                    self.call_from_thread(self._append_event, decoded)
            except Exception:
                break

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(
                "MiraBox Slot Mapping — Map protocol key IDs to physical (row,col) positions",
                id="title",
            )
            yield Static(id="firmware")
            yield Static(
                "n/p next/prev  +/- size  w/W h/H width/height  r rotate  c clear  y visible  N not visible  s skip  esc cancel  q quit",
                id="help",
            )
            yield Static(id="status")
            yield RichLog(id="event-log", max_lines=20, highlight=True)
            with Container(id="position-container"):
                inp = Input(
                    placeholder="row,col (e.g. 0,1) — press Enter to confirm",
                    id="position-input",
                )
                inp.display = False
                inp.can_focus = False
                yield inp
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#firmware", Static).update(self._session.device_info)
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)
        self.run_worker(self._event_log_worker, thread=True)

    def action_next_key(self) -> None:
        if self._awaiting_position:
            return
        self._key_index = min(self._key_index + 1, self._num_keys - 1)
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)

    def action_prev_key(self) -> None:
        if self._awaiting_position:
            return
        self._key_index = max(self._key_index - 1, 0)
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)

    def action_bigger(self) -> None:
        if self._awaiting_position:
            return
        self._image_width += SIZE_STEP
        self._image_height += SIZE_STEP
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)

    def action_smaller(self) -> None:
        if self._awaiting_position:
            return
        self._image_width = max(self._image_width - SIZE_STEP, SIZE_MIN)
        self._image_height = max(self._image_height - SIZE_STEP, SIZE_MIN)
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)

    def action_wider(self) -> None:
        if self._awaiting_position:
            return
        self._image_width += SIZE_STEP
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)

    def action_narrower(self) -> None:
        if self._awaiting_position:
            return
        self._image_width = max(self._image_width - SIZE_STEP, SIZE_MIN)
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)

    def action_taller(self) -> None:
        if self._awaiting_position:
            return
        self._image_height += SIZE_STEP
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)

    def action_shorter(self) -> None:
        if self._awaiting_position:
            return
        self._image_height = max(self._image_height - SIZE_STEP, SIZE_MIN)
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)

    def action_rotate(self) -> None:
        if self._awaiting_position:
            return
        self._rotation_index = (self._rotation_index + 1) % len(ROTATIONS)
        self._update_display()
        self.run_worker(self._send_current_image, thread=True)

    def action_clear_key(self) -> None:
        """Clear current key display (helps identify size)."""
        if self._awaiting_position:
            return
        key_id = self._protocol_key_id()
        self.run_worker(lambda: self._session.clear_key(key_id), thread=True)

    def action_visible(self) -> None:
        if self._awaiting_position:
            return
        self._awaiting_position = True
        inp = self.query_one("#position-input", Input)
        inp.value = ""
        inp.display = True
        inp.can_focus = True
        inp.focus()

    def action_not_visible(self) -> None:
        if self._awaiting_position:
            self._cancel_position_input()
        self.action_next_key()

    def action_skip(self) -> None:
        if self._awaiting_position:
            self._cancel_position_input()
        self.action_next_key()

    def action_cancel_position(self) -> None:
        """Cancel position input (Escape)."""
        if self._awaiting_position:
            self._cancel_position_input()

    def _cancel_position_input(self) -> None:
        self._awaiting_position = False
        inp = self.query_one("#position-input", Input)
        inp.display = False
        inp.can_focus = False
        inp.blur()
        self._update_display()

    def _parse_position(self, text: str) -> tuple[int, int] | None:
        text = text.strip().replace(" ", ",")
        parts = text.split(",")
        if len(parts) != 2:
            return None
        try:
            return (int(parts[0].strip()), int(parts[1].strip()))
        except ValueError:
            return None

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "position-input" or not self._awaiting_position:
            return
        if not event.value.strip():
            self._cancel_position_input()
            return
        pos = self._parse_position(event.value)
        if pos is not None:
            self._mapping[self._protocol_key_id()] = pos
            self._cancel_position_input()
            if self._key_index < self._num_keys - 1:
                self.action_next_key()
            else:
                self._update_display()
        else:
            event.input.value = ""
            event.input.placeholder = "Invalid. Use row,col (e.g. 0,1)"

    def on_exit(self) -> None:
        if self._mapping:
            mapping_str = str(dict(sorted(self._mapping.items())))
            self.notify(mapping_str, title="Slot mapping complete")


def run_wizard(num_keys: int = NUM_KEYS_DEFAULT) -> None:
    """Run the slot mapping wizard. Exits with error if no device found."""
    try:
        with DeviceSession() as session:
            app = SlotMappingApp(session, num_keys=num_keys)
            app.run()
            # Print after app.run() returns; on_exit output is lost when TUI tears down
            print(f"\n{session.device_info}")
            if app._mapping:
                print(f"Slot mapping: {dict(sorted(app._mapping.items()))}")
                print(f"Size: {app._image_width}x{app._image_height}")
                print(f"Rotation: {ROTATIONS[app._rotation_index]}")
    except DeviceSessionError as e:
        raise SystemExit(str(e)) from e
