# deckr-driver-mirabox

Python MiraBox hardware manager package for Deckr, including the `mirabox-setup`
utility and the built-in layout definitions used by both the Python and Rust
MiraBox managers.

## Included tooling

- `dev.deckr.hardware.mirabox` component entry point
- `mirabox-setup` console script for setup and control mapping

## Runtime

The Python manager participates as `hardware_manager:<manager-id>` on the
`hardware_messages` lane. The manager id comes from the generic component
instance's `endpoints.hardware_manager` value.

Set the hardware manager endpoint id explicitly when the manager identity is
part of deployment policy, such as room-pinned controller device config:

```toml
[deckr.components.instances.mirabox_kitchen]
component = "dev.deckr.hardware.mirabox"
instance_id = "kitchen"

[deckr.components.instances.mirabox_kitchen.endpoints]
hardware_manager = "kitchen"
```

## Known limitation

The HID path still assumes report ID `0`. That behavior is preserved during the repo
split and should only be changed with hardware-backed validation.

## Development

Build a local `deckr` wheel first:

```bash
cd ../deckr && uv build --wheel
cd ../deckr-driver-mirabox
uv sync --dev --find-links ../deckr/dist
uv run --find-links ../deckr/dist pytest
```
