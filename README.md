# deckr-driver-mirabox

Python MiraBox driver package for Deckr, including the `mirabox-setup` utility and the
built-in layout definitions used by both the Python and Rust MiraBox lanes.

## Included tooling

- `deckr.drivers.mirabox` driver entry point
- `mirabox-setup` console script for setup and slot mapping

## Runtime

The Python manager participates as `hardware_manager:<manager-id>` on the
`hardware_messages` lane. If `manager_id` is omitted from
`[deckr.drivers.mirabox]`, it defaults to `mirabox-python-<hostname>`.

Set `manager_id` explicitly only when the manager identity is part of deployment
policy, such as room-pinned controller device config:

```toml
[deckr.drivers.mirabox]
manager_id = "kitchen"
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
