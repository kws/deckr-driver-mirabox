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

Beacon advertises current devices as candidates for controller claims. The
manager accepts controller ownership by attaching its participant token to
matching `dev.deckr.profile.hardware_claim.v1` Concord contracts, and live claim
authority is Concord-only after negotiation. The shared
`deckr.hardware.runtime.HardwareManagerRuntime` implementation owns that
protocol flow; this package owns HID discovery, layout evaluation, command
execution, and device reset.

The old discovery inventory, endpoint-presence, and unilateral device-claim
current-state records are not emitted or consumed.

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

The `deckr` Python distribution now comes from the sibling
[`deckr-core-python`](https://github.com/kws/deckr-core-python) repository.
Check it out next to this repository at the revision in `.deckr-core-revision`. In the parent workspace, run
`uv sync --all-packages --all-groups`, then the repository-local checks:

```bash
uv run --no-sync ruff check .
uv run --no-sync pytest
```

For standalone development, build a wheel from the separated core and install
it without the parent workspace source override:

```bash
(cd ../deckr-core-python && uv build --wheel)
uv sync --no-sources --dev --find-links ../deckr-core-python/dist
uv run --no-sync pytest
```

Runtime imports remain under `deckr`; the reference-only `deckr` repository is
not a Python package dependency.
