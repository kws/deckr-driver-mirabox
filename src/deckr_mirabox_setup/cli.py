import click

from deckr_mirabox_setup.scan import main as scan_main
from deckr_mirabox_setup.connect_device import main as configure_main
from deckr_mirabox_setup.wizard import run_wizard


@click.group()
def cli():
    """MiraBox setup utilities for scanning and configuring devices."""


@cli.command()
def scan():
    """Scan for HID devices and report MiraBox/HANVON firmware versions."""
    scan_main()


@cli.command()
def configure():
    """Connect to first compatible device and run test configuration (wake, images, key wait)."""
    configure_main()


@cli.command("map-slots")
@click.option(
    "--keys",
    "num_keys",
    type=int,
    default=25,
    help="Number of display slots to map",
)
def map_slots(num_keys: int):
    """Interactive wizard to map protocol key IDs to physical slot positions."""
    run_wizard(num_keys=num_keys)
