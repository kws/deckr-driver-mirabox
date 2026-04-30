import colorsys
import random
import time
from io import BytesIO

import cairosvg
import hid
from PIL import Image, ImageDraw

from deckr.drivers.mirabox._protocol import MiraBoxProtocol
from deckr.drivers.mirabox._transport import BlockingHidTransport

VENDOR_ID = 0x0B00
PRODUCT_ID = 0x1001
IMAGE_SIZE = 40

ARROW_SVG = """
<svg xmlns="http://www.w3.org/2000/svg" width="100%" height="100%" viewBox="0 0 100 100">

  <!-- The shaft -->
  <line x1="10" y1="50" x2="90" y2="50"
        stroke="black" stroke-width="4"
  />
  <polygon points="2,50 10,45 10,55" stroke="none" fill="black"/>
  <polygon points="98,50 90,45 90,55" stroke="none" fill="black"/>
  <line x1="50" y1="10" x2="50" y2="90"
        stroke="black" stroke-width="4"
  />
  <polygon points="50,2 45,10 55,10" stroke="none" fill="black"/>
  <polygon points="50,98 45,90 55,90" stroke="none" fill="black"/>


  <rect
  x="1" y="1"
  width="98" height="98"
  fill="none"
  stroke="black"
  stroke-width="2"
  stroke-dasharray="6 6"
/>

<rect
  x="1" y="1"
  width="98" height="98"
  fill="none"
  stroke="white"
  stroke-width="2"
  stroke-dasharray="6 6"
  stroke-dashoffset="6"
/>

</svg>
"""


def svg_to_pil(svg_bytes: bytes, *, width: int, height: int) -> Image.Image:
    # Render to PNG bytes at the target size
    png_bytes = cairosvg.svg2png(
        bytestring=svg_bytes, output_width=width, output_height=height
    )

    # Load as a PIL image
    im = Image.open(BytesIO(png_bytes))
    return im.convert("RGBA")


def hue_to_rgb(hue):
    rgb = colorsys.hsv_to_rgb(hue, 1, 1)
    return tuple(int(c * 255) for c in rgb)


def generate_random_image(color=None, size=IMAGE_SIZE, text=None, svg=None):
    if color is None:
        hue = random.random()
        color = hue_to_rgb(hue)

    img = Image.new("RGB", (size, size), color=color)

    if svg is not None:
        svg_img = svg_to_pil(svg, width=size, height=size)
        img.paste(svg_img, (0, 0), svg_img)

    draw = ImageDraw.Draw(img)
    text_color = tuple([255 - c for c in color])
    text = f"{hue:.2f}" if text is None else str(text)
    draw.text((0, 0), text, fill=text_color)

    buffer = BytesIO()
    img.save(buffer, format="JPEG", quality=10)

    return buffer.getvalue()


def main():
    devices = list(hid.enumerate())
    compatible_devices = [d for d in devices if d["usage_page"] == 65440]

    device = compatible_devices[0]
    print("Opening device:", device)

    transport = BlockingHidTransport(device["path"])
    transport.open()

    report = transport.get_input_report(0)
    print("Report:", report)
    firmware_version = report[1:-1].decode("ascii")
    print(f"Firmware version: '{firmware_version}'")
    print(firmware_version.encode("ascii"))

    def write(chunks: list[bytes]):
        for chunk in chunks:
            transport.write(chunk)

    protocol = MiraBoxProtocol()

    write(protocol.encode_command("wake_display"))
    write(protocol.encode_command("clear_key", target=0xFF))
    time.sleep(1)

    start_offset = 5 if "Mbox_N4E" in firmware_version else 0

    size = 21
    for key in range(1, 7):
        my_size = size + (10 * key)
        jpeg_bytes = generate_random_image(
            size=my_size, text=my_size, svg=ARROW_SVG.encode("ascii")
        )
        cmds = protocol.encode_command(
            "set_key_image", key=key + start_offset, image=jpeg_bytes, x=0, y=0
        )
        write(cmds)

    write(protocol.encode_command("refresh"))

    print("Waiting for key press...")
    data = transport.read(1_000)
    print("Data:", data)

    transport.close()
