"""Render control number images using invariant-gfx for the mapping wizard."""

import io

import invariant_gfx
from invariant import Executor, Node, OpRegistry
from invariant.ops import stdlib
from invariant.params import ref
from invariant.store.memory import MemoryStore
from invariant_gfx.anchors import relative


def _build_executor() -> Executor:
    registry = OpRegistry()
    invariant_gfx.register_core_ops(registry)
    registry.register_package("stdlib", stdlib)
    return Executor(registry=registry, store=MemoryStore(cache="unbounded"))


_EXECUTOR = _build_executor()


def render_control_image(
    control_id: int,
    width: int,
    height: int,
    rotation: int = 0,
) -> bytes:
    """Render a control number on a dark background at the given dimensions.

    Uses invariant-gfx: create_solid + render_text + composite.
    rotation: degrees (0, 90, 180, 270) applied before encoding.
    Returns JPEG bytes suitable for set_key_image.
    """
    graph = {
        "bg": Node(
            op_name="gfx:create_solid",
            params={
                "size": ["${canvas.width-2}", "${canvas.height-2}"],
                "color": (100, 0, 0, 255),
            },
            deps=["canvas"],
        ),
        "frame": Node(
            op_name="gfx:create_solid",
            params={
                "size": ["${canvas.width}", "${canvas.height}"],
                "color": (255, 0, 0, 255),
            },
            deps=["canvas"],
        ),
        "text": Node(
            op_name="gfx:render_text",
            params={
                "text": str(control_id),
                "font": "Inter",
                "color": (255, 255, 255, 255),
                "size": "${decimal('24') * decimal(canvas.width / 72)}",
            },
            deps=["canvas"],
        ),
        "output": Node(
            op_name="gfx:composite",
            params={
                "layers": [
                    {"image": ref("frame"), "id": "frame"},
                    {
                        "image": ref("bg"),
                        "id": "bg",
                        "anchor": relative("frame", "c@c"),
                    },
                    {
                        "image": ref("text"),
                        "anchor": relative("bg", "c@c"),
                        "id": "text",
                    },
                ],
            },
            deps=["frame", "bg", "text"],
        ),
    }
    context = {"canvas": {"width": width, "height": height}}
    results = _EXECUTOR.execute(graph, context=context)
    image = results["output"].image
    if rotation != 0:
        image = image.rotate(rotation)
    if image.mode != "RGB":
        image = image.convert("RGB")
    buf = io.BytesIO()
    image.save(buf, format="JPEG", quality=100)
    return buf.getvalue()
