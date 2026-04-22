from pathlib import Path
from typing import Any, Dict, Generator
from deckr.drivers.mirabox.layouts._evaluator import eval_policy
from deckr.drivers.mirabox.layouts._data import Layout
import yaml
import json
import logging

logger = logging.getLogger(__name__)

BUILD_IN_LAYOUT_PATH = Path(__file__).parent / "built-in"
SEARCH_PATHS = [
    BUILD_IN_LAYOUT_PATH,
    # Add custom and user-defined search paths here
]

__all__ = ["Layout", "search_candidates"]


def resolve_config_files():
    for search_path in SEARCH_PATHS:
        for layout_file in search_path.rglob("*"):
            if layout_file.is_file() and layout_file.suffix in [
                ".yml",
                ".yaml",
                ".json",
            ]:
                yield layout_file


def parse_layout_file(file: Path) -> Dict[str, Any]:
    if file.suffix in [".yml", ".yaml"]:
        with open(file, "r") as f:
            return yaml.safe_load(f)
    elif file.suffix in [".json"]:
        with open(file, "r") as f:
            return json.load(f)
    else:
        raise ValueError(f"Unsupported file extension: {file.suffix}")


def search_candidates(
    descriptor: dict[str, Any],
) -> Generator[dict[str, Any], None, None]:
    for config_file in resolve_config_files():
        try:
            config = parse_layout_file(config_file)
        except Exception as e:
            logger.warning(f"Error parsing layout file {config_file}: {e}")
            continue

        if "candidate" not in config:
            logger.warning(f"Layout file {config_file} does not have a candidate")
            continue

        candidate = eval_policy(config["candidate"], descriptor)
        if candidate:
            yield config
