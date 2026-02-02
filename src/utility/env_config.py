import os
import yaml
from pathlib import Path

# Always resolve from project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

def load_env_config():
    env = os.getenv("ENV", "local")
    cfg_path = PROJECT_ROOT / "config" / f"{env}.yaml"

    with open(cfg_path, "r") as f:
        return yaml.safe_load(f)

_cfg = load_env_config()

from typing import Union, List

def input_path(file_name: Union[str, List[str]]):

    base = _cfg["input_base_path"]

    # ----------------------------
    # Case 1: multiple paths
    # ----------------------------
    if isinstance(file_name, list):

        resolved_paths = []

        for f in file_name:

            # already full abfss path
            if isinstance(f, str) and f.startswith("abfss://"):
                resolved_paths.append(f)
                continue

            # local filesystem
            if not base.startswith("abfss://"):
                resolved_paths.append(str(PROJECT_ROOT / base / f))
            else:
                resolved_paths.append(f"{base.rstrip('/')}/{f.lstrip('/')}")

        return resolved_paths

    # ----------------------------
    # Case 2: single path
    # ----------------------------
    if file_name.startswith("abfss://"):
        return file_name

    if not base.startswith("abfss://"):
        return str(PROJECT_ROOT / base / file_name)

    return f"{base.rstrip('/')}/{file_name.lstrip('/')}"


def delta_path(table_name: str) -> str:
    base = _cfg['delta_base_path']

    if not base.startswith("abfss://"):
        return str(PROJECT_ROOT/base/table_name)

    return f"{base}/{table_name}"

