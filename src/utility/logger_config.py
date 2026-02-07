import logging
import os
from pathlib import Path

from src.utility.env_config import PROJECT_ROOT, load_env_config


def _get_env_name():
    return os.getenv("ENV", "local")


# ---- build reports/<env> folder ----
ENV_NAME = _get_env_name()

REPORTS_DIR = PROJECT_ROOT / "reports" / ENV_NAME
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = REPORTS_DIR / "etl_automation.log"


def setup_logger():

    root_logger = logging.getLogger()

    if root_logger.handlers:
        return

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d | "
        "%(funcName)s | %(message)s"
    )

    file_handler = logging.FileHandler(LOG_FILE, mode="w")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
