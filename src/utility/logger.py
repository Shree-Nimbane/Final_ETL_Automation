import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler


def get_logger(job_name: str, env: str = "local"):
    """
    Central logging framework for ETL automation.
    Creates file + console loggers with rotation.
    """

    # project root (ETL_Automation)
    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )

    # logs/<env> folder
    log_dir = os.path.join(project_root, "logs", env)
    os.makedirs(log_dir, exist_ok=True)

    # timestamped file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{job_name}_{timestamp}.log")

    logger = logging.getLogger(job_name)
    logger.setLevel(logging.INFO)

    # avoid duplicate handlers when imported multiple times
    if not logger.handlers:

        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,   # 10 MB
            backupCount=5
        )

        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(module)s | %(message)s"
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
