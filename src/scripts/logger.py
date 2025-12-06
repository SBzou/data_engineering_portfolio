import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.config import runtime_config
from src.config.constants import PROJECT_ROOT


def setup_logging():
    """
    Initialize logging system with dynamic log file naming.
    Console output depends on the --verbose flag.
    """

    # Global log level for file logging
    file_log_level = logging.DEBUG

    # Console shows INFO/DEBUG only when verbose=True
    console_log_level = logging.INFO if runtime_config.VERBOSE else logging.WARNING

    # Build dynamic log file path
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / f"log_run_date={runtime_config.RUN_DATE}.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # ----------- Console handler -------------
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(console_log_level)

    # ----------- File handler -------------
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(file_log_level)

    # ----------- Root logger -------------
    root = logging.getLogger()
    root.setLevel(file_log_level)
    root.handlers.clear()
    root.addHandler(console)
    root.addHandler(file_handler)

    # ----------- Silence noisy libs -------------
    for lib in ["faker", "py4j", "pyspark", "google", "urllib3"]:
        logging.getLogger(lib).setLevel(logging.WARNING)

    return logging.getLogger(__name__)
