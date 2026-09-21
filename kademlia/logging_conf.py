"""Per-node logging setup, useful when debugging multiple node processes at once."""

import logging
import sys


def configure_logging(node_label: str, logfile: str = None, level: int = logging.INFO) -> None:
    fmt = f"%(asctime)s [{node_label}] %(levelname)s %(name)s: %(message)s"
    handlers = [logging.StreamHandler(sys.stderr)]
    if logfile:
        handlers.append(logging.FileHandler(logfile))
    logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)
