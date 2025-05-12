"""Logging configuration."""

from __future__ import annotations

import logging

# Name the logger after the package
logger = logging.getLogger(__package__)


def set_level(level: int) -> None:
    logger.setLevel(level)

    # Only configure handlers if none exist yet
    if not logger.handlers:
        # Create stream handler with stderr
        handler = logging.StreamHandler()

        # Configure formatter with timestamp, name, and log level
        handler.setFormatter(logging.Formatter(fmt="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

        # Add handler to the logger
        logger.addHandler(handler)
