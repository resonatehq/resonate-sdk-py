"""Logging configuration."""

from __future__ import annotations

import logging

_configured: bool = False
# Name the logger after the package
logger = logging.getLogger(__package__)


def set_level(level: int) -> None:
    global _configured  # noqa: PLW0603

    # ensures one-time initialization of configuration settings, even if multiple instances of the Resonate class are created.
    if not _configured:
        logger.setLevel(level)

        # Only configure handlers if none exist yet
        if not logger.handlers:
            # Create stream handler with stderr
            handler = logging.StreamHandler()

            # Configure formatter with timestamp, name, and log level
            handler.setFormatter(logging.Formatter(fmt="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

            # Add handler to the logger
            logger.addHandler(handler)

        _configured = True
