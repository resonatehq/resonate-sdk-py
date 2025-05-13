"""Logging configuration."""

from __future__ import annotations

import logging
import os

# Name the logger after the package


def _configure_logger() -> logging.Logger:
    logger = logging.getLogger(__package__)
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    if not logger.handlers:
        # Create stream handler with stderr
        handler = logging.StreamHandler()

        # Configure formatter with timestamp, name, and log level
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)s [%(name)s] %(computation_id)s::%(id)s %(message)s %(attempt)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                defaults={"id": "", "computation_id": "none", "attempt": ""},
            ),
        )

        # Add handler to the logger
        logger.addHandler(handler)
    return logger


logger = _configure_logger()
