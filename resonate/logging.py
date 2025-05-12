"""Logging configuration."""

from __future__ import annotations

import logging

# Name the logger after the package
logger = logging.getLogger(__package__)

# Global state tracking
_initialized = False


def set_level(level: int) -> None:
    global _initialized  # noqa: PLW0603

    # Update log level only if needed
    current_level = logger.getEffectiveLevel()
    if current_level != level:
        logger.setLevel(level)

    # Configure handler once
    if not _initialized:
        # Add handler only if none exist
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(fmt="%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
            logger.addHandler(handler)

        # Mark initialization as complete
        _initialized = True
