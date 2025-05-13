"""Logging configuration."""

from __future__ import annotations

import logging
import os


class LogRecordFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        record.computation_id = getattr(record, "computation_id", "none")
        record.id = getattr(record, "id", "")
        record.attempt = f"(attempt={attempt})" if (attempt := getattr(record, "attempt", 1)) > 1 else ""
        return super().format(record)


def _configure_logger() -> logging.Logger:
    # Name the logger after the package
    logger = logging.getLogger(__package__)
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
    if not logger.handlers:
        # Create stream handler with stderr
        handler = logging.StreamHandler()

        # Configure formatter with timestamp, name, and log level
        handler.setFormatter(
            LogRecordFormatter(
                fmt="%(asctime)s %(levelname)s [%(name)s] %(computation_id)s::%(id)s %(message)s %(attempt)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ),
        )

        # Add handler to the logger
        logger.addHandler(handler)
    return logger


logger = _configure_logger()
