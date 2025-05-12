"""Logging configuration."""

from __future__ import annotations

import logging

_configured: bool = False
# Name the logger after the package
logger = logging.getLogger(__package__)


class LogRecordFormat(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        assert hasattr(record, "root_id"), "log records must have a root_id"
        assert hasattr(record, "id"), "log records must have an id"
        assert hasattr(record, "event"), "log records must have an event"

        return super().format(record)


def set_level(level: int) -> None:
    global _configured  # noqa: PLW0603

    if not _configured:
        logger.setLevel(level)

        # Only configure handlers if none exist yet
        if not logger.handlers:
            # Create stream handler with stderr
            handler = logging.StreamHandler()

            # Configure formatter with timestamp, name, and log level
            handler.setFormatter(LogRecordFormat(fmt="%(asctime)s [%(name)s] %(levelname)s: RootPromiseID=%(root_id)s PromiseID=%(id)s Event=%(event)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))

            # Add handler to the logger
            logger.addHandler(handler)

        _configured = True
