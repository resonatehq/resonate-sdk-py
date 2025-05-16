from __future__ import annotations

import logging

logger = logging.getLogger(__package__)
logger.setLevel(logging.DEBUG)


class RecordFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Ensure required attributes exist with safe defaults
        record.cid = getattr(record, "cid")
        record.id = getattr(record, "id")
        record.attempt = f"(attempt={attempt})" if (attempt := getattr(record, "attempt", 1)) > 1 else ""
        return super().format(record)


# Improved filters with safe attribute access
def application_level(record: logging.LogRecord) -> bool:
    return getattr(record, "application", False)


def platform_level(record: logging.LogRecord) -> bool:
    return getattr(record, "platform", False)


application_handler = logging.StreamHandler()
application_handler.addFilter(application_level)
application_handler.setFormatter(RecordFormatter(fmt="%(asctime)s %(levelname)s [%(name)s::%(threadName)s] %(cid)s::%(id)s %(message)s %(attempt)s"))

platform_handler = logging.StreamHandler()
platform_handler.addFilter(platform_level)
platform_handler.setFormatter(logging.Formatter(fmt="%(asctime)s %(levelname)s [%(name)s::%(threadName)s] %(message)s"))

logger.addHandler(application_handler)
logger.addHandler(platform_handler)
