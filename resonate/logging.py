from __future__ import annotations

import logging

logger = logging.getLogger(__package__)
if not logger.handlers:
    formatter = logging.Formatter(
        "[%(asctime)s] [%(name)s::%(threadName)s] [%(levelname)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
