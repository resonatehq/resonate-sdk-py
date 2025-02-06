from __future__ import annotations

import os
import traceback
import urllib
import urllib.parse
from functools import wraps
from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from resonate_sdk.logging import logger

if TYPE_CHECKING:
    from collections.abc import Callable


def exit_on_exception(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception:
            v = version("resonate-sdk")
            logger.exception(
                "An unexpected error happened.\n\nPlease report this issue so we can fix it as fast a possible:\n - https://github.com/resonatehq/resonate-sdk-py/issues/new?body=%s\n\n",  # noqa: E501
                urllib.parse.quote(
                    f"Resonate (version {v}) process exited with error:\n```bash\n{traceback.format_exc()}```"  # noqa: E501
                ),
            )
            os._exit(1)

    return wrapper  # type: ignore[return-value]
