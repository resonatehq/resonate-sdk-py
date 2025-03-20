from __future__ import annotations

from typing import TYPE_CHECKING

from resonate.errors import ValidationError

if TYPE_CHECKING:
    from collections.abc import Callable


def validate(cond: bool, msg: Callable[[], str] | str) -> None:
    if not cond:
        raise ValidationError(msg if isinstance(msg, str) else msg())
