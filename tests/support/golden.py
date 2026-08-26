"""Golden-file comparison.

The workflow the post describes: generate the output once, eyeball it, commit
it. From then on the committed file *is* the expected value, and a change to
the SDK's wire format or execution-tree shape shows up as a reviewable diff
rather than as a runtime failure against a live server.

Regenerate every golden with::

    RESONATE_UPDATE_GOLDEN=1 uv run pytest

then read the diff before committing it. That review step is the whole point --
an unread golden proves nothing.
"""

from __future__ import annotations

import os
from pathlib import Path

GOLDEN_DIR = Path(__file__).parent.parent / "golden"

_UPDATE_ENV = "RESONATE_UPDATE_GOLDEN"


def golden_path(name: str) -> Path:
    """Absolute path of the golden file called ``name``."""
    return GOLDEN_DIR / name


def assert_golden(name: str, actual: str) -> None:
    """Compare ``actual`` against the committed golden ``name``.

    Fails directly rather than returning a result, so a test reads as one line.
    A missing golden is written and then failed: the first run records the
    output, and the failure is the prompt to go and read it.
    """
    path = golden_path(name)
    normalized = actual if actual.endswith("\n") else actual + "\n"

    if os.environ.get(_UPDATE_ENV):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(normalized)
        return

    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(normalized)
        msg = (
            f"golden {name!r} did not exist and has been written to {path}. "
            f"Read it, confirm it is correct, and commit it."
        )
        raise AssertionError(msg)

    expected = path.read_text()
    if expected != normalized:
        msg = (
            f"golden {name!r} mismatch.\n"
            f"--- expected ({path})\n{expected}\n"
            f"--- actual\n{normalized}\n"
            f"If the change is intended, re-run with {_UPDATE_ENV}=1 and "
            f"review the diff."
        )
        raise AssertionError(msg)
