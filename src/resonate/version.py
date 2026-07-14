"""Lockstep version checking for workspace member packages.

The platform packages (``resonate-sdk-aws``, future ``resonate-sdk-gcp``, ...)
release in lockstep with ``resonate-sdk`` and pin it exactly, so a normal
install can never skew -- but ``pip install --no-deps``, ``--force-reinstall``,
or a hand-edited environment still can, and the result is a confusing failure
deep inside the SDK. Each member calls :func:`check_lockstep_version` at
import to turn that into a one-line warning, following dagster's
``check_dagster_package_version``.
"""

from __future__ import annotations

import importlib.metadata
import warnings


def check_lockstep_version(distribution: str) -> None:
    """Warn when *distribution*'s installed version differs from resonate-sdk.

    Silently returns when either distribution is not installed (e.g. running
    from source), since there is nothing meaningful to compare.
    """
    try:
        sdk = importlib.metadata.version("resonate-sdk")
        member = importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        return
    if member != sdk:
        warnings.warn(
            f"{distribution} {member} requires resonate-sdk=={member}, but "
            f"resonate-sdk {sdk} is installed. The two packages release in "
            f"lockstep; reinstall with matching versions.",
            stacklevel=2,
        )
