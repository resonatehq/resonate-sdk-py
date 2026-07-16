"""Version compatibility checking for workspace member packages.

The platform packages (``resonate-sdk-aws``, future ``resonate-sdk-gcp``, ...)
version independently of ``resonate-sdk`` and declare the range of it they
support, so a normal install can never skew -- but ``pip install --no-deps``,
``--force-reinstall``, or a hand-edited environment still can, and the result
is a confusing failure deep inside the SDK. Each member calls
:func:`check_version_compatibility` at import to turn that into a one-line
warning, following dagster's ``check_dagster_package_version``.
"""

from __future__ import annotations

import importlib.metadata
import warnings


def check_version_compatibility(package: str | None) -> None:
    """Warn when the installed resonate-sdk falls outside *package*'s declared range.

    *package* is the caller's import package name (pass ``__package__``); the
    distribution name and its resonate-sdk requirement are resolved from the
    installed metadata at runtime, so members don't hardcode either.

    Silently returns when *package* is None, its distribution can't be
    resolved, either distribution is not installed (e.g. running from
    source), or ``packaging`` is unavailable to evaluate the range -- the
    check is best-effort, not a gate.
    """
    if package is None:
        return
    dists = importlib.metadata.packages_distributions().get(package)
    if not dists:
        return
    distribution = dists[0]
    try:
        # Function-local on purpose: packaging is not a runtime dependency,
        # and a module-level import would fail the whole SDK without it.
        from packaging.requirements import (  # noqa: PLC0415
            InvalidRequirement,
            Requirement,
        )
        from packaging.utils import canonicalize_name  # noqa: PLC0415
    except ImportError:
        return
    try:
        sdk = importlib.metadata.version("resonate-sdk")
        member = importlib.metadata.version(distribution)
        requires = importlib.metadata.requires(distribution) or []
    except importlib.metadata.PackageNotFoundError:
        return
    for raw in requires:
        try:
            requirement = Requirement(raw)
        except InvalidRequirement:
            continue
        if canonicalize_name(requirement.name) != "resonate-sdk":
            continue
        if requirement.marker is not None and not requirement.marker.evaluate():
            continue
        if not requirement.specifier.contains(sdk, prereleases=True):
            warnings.warn(
                f"{distribution} {member} requires {requirement}, but "
                f"resonate-sdk {sdk} is installed; reinstall a compatible "
                f"pair.",
                stacklevel=2,
            )
        return
