"""Enforce the release versioning contract.

Packages version independently: the git tag tracks the root SDK's version
(``--tag v<version>``, passed by CD from the release tag, enforces that),
and workspace members carry their own versions. A release publishes every
package at whatever version it holds; versions already on PyPI are skipped
at upload.

That skip is how an unchanged member rides along a root release, but it
would equally swallow a package that changed without a bump -- the upload
is skipped and none of the changes ship. With ``--released-at`` (the
release's publication time, also passed by CD) the check therefore fails
when the root's version, or the version of a member that changed since
the previous release tag, was already on PyPI before this release
existed. Comparing against the release time rather than bare existence
keeps re-runs after a mid-publish failure green: uploads from an earlier
attempt of the same release postdate it.

Each member must still declare a dependency on the root package whose
range admits the workspace's current root version -- uv silently resolves
workspace sources without consulting the specifier, so nothing else
catches a member whose published metadata claims a pairing the tree never
tested -- plus match the root's ``requires-python`` and resolve the root
from the workspace during development.

Metadata parsing and validation are delegated: pyproject-metadata (PyPA's
PEP 621 implementation) validates each ``[project]`` table and returns
typed fields, and packaging (pip's own version machinery) evaluates the
ranges. Only the ``[tool.uv.sources]`` check reads raw TOML, since tool
tables are outside the standard.

This assumes a two-tier workspace: the root SDK plus members that each
depend directly on it. A future member that depends on another member (or
on nothing) needs these checks extended first.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any

from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version
from pyproject_metadata import ConfigurationError, StandardMetadata


def _load(path: pathlib.Path) -> tuple[dict[str, Any], StandardMetadata]:
    """Parse and validate one pyproject.toml."""
    data = tomllib.loads(path.read_text())
    return data, StandardMetadata.from_pyproject(data, project_dir=path.parent)


def _previous_release_tag(current: Version) -> str | None:
    """Return the release tag with the highest version below *current*, if any."""
    listing = subprocess.run(
        ["git", "tag", "--list", "v*"], capture_output=True, text=True, check=True
    ).stdout
    older: list[tuple[Version, str]] = []
    for tag in listing.split():
        try:
            version = Version(tag.removeprefix("v"))
        except InvalidVersion:
            continue
        if version < current:
            older.append((version, tag))
    return max(older)[1] if older else None


def _changed_since(tag: str, directory: pathlib.Path) -> bool:
    """Return whether *directory* differs between *tag* and the checkout."""
    result = subprocess.run(
        ["git", "diff", "--quiet", tag, "--", str(directory)], check=False
    )
    if result.returncode not in (0, 1):
        sys.stderr.write(f"git diff against {tag} failed\n")
        sys.exit(result.returncode)
    return result.returncode == 1


def _stale_on_pypi(name: str, version: Version, released_at: datetime) -> bool:
    """Return whether *name* *version* reached PyPI before *released_at*.

    False when the version is not on PyPI at all, and when its files were
    uploaded by an earlier publish attempt of this same release (those
    postdate the release), so only leftovers from a previous release
    count as stale.
    """
    url = f"https://pypi.org/pypi/{name}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310
            files = json.load(response)["urls"]
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise
    return any(
        datetime.fromisoformat(file["upload_time_iso_8601"]) < released_at
        for file in files
    )


def main() -> None:
    """Check the root version against the tag and member constraints against the root."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="release tag the root version must match")
    parser.add_argument(
        "--released-at",
        type=datetime.fromisoformat,
        help="release publication time, for the changed-but-unbumped guard",
    )
    options = parser.parse_args()
    if (options.tag is None) != (options.released_at is None):
        parser.error("--tag and --released-at go together")

    try:
        _, root = _load(pathlib.Path("pyproject.toml"))
    except ConfigurationError as error:
        sys.stderr.write(f"pyproject.toml: {error}\n")
        sys.exit(1)
    if root.version is None:
        sys.stderr.write("pyproject.toml: version must be static, not dynamic\n")
        sys.exit(1)
    root_version = root.version

    errors = []

    previous = None
    if options.tag is not None:
        try:
            expected = Version(options.tag.removeprefix("v"))
        except InvalidVersion:
            errors.append(f"tag {options.tag} is not a PEP 440 version")
        else:
            if root_version != expected:
                errors.append(f"pyproject.toml: version {root_version} != tag {expected}")

        previous = _previous_release_tag(root_version)
        try:
            if _stale_on_pypi(root.canonical_name, root_version, options.released_at):
                errors.append(
                    f"pyproject.toml: {root.name} {root_version} predates this "
                    f"release on PyPI -- the tag was cut without bumping"
                )
        except OSError as error:
            errors.append(f"PyPI query for {root.name} {root_version} failed: {error}")

    for member in sorted(pathlib.Path("packages").glob("*/pyproject.toml")):
        try:
            data, project = _load(member)
        except ConfigurationError as error:
            errors.append(f"{member}: {error}")
            continue

        if project.version is None:
            errors.append(f"{member}: version must be static, not dynamic")

        # Members are tested in CI against the workspace root on every
        # Python the root supports and no other, so a member claiming a
        # different Python range is publishing untested metadata.
        if project.requires_python != root.requires_python:
            errors.append(
                f"{member}: requires-python {str(project.requires_python)!r} "
                f"!= {root.name} {str(root.requires_python)!r}"
            )

        dep = next(
            (
                r
                for r in project.dependencies
                if canonicalize_name(r.name) == root.canonical_name
            ),
            None,
        )
        if dep is None:
            errors.append(f"{member}: missing dependency on {root.name}")
        elif len(dep.specifier) == 0:
            errors.append(
                f"{member}: dependency on {root.name} needs a version constraint"
            )
        elif not dep.specifier.contains(root_version, prereleases=True):
            errors.append(
                f"{member}: '{dep}' does not admit {root.name} {root_version} "
                f"-- widen the range (and bump this member, its metadata changes)"
            )

        # Without a workspace source, uv quietly resolves the root package
        # from PyPI instead of the local tree, and the member's tests run
        # against already-released code.
        sources = data.get("tool", {}).get("uv", {}).get("sources", {})
        source = next(
            (
                v
                for k, v in sources.items()
                if canonicalize_name(k) == root.canonical_name
            ),
            None,
        )
        if not (isinstance(source, dict) and source.get("workspace") is True):
            errors.append(
                f"{member}: missing '{root.name} = {{ workspace = true }}' in [tool.uv.sources]"
            )

        # skip-existing silently skips an already-published version at
        # upload, so a member that changed since the previous release must
        # carry a version PyPI first saw during this release.
        if (
            options.released_at is not None
            and previous is not None
            and project.version is not None
            and _changed_since(previous, member.parent)
        ):
            try:
                stale = _stale_on_pypi(
                    project.canonical_name, project.version, options.released_at
                )
            except OSError as error:
                errors.append(
                    f"PyPI query for {project.name} {project.version} failed: {error}"
                )
            else:
                if stale:
                    errors.append(
                        f"{member}: changed since {previous} but {project.name} "
                        f"{project.version} predates this release on PyPI -- "
                        f"bump it, or none of its changes ship"
                    )

    for error in errors:
        sys.stderr.write(f"{error}\n")
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
