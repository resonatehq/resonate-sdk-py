"""Tests for :func:`resonate.version.check_version_compatibility`."""

from __future__ import annotations

import importlib.metadata
import sys
import warnings

import pytest

from resonate.version import check_version_compatibility


def test_warns_when_sdk_falls_outside_the_member_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importlib.metadata,
        "packages_distributions",
        lambda: {"resonate_aws": ["resonate-sdk-aws"]},
    )
    versions = {"resonate-sdk": "0.8.0", "resonate-sdk-aws": "0.7.4"}
    monkeypatch.setattr(importlib.metadata, "version", versions.__getitem__)
    monkeypatch.setattr(
        importlib.metadata,
        "requires",
        lambda _: ["msgspec>=0.21.1,<1", "resonate-sdk>=0.7.4,<0.8"],
    )

    with pytest.warns(
        UserWarning,
        match=r"resonate-sdk-aws 0.7.4 requires resonate-sdk<0.8,>=0.7.4, but "
        r"resonate-sdk 0.8.0 is installed",
    ):
        check_version_compatibility("resonate_aws")


def test_silent_when_sdk_is_inside_the_member_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importlib.metadata,
        "packages_distributions",
        lambda: {"resonate_aws": ["resonate-sdk-aws"]},
    )
    versions = {"resonate-sdk": "0.7.9", "resonate-sdk-aws": "0.7.4"}
    monkeypatch.setattr(importlib.metadata, "version", versions.__getitem__)
    monkeypatch.setattr(
        importlib.metadata,
        "requires",
        lambda _: ["msgspec>=0.21.1,<1", "resonate-sdk>=0.7.4,<0.8"],
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        check_version_compatibility("resonate_aws")


def test_silent_when_distribution_is_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importlib.metadata,
        "packages_distributions",
        lambda: {"resonate_aws": ["resonate-sdk-aws"]},
    )

    def missing(name: str) -> str:
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "version", missing)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        check_version_compatibility("resonate_aws")


def test_silent_when_package_is_not_mapped_to_a_distribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(importlib.metadata, "packages_distributions", dict)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        check_version_compatibility("resonate_aws")


def test_silent_when_packaging_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        importlib.metadata,
        "packages_distributions",
        lambda: {"resonate_aws": ["resonate-sdk-aws"]},
    )
    monkeypatch.delitem(sys.modules, "packaging.requirements", raising=False)
    monkeypatch.setitem(sys.modules, "packaging", None)  # type: ignore[arg-type]

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        check_version_compatibility("resonate_aws")
