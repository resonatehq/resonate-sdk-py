"""The ``poll://`` address format, pinned.

These are :mod:`resonate.connections.sse`'s own addresses, not a format the SDK
imposes -- a connector names its destinations however its substrate does. They
get a suite of their own anyway because of *how* they fail: a malformed address
raises nowhere. The server accepts it, stores it, and then never delivers, so
the task sits until its lease lapses and is re-queued, forever. Nothing in a
connector's own tests would notice.
"""

from __future__ import annotations

import pytest

from resonate.connections.sse import ANYCAST, UNICAST, resolve_target, unicast
from resonate.error import InvalidIdError


def test_unicast_names_a_concrete_process() -> None:
    assert unicast("workers", "7f3a") == "poll://uni@workers/7f3a"


def test_resolve_target_names_a_group_with_no_pid() -> None:
    """A resolver is handed a group whose members it does not know."""
    assert resolve_target("workers") == "poll://any@workers"


def test_the_two_forms_differ_only_in_kind() -> None:
    assert (
        unicast("workers", "7f3a").replace(UNICAST, ANYCAST)
        == "poll://any@workers/7f3a"
    )


@pytest.mark.parametrize(
    "group",
    ["", "has@sign", "has/slash", "has:colon", "has?query", "has#frag"],
)
def test_a_group_that_would_change_the_url_split_is_refused(group: str) -> None:
    with pytest.raises(InvalidIdError):
        unicast(group, "pid")


def test_uppercase_is_refused_rather_than_folded() -> None:
    """The server lowercases the URL host, so an uppercase group cannot round trip.

    Folding it silently here would produce a valid-looking address pointing at
    a group nobody listens on -- the exact silent drop these checks prevent.
    """
    with pytest.raises(InvalidIdError, match="lowercase"):
        unicast("Workers", "7f3a")

    with pytest.raises(InvalidIdError, match="lowercase"):
        resolve_target("Workers")


def test_an_empty_pid_is_refused() -> None:
    with pytest.raises(InvalidIdError):
        unicast("workers", "")
