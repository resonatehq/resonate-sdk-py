from __future__ import annotations

from resonate.resonate import Resonate


def test_create() -> None:
    resonate = Resonate.remote()
    schedule = resonate.schedules.create("foo", "foo", 10, promise_data="foo", promise_headers={"a": "1"}, description="clean up", promise_tags={"a": "b"})
    schedule_r = resonate.schedules.read("foo")
    assert schedule == schedule_r
    # resonate.schedules.delete("foo")
    # resonate.schedules.delete("foo")


def test_create_with_ikey() -> None:
    resonate = Resonate.remote()
    schedule = resonate.schedules.create("bar", "bar", 10, ikey="bar")
    schedule_deduped = resonate.schedules.create("bar", "abc", 10, ikey="bar")
    assert schedule == schedule_deduped
