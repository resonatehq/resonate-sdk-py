from __future__ import annotations

import sys

from resonate.resonate import Resonate


def test_create_read_delete() -> None:
    resonate = Resonate.remote()
    schedule = resonate.schedules.create("foo", "0 * * * *", "foo", sys.maxsize)
    assert schedule == resonate.schedules.read("foo")
    resonate.schedules.delete("foo")
