from __future__ import annotations

from threading import Event

from resonate import Context, Resonate

resonate = Resonate.remote(host="http://localhost", group="math")


@resonate.register
def add(ctx: Context, a: int, b: int) -> int:
    return a + b


if __name__ == "__main__":
    resonate.start()
    Event().wait()
