"""Error handling across the Resonate durability boundary.

This example shows why durable functions catch :class:`ApplicationError`
rather than the original exception type a function raised.

When a durable function like ``bar`` fails, its exception does not propagate
in-process the way a normal Python call would. Resonate **encodes the failure,
writes it to a durable promise, and later decodes it** for whoever awaits the
result. That awaiter may be the same process, a different worker (via
``ctx.rpc``), or a process that recovered after a crash and never saw the
original exception at all.

A live Python exception cannot survive that round trip: its class, traceback,
and custom attributes can't be serialized to JSON and reconstructed on the
other side (the awaiting process may not even import the class). So Resonate
flattens every user failure to a single transport-safe shape -- its message
string -- and reconstructs it on the receiving side as an
:class:`ApplicationError`. By the time the error reaches the ``except`` block
in ``foo``, the original ``UsernameTakenError`` / ``ValueError`` type is gone;
only the message survives.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from typing import TYPE_CHECKING, Literal

from resonate.error import ApplicationError
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context


class UsernameTakenError(Exception):
    def __init__(self, msg: str) -> None:
        super().__init__(f"USERNAME_TAKEN: {msg}")


def bar(ctx: Context, username: str, age: int) -> str:
    existing_users = ["admin", "coder123", "python_fan"]

    if age < 0:
        msg = "Age cannot be negative."
        raise ValueError(msg)

    if username.lower() in existing_users:
        msg = f"The username '{username}' is already in use."
        raise UsernameTakenError(msg)

    return f"Registration successful for {username}!"


async def foo(
    ctx: Context, username: str, age: int, mode: Literal["run", "rpc"]
) -> None:
    try:
        v = await (
            ctx.run(bar, username, age)
            if mode == "run"
            else ctx.rpc("bar", username, age)
        )
        print(f"🎉 Success: {v}")
    except ApplicationError as e:
        if e.message.startswith("USERNAME_TAKEN"):
            print(f"⚠️ Business Rule Error: {e}")
            print("💡 Suggestion: Please try adding numbers to your username.")
        else:
            print(f"💥 Critical Unknown Error: {e}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("run", "rpc"), default="run")
    parser.add_argument(
        "--error",
        choices=("none", "taken", "value"),
        default="none",
        help="The type of error scenario you want to trigger",
    )
    args = parser.parse_args()

    if args.error == "taken":
        username, age = "admin", 25  # Triggers UsernameTakenError
    elif args.error == "value":
        username, age = "alice", -5  # Triggers ValueError
    else:
        username, age = "alice", 25  # Successful path

    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)

    # Registering both tasks so RPC coordination works flawlessly
    r.register(foo)
    r.register(bar)

    try:
        id = f"error-handling-{time.time_ns()}"
        await r.run(id, foo, username, age, args.mode).result()

    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
