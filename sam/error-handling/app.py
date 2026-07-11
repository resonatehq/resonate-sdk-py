r"""AWS Lambda entrypoint for the Resonate "error-handling" example.

Shows how a failure raised in a durable child (``bar``) reaches whoever awaits
it across the durability boundary. Resonate encodes the failure onto the
durable promise and decodes it for the awaiter: a Python awaiter that can
import the class gets the original type back (``foo`` catches
``UsernameTakenError`` / ``ValueError`` directly); otherwise it falls back to
``ApplicationError`` carrying the message.

``retry_policy=Never()`` so a raised error settles the promise immediately
instead of being retried.

``foo`` takes a ``mode`` ("run" | "rpc") selecting how it calls ``bar``. Invoke
from a client (see the README):

    resonate invoke eh.1 --func foo \\
      --arg admin --arg 25 --arg run \\
      --target http://127.0.0.1:3000/error-handling
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from resonate.error import ApplicationError
from resonate.faas.aws import Resonate
from resonate.retry import Never

if TYPE_CHECKING:
    from resonate.context import Context

resonate = Resonate(retry_policy=Never())


class UsernameTakenError(Exception):
    """Raised when a username is already registered.

    Kept as a plain ``Exception`` subclass so it round-trips through the codec's
    pickle cleanly (no message reformatting in ``__init__``).
    """


@resonate.register
def bar(ctx: Context, username: str, age: int) -> str:
    existing_users = ["admin", "coder123", "python_fan"]

    if age < 0:
        msg = "Age cannot be negative."
        raise ValueError(msg)

    if username.lower() in existing_users:
        msg = f"The username '{username}' is already in use."
        raise UsernameTakenError(msg)

    return f"Registration successful for {username}!"


@resonate.register
async def foo(
    ctx: Context, username: str, age: int, mode: Literal["run", "rpc"]
) -> None:
    try:
        await (
            ctx.run(bar, username, age)
            if mode == "run"
            else ctx.rpc("bar", username, age)
        )
    except UsernameTakenError:
        pass
    except ValueError:
        pass
    except ApplicationError as e:
        msg = """ApplicationError is a fallback error.
        Used when the raised error cannot be deserialized by pickle.
        Not applicable in this example."""
        raise AssertionError(msg) from e


lambda_handler = resonate.handler()
