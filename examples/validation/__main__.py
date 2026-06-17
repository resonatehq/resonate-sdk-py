"""validation shows ctx.run checking a function's output before it is persisted.

Some leaves are non-deterministic -- the classic case is a call to an LLM, which
can return malformed or incomplete output. ``ctx.options(validate=fn).run(...)``
runs ``fn`` on the return value *before* Resonate persists it: a falsy verdict
(or a raised exception) discards the result and retries the leaf, exactly like an
outright failure. Only once the output validates is it stored as the child's
durable result, so an invalid value is never written.

Validation reuses the same machinery as ``retries`` (see that example), so it
inherits the same rule: only a **leaf** (a function with no durable ``ctx.*`` op)
is ever re-run. Our leaf is ``extract_contact``, a call to a fake LLM.

One workflow, one validator (wants a ``name`` and an ``email``), two prompts:

* ``onboard("ada")``   -- the model warms up and returns a valid contact on the
  third try, so the workflow succeeds.
* ``onboard("grace")`` -- the model stays broken and never returns an email, so
  retries are exhausted and ``ctx.run`` settles a ``ValidationError``.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/validation
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from resonate.error import ValidationError
from resonate.resonate import Resonate
from resonate.retry import Constant

if TYPE_CHECKING:
    from resonate.context import Context

# Retry up to 5 times with no delay between attempts (keeps the demo instant).
POLICY = Constant(max_retries=5, delay=0)

# What the model is "extracting" from -- keyed by a short prompt id.
CONTACTS = {
    "ada": {"name": "Ada Lovelace", "email": "ada@example.com"},
    "grace": {"name": "Grace Hopper", "email": "grace@example.com"},
}


@dataclass
class Model:
    """A fake LLM, keyed by prompt id.

    'ada' warms up -- unparseable prose, then JSON missing the email, then a
    well-formed contact. 'grace' stays broken -- always JSON missing the email,
    so the validator never accepts it.
    """

    calls: dict[str, int] = field(default_factory=dict)

    def complete(self, prompt: str) -> str:
        n = self.calls[prompt] = self.calls.get(prompt, 0) + 1
        print(f"  model call {n} for {prompt!r}")
        contact = CONTACTS[prompt]
        no_email = json.dumps({"name": contact["name"]})
        if prompt == "grace":
            return no_email  # never includes the email
        if n == 1:
            return f"Sure! The contact is {contact['name']}."  # prose, not JSON
        if n == 2:
            return no_email  # JSON, but missing the email
        return json.dumps(contact)  # well-formed


async def extract_contact(ctx: Context, prompt: str) -> str:
    # A leaf -- no ctx.* durable op -- so Resonate may retry it when the
    # validator rejects its output.
    return ctx.get_dependency(Model).complete(prompt)


def is_valid_contact(raw: str) -> bool:
    """Accept only well-formed JSON carrying a name and an email.

    A validator returns a bool. Returning ``False`` -- or raising, as the
    ``json.loads`` below does on the model's prose reply -- makes ``ctx.run``
    discard the value and retry the leaf.
    """
    contact = json.loads(raw)  # not JSON -> JSONDecodeError -> retry
    ok = "name" in contact and "@" in contact.get("email", "")
    print(f"    validator: {'ok' if ok else f'incomplete {contact}'}")
    return ok


async def onboard(ctx: Context, prompt: str) -> str:
    # Validate the LLM output before it is durably persisted; the accepted value
    # is what gets stored as this ctx.run child's result.
    raw = await ctx.options(retry_policy=POLICY, validate=is_valid_contact).run(
        extract_contact, prompt
    )
    contact = json.loads(raw)
    return f"{contact['name']} <{contact['email']}>"


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    r.with_dependency(Model())
    r.register(onboard)
    ts = time.time_ns()
    try:
        print("onboard('ada') -- output validated before it is stored:")
        ok = await r.run(f"validate-ok-{ts}", onboard, "ada").result()
        print(f"  -> {ok}")

        print("onboard('grace') -- output never validates:")
        try:
            await r.run(f"validate-fail-{ts}", onboard, "grace").result()
        except ValidationError as exc:
            # The original type survives the durability boundary, so the caller
            # can tell "never validated" apart from an ordinary domain failure.
            print(f"  -> rejected ({type(exc).__name__}): {exc}")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
