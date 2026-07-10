"""pydantic-ai shows durable execution for Pydantic AI agents.

``ResonateAgent`` wraps any Pydantic AI agent so that ``run()`` executes as a
durable workflow: every model request (and MCP round-trip) is checkpointed in
a durable promise, and a crashed run replays from the journal instead of
re-hitting the provider. The run's ``id`` is its identity: two runs with the
same id resolve to the same durable promise, so retrying a run after a crash
-- or calling it twice -- performs the work exactly once.

Requires the ``pydantic-ai`` extra::

    uv pip install -e '.[pydantic-ai]'

Uses Pydantic AI's offline ``TestModel`` so no API key is needed; swap in any
real model (e.g. ``'openai:gpt-5.2'``) for the real thing.

Start a Resonate server on localhost:8001 first (``resonate dev``), then::

    uv run python examples/pydantic-ai
"""

from __future__ import annotations

import asyncio
import os
import time

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.test import TestModel

from resonate.ext.pydantic_ai import ResonateAgent
from resonate.resonate import Resonate


class Answer(BaseModel):
    city: str
    country: str


async def main() -> None:
    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)

    # Any Pydantic AI agent; TestModel keeps the example offline. The required
    # `name` identifies the agent's durable run function in the registry.
    agent = Agent(
        TestModel(),
        name="geography",
        instructions="You're an expert in geography.",
        output_type=Answer,
    )
    durable_agent = ResonateAgent(agent, r)

    try:
        id = f"capital-of-mexico-{time.time_ns()}"

        print(f"[run] starting durable run id={id}")
        result = await durable_agent.run("What is the capital of Mexico?", id=id)
        assert isinstance(result.output, Answer)  # rebuilt as the model, not a dict
        print(f"[run] output: {result.output!r}")
        print(f"[run] usage: {result.usage}")

        # Same id -> same durable promise: the answer is served from the
        # journal, the model is not invoked again. After a crash, a re-run
        # with the same id recovers the result the same way.
        again = await durable_agent.run("What is the capital of Mexico?", id=id)
        assert again.output == result.output
        print("[run] re-run with the same id served from the journal")

        # Outside a durable run the wrapped agent stays usable as normal.
        direct = await agent.run("And of France?")
        print(f"[direct] output: {direct.output!r}")
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
