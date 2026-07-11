"""AWS Lambda entrypoint for the Resonate "pydantic-ai" example.

A **durable Pydantic AI agent running serverless**: ``ResonateAgent`` wraps a
multi-step support-triage agent and registers its run loop (``support.run``)
as a durable function on the FaaS shim. The Resonate server drives the run by
pushing tasks at this Lambda; every model request is checkpointed as a durable
promise, so each re-invocation replays from the journal instead of re-hitting
the model.

The agent is deliberately multi-step -- three model round-trips, each its own
checkpoint:

1. Model call 1 fans out two tool calls in parallel (``lookup_order`` +
   ``check_inventory``); ``parallel_ordered_events`` keeps their checkpoint
   order deterministic across replays.
2. Model call 2 escalates via ``escalate_refund``, a tool that reaches the
   ambient durable ``Context``: ``ctx.sleep`` (a durable timer -- the Lambda
   returns *suspended* and holds no state until the server re-pushes the task)
   and ``ctx.rpc(process_refund, ...)`` (a child task pushed back to this same
   function as its own invocation).
3. Model call 3 emits the final structured ``Triage`` output.

So one agent run spans several Lambda invocations, and no invocation repeats
work a previous one checkpointed.

Uses Pydantic AI's offline ``FunctionModel`` (scripted, deterministic, no API
key); swap in any real model (e.g. ``'openai:gpt-5.2'``) at agent construction
for the real thing.

Invoke from a client (see the README); the durable run takes one ``RunParams``
JSON object:

    resonate invoke triage.1 --func support.run
      --json-args '[{"user_prompt": "Triage order A-1"}]'
      --target http://127.0.0.1:3000/pydantic-ai
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.messages import ModelResponse, ToolCallPart, ToolReturnPart
from pydantic_ai.models.function import FunctionModel

from resonate.ext.pydantic_ai import ResonateAgent
from resonate.ext.pydantic_ai.context import workflow_context
from resonate.faas.aws import Resonate

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.models.function import AgentInfo

    from resonate.context import Context

resonate = Resonate()

ORDERS = {"A-1": "shipped"}


class Triage(BaseModel):
    """Structured result of a triage -- rebuilt as this model, not a dict."""

    order_id: str
    status: str
    refund: str
    action: str


def support_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Act as a scripted, deterministic stand-in for a real LLM.

    Three steps, keyed off which tool results are already in the history --
    each return is one model request, checkpointed as one durable promise.
    """
    returned = {
        part.tool_name
        for message in messages
        for part in message.parts
        if isinstance(part, ToolReturnPart)
    }
    if "lookup_order" not in returned:
        # Step 1: fan out two lookups at once (parallel tool calls).
        return ModelResponse(
            parts=[
                ToolCallPart(tool_name="lookup_order", args={"order_id": "A-1"}),
                ToolCallPart(tool_name="check_inventory", args={"sku": "sku-9"}),
            ]
        )
    if "escalate_refund" not in returned:
        # Step 2: escalate -- the tool drives ctx.sleep + ctx.rpc.
        return ModelResponse(
            parts=[
                ToolCallPart(
                    tool_name="escalate_refund", args={"order_id": "A-1", "amount": 42}
                )
            ]
        )
    # Step 3: emit the final structured output via the agent's output tool.
    output_tool = info.output_tools[0]
    return ModelResponse(
        parts=[
            ToolCallPart(
                tool_name=output_tool.name,
                args={
                    "order_id": "A-1",
                    "status": "shipped",
                    "refund": "refunded $42 for A-1",
                    "action": "notify customer of refund and tracking number",
                },
            )
        ]
    )


@resonate.register
async def process_refund(ctx: Context, order_id: str, amount: int) -> str:
    """Settle a refund; the agent's tool dispatches here via ``ctx.rpc``.

    Runs as its own pushed task -- its own Lambda invocation -- while the
    suspended agent run holds no compute.
    """
    return f"refunded ${amount} for {order_id}"


def build_agent() -> Agent[object, str]:
    """Build the triage agent: two lookup tools plus a durable escalation."""
    agent = Agent(
        FunctionModel(support_model),
        name="support",
        instructions="You triage customer support tickets about orders.",
        output_type=Triage,
    )

    @agent.tool_plain
    def lookup_order(order_id: str) -> str:
        # Runs inline in the workflow body (re-executed on replay, cheap).
        return ORDERS.get(order_id, "unknown")

    @agent.tool_plain
    def check_inventory(sku: str) -> str:
        return "in stock"

    @agent.tool_plain
    async def escalate_refund(order_id: str, amount: int) -> str:
        # Function tools run inline in the workflow body, so the ambient
        # durable Context is available -- Resonate primitives from inside a tool.
        ctx = workflow_context()
        assert ctx is not None, "the tool runs inside the durable workflow body"

        # Durable cooldown: this invocation returns "suspended"; the server
        # re-pushes the task when the timer fires. Checkpointed, so a replay
        # does not restart the wait.
        await ctx.sleep(timedelta(seconds=2))

        # Dispatch the refund as its own durable child task, routed back to
        # this same Lambda as a separate invocation.
        return await ctx.rpc(process_refund, order_id, amount)

    return agent


# Wrapping registers the agent's run loop as the durable function
# ``support.run`` -- the name a client invokes.
ResonateAgent(build_agent(), resonate)

lambda_handler = resonate.handler()
