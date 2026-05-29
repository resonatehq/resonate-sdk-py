"""saga shows a multi-step durable workflow with compensation on failure
(the canonical "distributed transactions" pattern):

    book_trip:
        1. reserve_flight
        2. reserve_hotel   (on failure: release_flight)
        3. charge_card     (on failure: release_hotel, release_flight)

Each step is its own registered function dispatched via ``ctx.rpc``. Step
settlement is recorded in a durable promise, so if the worker crashes between
two steps a restart skips the steps that already settled and runs only the
missing ones -- including the compensations.

Mirrors the Go SDK's ``saga`` example. Start a Resonate server on
localhost:8001 first (``resonate dev``), then either of::

    uv run python examples/saga                  # happy path
    uv run python examples/saga --fail charge    # both compensations run

Note on replay: a durable orchestrator re-executes from the top each time it
awaits a not-yet-settled future, so any side effect (a ``print``, an external
call) belongs in a leaf step function -- which settles once and never re-runs
-- not in ``book_trip`` itself. That is why every log line below lives in a
step, never in the orchestrator.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from typing import TYPE_CHECKING

from resonate.error import ApplicationError
from resonate.resonate import Resonate

if TYPE_CHECKING:
    from resonate.context import Context

# -- Step functions (leaves: each prints once, settles once) ---------------


async def reserve_flight(
    ctx: Context,
    customer: str,
    frm: str,
    to: str,
) -> str:
    ref = f"FL-{customer}-{frm}-{to}"
    print(f"  [reserve_flight] reserved {ref}")
    return ref


async def reserve_hotel(
    ctx: Context,
    customer: str,
    city: str,
    fail: bool,
) -> str:
    if fail:
        print(f"  [reserve_hotel] FAILED for {customer} in {city}")
        msg = f"no rooms available in {city}"
        raise ApplicationError(msg)
    ref = f"HT-{customer}-{city}"
    print(f"  [reserve_hotel] reserved {ref}")
    return ref


async def charge_card(ctx: Context, customer: str, amount: int, fail: bool) -> str:
    if fail:
        print(f"  [charge_card] FAILED for {customer} (${amount})")
        msg = f"card declined for ${amount}"
        raise ApplicationError(msg)
    ref = f"CH-{customer}-{amount}"
    print(f"  [charge_card] charged {ref}")
    return ref


async def release_flight(ctx: Context, ref: str) -> str:
    print(f"  [release_flight] released {ref}")
    return ref


async def release_hotel(ctx: Context, ref: str) -> str:
    print(f"  [release_hotel] released {ref}")
    return ref


# -- Saga orchestrator -----------------------------------------------------


async def book_trip(
    ctx: Context,
    customer: str,
    frm: str,
    to: str,
    amount: int,
    fail_at: str,
) -> tuple[str, str, str]:
    # Step 1: flight
    flight = await ctx.rpc("reserve_flight", customer=customer, frm=frm, to=to)

    # Step 2: hotel (compensate the flight on failure)
    try:
        hotel = await ctx.rpc(
            "reserve_hotel",
            customer=customer,
            city=to,
            fail=fail_at == "hotel",
        )
    except ApplicationError:
        await compensate(ctx, "", flight)
        raise

    # Step 3: charge (compensate hotel + flight on failure, reverse order)
    try:
        charge = await ctx.rpc(
            "charge_card",
            customer=customer,
            amount=amount,
            fail=fail_at == "charge",
        )
    except ApplicationError:
        await compensate(ctx, hotel, flight)
        raise

    return flight, hotel, charge


async def compensate(ctx: Context, hotel_ref: str, flight_ref: str) -> None:
    """Run the inverse of any completed steps in reverse order.

    Empty refs are skipped. Each compensation is its own durable promise, so a
    crash mid-rollback resumes at the first unsettled one.
    """
    if hotel_ref:
        await ctx.rpc("release_hotel", ref=hotel_ref)
    if flight_ref:
        await ctx.rpc("release_flight", ref=flight_ref)


# -- main ------------------------------------------------------------------


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fail", choices=("hotel", "charge"), default="")
    args = parser.parse_args()

    url = os.environ.get("RESONATE_URL", "http://localhost:8001")
    r = Resonate(url=url)
    for fn in (
        book_trip,
        reserve_flight,
        reserve_hotel,
        charge_card,
        release_flight,
        release_hotel,
    ):
        r.register(fn)

    try:
        id = f"saga-{time.time_ns()}"
        print(f"[book_trip] starting workflow id={id} fail_at={args.fail!r}")
        handle = r.run(id, book_trip, "alice", "SFO", "JFK", 850, args.fail)
        try:
            flight_ref, hotel_ref, charge_ref = await handle.result()
        except ApplicationError as exc:
            print(f"[book_trip] FAILED: {exc}")
            return
        print(
            f"[book_trip] OK: flight={flight_ref} hotel={hotel_ref} charge={charge_ref}"
        )
    finally:
        await r.stop()


if __name__ == "__main__":
    asyncio.run(main())
