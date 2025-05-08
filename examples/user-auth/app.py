# type: ignore[missing-import]
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import requests
from fastapi import FastAPI

from resonate import Context, Resonate, Yieldable

if TYPE_CHECKING:
    from collections.abc import Generator


resonate = Resonate.local()

# Workflows


@resonate.register()
def auth_user(ctx: Context, name: str, email: str) -> Generator[Yieldable, Any, None]:
    p = yield ctx.promise()
    yield ctx.lfc(send_auth_email, name, email, p.id)
    yield p


resonate.start()


# Functions


def send_auth_email(ctx: Context, name: str, email: str, promise_id: str) -> None:
    url = "https://send.api.mailtrap.io/api/send"
    payload = json.dumps(
        {
            "from": {"email": "hello@demomailtrap.co", "name": "Mailtrap Test"},
            "to": [{"email": email}],
            "template_uuid": "****153e7",
            "template_variables": {"company_info_name": "ResonateHQ", "name": name, "promiseId": promise_id},
        }
    )
    headers = {"Authorization": "Bearer d1999ad3b5ec0c4e68057df410e7cb39", "Content-Type": "application/json"}
    requests.request("POST", url, headers=headers, data=payload, timeout=10)


app = FastAPI()

# API handlers


@app.get("/resolve/{promise_id}")
async def resolve(promise_id: str) -> None:
    resonate.promises.resolve(id=promise_id, ikey=promise_id)  # ikey==promise_id to make the operation idempotent


@app.post("/")
async def auth(email: str, name: str) -> str:
    handle = auth_user.run(f"auth-user-{email}", name, email)
    try:
        handle.result(timeout=0.5)
    except TimeoutError:
        return "User has not been authenticated yet. Refresh the page for updates"
    else:
        return "User has been authenticated!"
