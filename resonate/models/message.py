from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from resonate.models.durable_promise import DurablePromise
from resonate.models.store import Store
from resonate.models.task import Task

if TYPE_CHECKING:
    from resonate.models.store import Store

@dataclass
class Mesg:
    type: Literal["invoke", "resume", "notify"]

    @classmethod
    def from_dict(cls, store: Store, data: dict[str, Any]) -> Mesg:
        match data["type"]:
            case "invoke":
                return InvokeMesg.from_dict(store, data)
            case "resume":
                return ResumeMesg.from_dict(store, data)
            case "notify":
                return NotifyMesg.from_dict(store, data)
            case _:
                # TODO: handle this better
                raise ValueError("Unknown message type")

@dataclass
class InvokeMesg(Mesg):
    task: Task

    @classmethod
    def from_dict(cls, store: Store, data: dict[str, Any]) -> InvokeMesg:
        return cls(type="invoke", task=Task.from_dict(store, data["task"]))


@dataclass
class ResumeMesg(Mesg):
    task: Task

    @classmethod
    def from_dict(cls, store: Store, data: dict[str, Any]) -> ResumeMesg:
        return cls(type="resume", task=Task.from_dict(store, data["task"]))


@dataclass
class NotifyMesg(Mesg):
    promise: DurablePromise

    @classmethod
    def from_dict(cls, store: Store, data: dict[str, Any]) -> NotifyMesg:
        return cls(type="notify", promise=DurablePromise.from_dict(store, data["promise"]))
