from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, TypedDict


type Mesg = InvokeMesg | ResumeMesg | NotifyMesg


class InvokeMesg(TypedDict):
    type: Literal["invoke"]
    task: TaskMesg


class ResumeMesg(TypedDict):
    type: Literal["resume"]
    task: TaskMesg


class NotifyMesg(TypedDict):
    type: Literal["notify"]
    promise: Any


class TaskMesg(TypedDict):
    id: str
    counter: int
