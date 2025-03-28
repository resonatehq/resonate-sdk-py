from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, TypedDict

type Mesg = InvokeMesg | ResumeMesg | NotifyMesg


@dataclass
class InvokeMesg:
    type: Literal["invoke"]
    task: TaskMesg


@dataclass
class ResumeMesg:
    type: Literal["resume"]
    task: TaskMesg


@dataclass
class NotifyMesg:
    type: Literal["notify"]
    promise: Any


class TaskMesg(TypedDict):
    id: str
    counter: int
