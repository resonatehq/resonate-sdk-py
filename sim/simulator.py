from __future__ import annotations

import sys
import urllib.parse
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from resonate import Context
from resonate.dependencies import Dependencies
from resonate.errors import ResonateStoreError
from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
    ClaimTaskReq,
    ClaimTaskRes,
    Command,
    CompleteTaskReq,
    CompleteTaskRes,
    CreateCallbackReq,
    CreateCallbackRes,
    CreatePromiseReq,
    CreatePromiseRes,
    CreatePromiseWithTaskReq,
    CreatePromiseWithTaskRes,
    Function,
    Invoke,
    Network,
    Receive,
    RejectPromiseReq,
    RejectPromiseRes,
    ResolvePromiseReq,
    ResolvePromiseRes,
    Resume,
    Return,
)
from resonate.models.options import Options
from resonate.models.result import Ko, Ok, Result
from resonate.models.task import Task
from resonate.resonate import FuncCallingConvention
from resonate.scheduler import Done, More, Scheduler
from resonate.stores.local import LocalStore

if TYPE_CHECKING:
    from collections.abc import Callable
    from random import Random

    from resonate.registry import Registry

type Addr = Unicast | Anycast

@dataclass
class Unicast:
    id: str

@dataclass
class Anycast:
    gid: str
    pid: str | None = None

UUID = 0

class Message:
    def __init__(self, send: str, recv: Addr, data: Any, time: int) -> None:
        global UUID  # noqa: PLW0603
        UUID += 1
        self.uuid = UUID
        self.send = send
        self.recv = recv
        self.data = data
        self.time = time

    def __repr__(self) -> str:
        return f"Message(from={self.send}, to={self.recv}, payload={self.data}, time={self.time})"

class StepClock:
    def __init__(self) -> None:
        self._time = 0

    def time(self) -> int:
        return self._time

    def set_time(self, time: int) -> None:
        assert time >= self._time, "The arrow of time only flows forward."
        self._time = time

class Simulator:
    def __init__(self, r: Random) -> None:
        self.r = r
        self.time = 0
        self.logs = []
        self.buffer: list[Message] = []
        self.components: list[Component] = []

    def add_component(self, component: Component) -> None:
        self.components.append(component)

    def send_msg(self, addr: Addr, data: Any) -> Message:
       msg = Message("E", addr, data, self.time)
       self.buffer.append(msg)
       return msg

    def run(self, steps: int) -> None:
        for _ in range(steps):
            self.step()

    def step(self) -> None:
        self.time += 1
        # print(f"\n=== Step {self.time} ===")

        recv: list[Message] = []
        outgoing: dict[Component, list[Message]] = {}
        components_by_uni: dict[str, Component] = {}
        components_by_any: dict[str, list[Component]] = {}

        for comp in self.components:
            assert comp.uni not in components_by_uni
            components_by_uni[comp.uni] = comp
            components_by_any.setdefault(comp.any, []).append(comp)

        for send in self.buffer:
            match send.recv:
                case Unicast(uni):
                    if comp := components_by_uni.get(uni):
                        self.buffer.remove(send)
                        outgoing.setdefault(comp, []).append(send)
                case Anycast(any, uni):
                    comp: Component | None = None
                    if uni in components_by_uni:
                        comp = components_by_uni[uni]
                    if not comp and any in components_by_any:
                        comp = self.r.choice(components_by_any[any])
                    if comp:
                        self.buffer.remove(send)
                        outgoing.setdefault(comp, []).append(send)

        for comp, send in outgoing.items():
            self.r.shuffle(send)

            for msg in send:
                self.logs.append({"type": "recv", "uuid": msg.uuid, "source": msg.send, "target": comp.uni, "data": msg.data})

            for msg in comp.step(self.time, send):
                self.logs.append({"type": "send", "uuid": msg.uuid, "source": comp.uni, "target": msg.recv, "data": msg.data})
                recv.append(msg)

        # Add outgoing messages to the buffer
        self.buffer.extend(recv)

    def freeze(self) -> str:
        return ":".join(component.freeze() for component in self.components)

class Component:
    def __init__(self, uni: str, any: str) -> None:
        self.uni = uni
        self.any = any
        self.time = 0
        self.msgcount = 0
        self.outgoing = []
        self.awaiting = {}
        self.deferred = []
        self.init()

    def init(self) -> None:
        pass

    def step(self, time: int, messages: list[Message]) -> list[Message]:
        self.time = time
        self.outgoing = []
        self.handle_deferred()
        self.deferred = []
        self.handle_response(messages)
        self.handle_messages(messages)
        self.next()
        return self.outgoing

    def next(self) -> None:
        pass

    def send_msg(self, addr: Addr, data: Any) -> Message:
        """Send a message to another component."""
        msg = Message(self.uni, addr, data, self.time)
        self.outgoing.append(msg)
        return msg

    def send_req(self, addr: Addr, data: Any, callback: Callable[[Message], None], timeout: int = 0) -> Message:
        """Send a request and register a callback for handling the response."""
        self.msgcount += 1
        self.awaiting[self.msgcount] = callback
        msg = Message(self.uni, addr, {"type": "req", "uuid": self.msgcount, "data": data}, self.time)
        self.outgoing.append(msg)
        return msg

    def defer(self, callback: Callable[[], None], timeout: int = 0) -> None:
        """Defers a callback to be called after a certain time."""
        self.deferred.append(callback)

    def handle_deferred(self) -> None:
        for callback in self.deferred:
            callback()

    def handle_response(self, messages: list[Message]) -> None:
        for message in messages:
            if isinstance(message.data, dict) and message.data.get("type") == "res":
                uuid = message.data.get("uuid")
                if uuid in self.awaiting:
                    callback = self.awaiting.pop(uuid)
                    callback(message)

    def handle_messages(self, messages: list[Message]) -> None:
        for message in messages:
            if not(isinstance(message.data, dict) and message.data.get("type") == "res"):
                self.on_message(message)

    def on_message(self, msg: Message) -> None:
        raise NotImplementedError

    def freeze(self) -> str:
        raise NotImplementedError

class Server(Component):
    def __init__(self, uni: str, any: str) -> None:
       super().__init__(uni, any)
       self.clock = StepClock()
       self.store = LocalStore(clock=self.clock)

    def on_message(self, msg: Message) -> None:
        # set clock
        self.clock.set_time(self.time)

        match msg.data.get("data"):
            case CreatePromiseReq(id, timeout, ikey, strict, headers, data, tags):
                promise = self.store.promises.create(
                    id=id,
                    timeout=timeout,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                    tags=tags,
                )
                self.send_msg(Unicast(msg.send), {"type": "res", "uuid": msg.data.get("uuid"), "data": CreatePromiseRes(promise)})

            case CreatePromiseWithTaskReq(id, timeout, pid, ttl, ikey, strict, headers, data, tags):
                promise, task = self.store.promises.create_with_task(
                    id=id,
                    timeout=timeout,
                    pid=pid,
                    ttl=ttl,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                    tags=tags,
                )
                self.send_msg(Unicast(msg.send), {"type": "res", "uuid": msg.data.get("uuid"), "data": CreatePromiseWithTaskRes(promise, task)})

            case ResolvePromiseReq(id, ikey, strict, headers, data):
                promise = self.store.promises.resolve(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                self.send_msg(Unicast(msg.send), {"type": "res", "uuid": msg.data.get("uuid"), "data": ResolvePromiseRes(promise)})

            case RejectPromiseReq(id, ikey, strict, headers, data):
                promise = self.store.promises.reject(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                self.send_msg(Unicast(msg.send), {"type": "res", "uuid": msg.data.get("uuid"), "data": RejectPromiseRes(promise)})

            case CancelPromiseReq(id, ikey, strict, headers, data):
                promise = self.store.promises.cancel(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                self.send_msg(Unicast(msg.send), {"type": "res", "uuid": msg.data.get("uuid"), "data": CancelPromiseRes(promise)})

            case CreateCallbackReq(id, promise_id, root_promise_id, timeout, recv):
                promise, callback = self.store.promises.callback(
                    id=id,
                    promise_id=promise_id,
                    root_promise_id=root_promise_id,
                    timeout=timeout,
                    recv=recv,
                )
                self.send_msg(Unicast(msg.send), {"type": "res", "uuid": msg.data.get("uuid"), "data": CreateCallbackRes(promise, callback)})

            case ClaimTaskReq(id, counter, pid, ttl):
                try:
                    root, leaf = self.store.tasks.claim(
                        id=id,
                        counter=counter,
                        pid=pid,
                        ttl=ttl,
                    )

                    result = Ok(ClaimTaskRes(root, leaf, Task(id, counter, self.store)))
                except ResonateStoreError as e:
                    result = Ko(e)

                self.send_msg(Unicast(msg.send), {"type": "res", "uuid": msg.data.get("uuid"), "data": result})

            case CompleteTaskReq(id, counter):
                try:
                    self.store.tasks.complete(id=id, counter=counter)
                    result = Ok(CompleteTaskRes())
                except ResonateStoreError as e:
                    result = Ko(e)

                self.send_msg(Unicast(msg.send), {"type": "res", "uuid": msg.data.get("uuid"), "data": result})

            case _:
                raise NotImplementedError

    def next(self) -> None:
        # set clock
        self.clock.set_time(self.time)

        for recv, mesg in self.store.step():
            parsed = urllib.parse.urlparse(recv)
            assert parsed.scheme == "poller", "scheme must be poller"

            match mesg["type"]:
                case "invoke" | "resume":
                    self.send_msg(Anycast(parsed.netloc, parsed.path[1:] or None), mesg)
                case "notify":
                    self.send_msg(Unicast(parsed.path[1:]), mesg)
                case _:
                    raise NotImplementedError

    def freeze(self) -> str:
        return "server"

class Worker(Component):
    def __init__(self, uni: str, any: str, registry: Registry) -> None:
        super().__init__(uni, any)
        self.registry = registry
        self.dependencies = Dependencies()
        self.scheduler = Scheduler(lambda id, info: Context(id, info, Options(), self.registry, self.dependencies, FuncCallingConvention), pid=self.uni)
        self.commands: list[Command] = []
        self.tasks: dict[str, Task] = {}

    def on_message(self, msg: Message) -> None:
        match msg.data:
            case Invoke(id, func, _, args, kwargs, opts):
                self.send_req(
                    Unicast("Server"),
                    CreatePromiseWithTaskReq(
                        id,
                        opts.timeout,
                        self.scheduler.pid,
                        sys.maxsize,
                        ikey=id,
                        data={"func": func, "args": args, "kwargs": kwargs},
                        tags={"resonate:invoke": self.uni, "resonate:scope": "global"},
                    ),
                    lambda res: self.__invoke(res.data.get("data")),
                )
            case {"type": "invoke", "task": {"id": id, "counter": counter}}:
                self.send_req(
                    Unicast("Server"),
                    ClaimTaskReq(id, counter, self.uni, sys.maxsize),
                    lambda res: self._invoke(res.data.get("data")),
                )
            case {"type": "resume", "task": {"id": id, "counter": counter}}:
                self.send_req(
                    Unicast("Server"),
                    ClaimTaskReq(id, counter, self.uni, sys.maxsize),
                    lambda res: self._resume(res.data.get("data")),
                )
            case _:
                raise NotImplementedError

    def next(self) -> None:
        if not self.commands:
            return

        cmd = self.commands.pop(0)

        match self.scheduler.step(cmd):
            case More(reqs):
                for req in reqs:
                    match req:
                        case Function(id, cid, func):
                            try:
                                self.defer(lambda id=id, cid=cid: self.commands.append(Return(id, cid, Ok(func()))))
                            except Exception as e:
                                self.defer(lambda id=id, cid=cid, e=e: self.commands.append(Return(id, cid, Ko(e))))

                        case Network(id, cid, req):
                            self.send_req(Unicast("Server"), req, lambda res, id=id, cid=cid: self.commands.append(Receive(id, cid, res.data.get("data"))))

            case Done(reqs):
                assert cmd.cid in self.tasks

                task = self.tasks[cmd.cid]
                count = 0

                def create_callback_callback(res: CreateCallbackRes) -> None:
                    nonlocal count

                    if res.promise.completed:
                        assert not res.callback
                        self.commands.append(Resume(res.promise.id, cmd.cid, res.promise))
                    else:
                        count += 1

                    if count == len(reqs):
                        self.send_req(
                            Unicast("Server"),
                            CompleteTaskReq(task.id, task.counter),
                            lambda _: None,
                        )

                for req in reqs:
                    match req:
                        case Network(id, cid, CreateCallbackReq() as req):
                            self.send_req(Unicast("Server"), req, lambda res: create_callback_callback(res.data.get("data")))
                        case _:
                            raise NotImplementedError

                if not reqs:
                    self.send_req(
                        Unicast("Server"),
                        CompleteTaskReq(task.id, task.counter),
                        lambda _: None,
                    )

    def __invoke(self, res: CreatePromiseWithTaskRes) -> None:
        if not res.task:
            return

        assert res.promise.id not in self.tasks
        self.tasks[res.promise.id] = res.task

        self.commands.append(Invoke(
            res.promise.id,
            res.promise.param.data["func"],
            self.registry.get(res.promise.param.data["func"])[0],
            res.promise.param.data["args"],
            res.promise.param.data["kwargs"],
        ))

    def _invoke(self, result: Result[ClaimTaskRes]) -> None:
        match result:
            case Ok(res):
                assert res.root.pending, "Root promise must be pending."
                assert isinstance(res.root.param.data, dict)
                assert "func" in res.root.param.data, "Root param must have func."
                assert "args" in res.root.param.data, "Root param must have args."
                assert "kwargs" in res.root.param.data, "Root param must have kwargs."

                # assert res.root.id not in self.tasks
                self.tasks[res.root.id] = res.task

                self.commands.append(Invoke(
                    res.root.id,
                    res.root.param.data["func"],
                    self.registry.get(res.root.param.data["func"])[0],
                    res.root.param.data["args"],
                    res.root.param.data["kwargs"],
                ))

            case Ko():
                # It's possible that is task is already claimed or completed, in that case just
                # ignore.
                pass

    def _resume(self, result: Result[ClaimTaskRes]) -> None:
        match result:
            case Ok(res):
                assert res.leaf, "Leaf must be set."
                assert res.leaf.completed, "Leaf promise must be completed."
                assert isinstance(res.leaf.param.data, dict)
                assert "func" in res.leaf.param.data, "Leaf param must have func."
                assert "args" in res.leaf.param.data, "Leaf param must have args."
                assert "kwargs" in res.leaf.param.data, "Leaf param must have kwargs."

                # assert res.root.id not in self.tasks
                self.tasks[res.root.id] = res.task

                self.commands.append(Resume(
                    id=res.leaf.id,
                    cid=res.root.id,
                    promise=res.leaf,
                    invoke=Invoke(
                        res.root.id,
                        res.root.param.data["func"],
                        self.registry.get(res.root.param.data["func"])[0],
                        res.root.param.data["args"],
                        res.root.param.data["kwargs"],
                    ),
                ))

            case Ko():
                # It's possible that is task is already claimed or completed, in that case just
                # ignore.
                pass

    def freeze(self) -> str:
        return "worker"
