from __future__ import annotations

import functools
import queue
import re
import sys
import threading
import uuid
from collections.abc import Callable, Generator
from concurrent.futures import Future
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import Any, Literal, Sequence

from resonate.context import Context
from resonate.graph import Graph, Node
from resonate.models.commands import CallbackEffect, Command, Invoke, Listen, Notify, Resume, Return, Return
from resonate.models.context import (
    AWT,
    LFC,
    LFI,
    LFX,
    RFC,
    RFI,
    RFX,
    Contextual,
    Yieldable,
)
from resonate.models.durable_promise import DurablePromise
from resonate.models.callback import Callback
from resonate.models.enqueueable import Enqueueable
from resonate.models.message import Mesg
from resonate.models.result import Ko, Ok, Result
from resonate.models.store import Store
from resonate.models.task import Task
from resonate.processor import Cb, Processor
from resonate.registry import Registry
from resonate.stores import LocalStore
from resonate.stores.local import LocalSender, Recv

# Scheduler


class Scheduler:
    def __init__(
        self,
        cq: queue.Queue[Command | tuple[Command, Future] | None] | None = None,
        pid: str | None = None,
        ctx: type[Contextual] = Context,
        processor: Processor | None = None,
        registry: Registry | None = None,
        store: Store | None = None,
    ) -> None:
        # command queue
        self.cq = cq or queue.Queue[Command | tuple[Command, Future] | None]()

        # pid
        self.pid = pid or uuid.uuid4().hex

        # ctx
        self.ctx = ctx

        # computations
        self.computations: dict[str, Computation] = {}

        # registry
        self.registry = registry or Registry()

        # processor
        self.processor = processor or Processor()

        # store
        self.store = store or LocalStore()

        # fake it until you make it
        if isinstance(self.store, LocalStore):
            self.store.add_sender(self.pid, LocalSender(self, self.registry, self.store))

        # scheduler thread
        self.thread = threading.Thread(target=self.loop, daemon=True)

    # def start(self) -> None:
    #     # connect to the store, must be done after instantiation to avoid race
    #     # condition with registered functions
    #     if isinstance(self.store, LocalStore):
    #         self.store.connect(self.pid, LocalSender(self, self.store, self.registry, self.pid))

    #     self.processor.start()

    #     if not self.thread.is_alive():
    #         self.thread.start()

    # def stop(self) -> None:
    #     if isinstance(self.store, LocalStore):
    #         self.store.disconnect(self.pid)

    #     self.processor.stop()

    #     if self.thread.is_alive():
    #         self.cq.put(None)
    #         self.thread.join()

    def enqueue(self, cmd: Command, future: Future | None = None) -> None:
        if future:
            self.cq.put((cmd, future))
        else:
            self.cq.put(cmd)

    def loop(self) -> None:
        while cqe := self.cq.get():
            match cqe:
                case (cmd, future):
                    self._step(cmd, future)
                case cmd:
                    self._step(cmd)

    def step(self) -> None:
        if cqe := self.cq.get_nowait():
            match cqe:
                case (cmd, future):
                    self._step(cmd, future)
                case cmd:
                    self._step(cmd)

            # TODO: should we do this here?
            try:
                while True:
                    self.processor.step()
            except queue.Empty:
                pass

    def _step(self, cmd: Command, future: Future | None = None) -> None:
        computation = self.computations.setdefault(cmd.cid, Computation(cmd.cid, self.pid, self.ctx, self.registry, self.store))

        computation.apply(cmd)

        for req in computation.eval():
            self.processor.enqueue(
                req.func,
                lambda r, cmd=cmd, req=req: self.enqueue(Return(cmd.id, cmd.cid, req.cont, r)),
            )
        # reqs = [
        #     *computation.apply(cmd),
        #     *computation.run_until_blocked(),
        # ]

        # for req in reqs:
        #     self.processor.enqueue(
        #         req.func,
        #         lambda r, cmd=cmd, cont=req.cont: self.enqueue(Return(cmd.id, cmd.cid, cont, r)),
        #     )

        # # subscribe to the computation
        # if future:
        #     computation.subscribe(future)

        # # resolve handles is computation has a result
        # if done:
        #     for f in computation.subscriptions:
        #         match computation.result():
        #             case Ok(r):
        #                 f.set_result(r)
        #             case Ko(e):
        #                 f.set_exception(e)

        #     # clear the subscriptions because futures cannot be set more than once
        #     computation.subscriptions.clear()

        # print the computation
        computation.print()


#####################################################################
## Here We Go
#####################################################################

type S = Enabled[E] | Blocked[E]

type E = Running[N] | Suspended[N] | Completed[N]

type N = Init | Pend | Func | Coro

type R = Literal["spawned", "waiting_p", "waiting_v"]

@dataclass
class Enabled[T: E]:
    value: T

    def __repr__(self) -> str:
        return f"Enabled::{self.value}"

@dataclass
class Blocked[T: E]:
    value: T

    def __repr__(self) -> str:
        return f"Blocked::{self.value}"

@dataclass
class Running[T: N]:
    value: T

    def __repr__(self) -> str:
        return f"Running::{self.value}"

@dataclass
class Suspended[T: N]:
    value: T

    def __repr__(self) -> str:
        return f"Suspended::{self.value}"

@dataclass
class Completed[T: N]:
    value: T
    result: Result

    def __repr__(self) -> str:
        return f"Completed::{self.value}"

@dataclass
class Init:
    pass

@dataclass
class Pend:
    next: tuple[Literal["top_level", "local"], Func | Coro] | tuple[Literal["remote"], Func]

@dataclass
class Func:
    id: str
    type: Literal["local", "remote"]
    name: str | None
    func: Callable[..., Any] | None
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    result: Result | None = None

    def __repr__(self) -> str:
        return f"Func({self.id}, {self.name})"

@dataclass
class Coro:
    id: str
    name: str | None
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    coro: Coroutine
    next: None | AWT | Result = None

    def __repr__(self) -> str:
        return f"Coro({self.id}, {self.name}, {self.next})"

@dataclass
class Request[T]:
    func: Callable[[], T]
    cont: Callable[[Result[T]], None]

    def __repr__(self) -> str:
        return "Request()"

class Computation:
    def __init__(self, id: str, pid: str, ctx: type[Contextual], registry: Registry, store: Store) -> None:
        self.id = id
        self.pid = pid
        self.ctx = ctx
        self.registry = registry
        self.store = store
        self.graph = Graph[S, R](Node(id, Enabled(Suspended(Init()))), "spawned")

        self.task: Task | None = None
        self.subscriptions: list[Future] = []

        self.invoked = False
        self.subscribed = False

    # def done(self) -> bool:
    #     return isinstance(self.graph.root.value, Done)

    # def runnable(self) -> bool:
    #     return self.graph.find(lambda n: n.value.runnable) is not None

    # def suspendable(self) -> bool:
    #     return self.graph.find(lambda n: n.value.suspended) is not None

    def subscribe(self, future: Future) -> None:
        self.subscriptions.append(future)

    # def result(self) -> Result | None:
    #     match self.graph.root.value:
    #         case Done(result):
    #             return result
    #         case _:
    #             return None

    def apply(self, cmd: Command) -> None:
        print(cmd)

        match cmd:
            case Invoke(id, name, func, args, kwargs):
                next = Coro(
                    id,
                    name,
                    func,
                    args,
                    kwargs,
                    Coroutine(id, self.id, func(self.ctx(id, self.registry), *args, **kwargs)),
                ) if isgeneratorfunction(func) else Func(
                    id,
                    "local",
                    name,
                    func,
                    args,
                    kwargs,
                )
                self.graph.root.transition(Enabled(Running(Pend(("top_level", next)))))

            case Resume(id, _, result, invoke):
                if node := self.graph.find(lambda n: n.id == id):
                    node.transition(Enabled(Completed(node.value.value.value, result)))
                    self.unblock(node, "waiting_p", Ok(AWT(id, self.id)))
                    self.unblock(node, "waiting_v", result)
                else:
                    self.apply(invoke)

            case Return(id, _, cont, result):
                cont(result)

    def eval(self) -> Sequence[Request]:
        reqs: Sequence[Request] = []

        for node in self.graph.traverse():
            reqs.extend(self._eval(node))

        return reqs

    def _eval(self, node: Node[S, R]) -> Sequence[Request]:
        match node.value:
            case Enabled(Running(Pend(("top_level", Func(id, name, args=args, kwargs=kwargs) | Coro(id, name, args=args, kwargs=kwargs) as next))) as exec):
                assert name is not None, "Name is required for top level invocation."
                node.transition(Blocked(exec))

                return [
                    Request(
                        functools.partial(
                            self.store.promises.create_with_task,
                            id=id,
                            ikey=id,
                            timeout=sys.maxsize,
                            data={"func": name, "args": args, "kwargs": kwargs},
                            tags={"resonate:invoke": self.pid, "resonate:scope": "global"},
                            pid=self.pid,
                            ttl=0,
                        ),
                        functools.partial(
                            self.transition,
                            node,
                            Running(next),
                        ),
                    ),
                ]

            case Enabled(Running(Pend(("local", Func(id) | Coro(id) as next))) as exec):
                node.transition(Blocked(exec))

                return [
                    Request(
                        functools.partial(
                            self.store.promises.create,
                            id=id,
                            ikey=id,
                            timeout=sys.maxsize,
                            tags={"resonate:scope": "local"},
                        ),
                        functools.partial(
                            self.transition,
                            node,
                            Running(next),
                        ),
                    ),
                ]

            case Enabled(Running(Pend(("remote", Func(id, name, args=args, kwargs=kwargs) as next))) as exec):
                assert name is not None, "Name is required for remote invocation."
                node.transition(Blocked(exec))

                return [
                    Request(
                        functools.partial(
                            self.store.promises.create,
                            id=id,
                            ikey=id,
                            timeout=sys.maxsize,
                            data={"func": name, "args": args, "kwargs": kwargs},
                            tags={"resonate:invoke": self.pid, "resonate:scope": "global"},
                        ),
                        functools.partial(
                            self.transition,
                            node,
                            Suspended(next),
                        ),
                    ),
                ]

            case Enabled(Running(Func(id, type, name, func, args, kwargs, result) as f) as exec):
                assert type == "local", "Only local functions are runnable."
                assert func is not None, "Func is required for local function."
                node.transition(Blocked(exec))

                match result:
                    case None:
                        return [
                            Request(
                                lambda: func(*args, **kwargs),
                                lambda r: node.transition(Enabled(Running(Func(id, type, name, func, args, kwargs, r)))),
                            ),
                        ]
                    case Ok(v):
                        return [
                            Request(
                                functools.partial(
                                    self.store.promises.resolve,
                                    id=id,
                                    ikey=id,
                                    data=v
                                ),
                                functools.partial(
                                    self.transition,
                                    node,
                                    Running(f),
                                ),
                            ),
                        ]
                    case Ko(v):
                        # TODO: retry if there is retry budget
                        return [
                            Request(
                                functools.partial(
                                    self.store.promises.reject,
                                    id=id,
                                    ikey=id,
                                    data=v
                                ),
                                functools.partial(
                                    self.transition,
                                    node,
                                    Running(f),
                                ),
                            ),
                        ]

            case Enabled(Running(Coro(coro=coro, next=next) as c)):
                cmd = coro.send(next)
                child = self.graph.find(lambda n: n.id == cmd.id) or Node(cmd.id, Enabled(Running(Init())))

                print(cmd)

                match cmd, child.value.value:
                    case LFI(id, func, args, kwargs) | LFC(id, func, args, kwargs), Running(Init()):
                        next = Coro(
                            id,
                            None,
                            func,
                            args,
                            kwargs,
                            Coroutine(id, self.id, func(self.ctx(id, self.registry), *args, **kwargs)),
                        ) if isgeneratorfunction(func) else Func(
                            id,
                            "local",
                            None,
                            func,
                            args,
                            kwargs,
                        )

                        child.transition(Enabled(Running(Pend(("local", next)))))

                        node.add_edge("spawned", child)
                        node.add_edge("waiting_p" if isinstance(cmd, LFI) else "waiting_v", child)
                        node.transition(Blocked(Running(c)) if isinstance(cmd, LFI) else Enabled(Suspended(c)))

                        return []

                    case RFI(id, func, args, kwargs) | RFC(id, func, args, kwargs), Running(Init()):
                        next = Func(
                            id,
                            "remote",
                            func,
                            None,
                            args,
                            kwargs,
                        )

                        child.transition(Enabled(Running(Pend(("remote", next)))))

                        node.add_edge("spawned", child)
                        node.add_edge("waiting_p" if isinstance(cmd, RFI) else "waiting_v", child)
                        node.transition(Blocked(Running(c)) if isinstance(cmd, RFI) else Enabled(Suspended(c)))

                        return []

                    case LFI() | LFC() | RFI() | RFC(), Running(Pend()) | Suspended(Pend()):
                        node.add_edge("spawned", child)
                        node.add_edge("waiting_p" if isinstance(cmd, LFI) else "waiting_v", child)
                        node.transition(Enabled(Suspended(c)))
                        return []

                    case LFC() | RFC(), Completed(result=result):
                        node.add_edge("spawned", child)
                        c.next = result
                        return []

                    case LFI(id) | RFI(id), _:
                        node.add_edge("spawned", child)
                        c.next = AWT(id, self.id)
                        return []

                    case LFC() | RFC(), _:
                        node.add_edge("spawned", child)
                        node.add_edge("waiting_v", child)
                        node.transition(Enabled(Suspended(c)))
                        return []

                    case AWT(id, cid), Completed(result=result):
                        assert cid == self.id, "Await id must match computation id."
                        c.next = result
                        return []

                    case AWT(id, cid), Running(Func(type="remote")) | Suspended(Func(type="remote")):
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge("waiting_v", child)
                        node.transition(Blocked(Suspended(c)))
                        return [
                            Request(
                                functools.partial(
                                    self.store.promises.callback,
                                    id=f"{self.id}.{id}",
                                    promise_id=id,
                                    root_promise_id=self.id,
                                    timeout=sys.maxsize,
                                    recv=self.pid,
                                ),
                                functools.partial(
                                    self.transition,
                                    node,
                                    Suspended(c),
                                ),
                            )
                        ]

                    case AWT(id, cid), _:
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge("waiting_v", child)
                        node.transition(Enabled(Suspended(c)))
                        return []

                    case TRM(id, result), _:
                        assert id == node.id, "Terminate id must match node id."

                        match result:
                            case Ok(v):
                                node.transition(Blocked(Running(c)))

                                return [
                                    Request(
                                        functools.partial(
                                            self.store.promises.resolve,
                                            id=id,
                                            ikey=id,
                                            data=v
                                        ),
                                        functools.partial(
                                            self.transition,
                                            node,
                                            Running(c),
                                        ),
                                    ),
                                ]
                            case Ko(v):
                                node.transition(Blocked(Running(c)))

                                # TODO: retry if there is retry budget
                                return [
                                    Request(
                                        functools.partial(
                                            self.store.promises.reject,
                                            id=id,
                                            ikey=id,
                                            data=v
                                        ),
                                        functools.partial(
                                            self.transition,
                                            node,
                                            Running(c),
                                        ),
                                    ),
                                ]

                    case _:
                        raise NotImplementedError

            # case _:
            #     print("*************")
            #     print(node.value)
            #     print("*************")
            #     assert False, "nah dawg"

        return []

    def print(self) -> None:
        for node, level in self.graph.traverse_with_level():
            print(f"{'  ' * level}{node}")

    def transition(
        self,
        node: Node[S, R],
        next: E,
        result: Result[DurablePromise] | Result[tuple[DurablePromise, Callback | Task | None]]
    ) -> None:
        match result:
            case Ok((promise, _)) | Ok(promise):
                match promise.pending:
                    case True:
                        self.unblock(node, "waiting_p", AWT(node.id, self.id))
                        node.transition(Enabled(next))
                    case False:
                        self.unblock(node, "waiting_p", AWT(node.id, self.id))
                        self.unblock(node, "waiting_v", promise.result)
                        node.transition(Enabled(Completed(next.value, promise.result)))
            case Ko():
                raise NotImplementedError

    def unblock(self, node: Node[S, R], edge: R, next: AWT | Result) -> None:
        for blocked in self.graph.filter(lambda n: n.has_edge(edge, node)):
            assert isinstance(blocked.value.value.value, Coro), "Blocked must be coro."
            blocked.rmv_edge(edge, node)

            # TODO: what the heck is this?
            blocked.value.value.value.next = next
            blocked.transition(Enabled(Running(blocked.value.value.value)))

# Coroutine

@dataclass
class TRM:
    id: str
    result: Result

class Coroutine:
    def __init__(self, id: str, cid: str, gen: Generator[Yieldable, Any, Any]) -> None:
        self.id = id
        self.cid = cid
        self.gen = gen

        self.next: type[None | AWT] | tuple[type[Result], ...] = type(None)
        self.done = False
        self.unyielded: list[AWT | TRM] = []

    def __repr__(self) -> str:
        return f"Coroutine(done={self.done})"

    def send(self, value: None | AWT | Result) -> Yieldable | TRM:
        assert isinstance(value, self.next), "Promise must follow LFI/RFI. Value must follow LFC/RFC/AWT."

        if self.done:
            match self.unyielded:
                case [head, *tail]:
                    self.unyielded = tail
                    return head
                case _:
                    raise StopIteration

        try:
            match value:
                case Ok(v):
                    yielded = self.gen.send(v)
                case Ko(e):
                    yielded = self.gen.throw(e)
                case _:
                    yielded = self.gen.send(value)

            match yielded:
                case LFI(id) | RFI(id, mode="attached"):
                    self.next = AWT
                    self.unyielded.append(AWT(id, self.cid))
                case LFC() | RFC():
                    self.next = (Ok, Ko)
                case AWT(id):
                    self.next = (Ok, Ko)
                    self.unyielded = [y for y in self.unyielded if y.id != id]

        except StopIteration as e:
            self.done = True
            self.unyielded.append(TRM(self.id, Ok(e.value)))
            return self.unyielded.pop(0)
        except Exception as e:
            self.done = True
            self.unyielded.append(TRM(self.id, Ko(e)))
            return self.unyielded.pop(0)
        else:
            return yielded
