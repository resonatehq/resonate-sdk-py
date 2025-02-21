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
from typing import Any, Literal, Sequence, TypeGuard

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
        cq: queue.Queue[Command | tuple[Command, tuple[Future, Future]] | None] | None = None,
        pid: str | None = None,
        ctx: type[Contextual] = Context,
        processor: Processor | None = None,
        registry: Registry | None = None,
        store: Store | None = None,
    ) -> None:
        # command queue
        self.cq = cq or queue.Queue[Command | tuple[Command, tuple[Future, Future]] | None]()

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

    def start(self) -> None:
        self.processor.start()

        if not self.thread.is_alive():
            self.thread.start()

    def stop(self) -> None:
        self.processor.stop()

        if self.thread.is_alive():
            self.cq.put(None)
            self.thread.join()

    def enqueue(self, cmd: Command, futures: tuple[Future, Future] | None = None) -> None:
        if futures:
            self.cq.put((cmd, futures))
        else:
            self.cq.put(cmd)

    def loop(self) -> None:
        while cqe := self.cq.get():
            match cqe:
                case (cmd, futures):
                    self._step(cmd, futures)
                case cmd:
                    self._step(cmd)

    def step(self) -> None:
        if cqe := self.cq.get_nowait():
            match cqe:
                case (cmd, futures):
                    self._step(cmd, futures)
                case cmd:
                    self._step(cmd)

            # TODO: should we do this here?
            try:
                while True:
                    self.processor.step()
            except queue.Empty:
                pass

    def _step(self, cmd: Command, futures: tuple[Future, Future] | None = None) -> None:
        computation = self.computations.setdefault(cmd.cid, Computation(cmd.cid, self.pid, self.ctx, self.registry, self.store))

        # subscribe
        if futures:
            computation.subscribe(*futures)

        # apply
        computation.apply(cmd)

        # eval
        for req in computation.eval():
            self.processor.enqueue(
                req.func,
                lambda r, cmd=cmd, req=req: self.enqueue(Return(cmd.id, cmd.cid, lambda: req.cont(r))),
            )

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

    @property
    def running(self) -> bool:
        return self.value.running

    @property
    def suspended(self) -> bool:
        return self.value.suspended

@dataclass
class Blocked[T: E]:
    value: T

    def __repr__(self) -> str:
        return f"Blocked::{self.value}"

    @property
    def running(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return False

@dataclass
class Running[T: N]:
    value: T

    def __repr__(self) -> str:
        return f"Running::{self.value}"

    @property
    def running(self) -> bool:
        return True

    @property
    def suspended(self) -> bool:
        return False

@dataclass
class Suspended[T: N]:
    value: T

    def __repr__(self) -> str:
        return f"Suspended::{self.value}"

    @property
    def running(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return True

@dataclass
class Completed[T: N]:
    value: T
    result: Result

    def __repr__(self) -> str:
        return f"Completed::{self.value}"

    @property
    def running(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return True

@dataclass
class Init:
    pass

@dataclass
class Pend:
    next: Func | Coro

@dataclass
class Func:
    id: str
    name: str | None
    func: Callable[..., Any] | None
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    type: Literal["local", "remote"]
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
        self.callbacks: dict[str, Callback] = {}

        self._fp: list[Future] = []
        self._fv: list[Future] = []

    def runnable(self) -> bool:
        return any(n.value.running for n in self.graph.traverse())

    def suspendable(self) -> bool:
        return all(n.value.suspended for n in self.graph.traverse())

    def subscribe(self, fp: Future, fv: Future) -> None:
        self._fp.append(fp)
        self._fv.append(fv)

    @property
    def promised(self) -> bool:
        return isinstance(self.graph.root.value.value.value, (Func, Coro))

    @property
    def completed(self) -> bool:
        return isinstance(self.graph.root.value.value, Completed)

    def value(self) -> Result:
        if not isinstance(self.graph.root.value.value, Completed):
            raise RuntimeError("Computation is not completed")

        return self.graph.root.value.value.result

    def apply(self, cmd: Command) -> None:
        print(cmd)

        match cmd, self.graph.root.value.value.value:
            case Invoke(id, name, func, args, kwargs, task), Init():
                next = Coro(
                    id,
                    name,
                    func,
                    args,
                    kwargs,
                    Coroutine(id, self.id, func(self.ctx(id, self.registry), *args, **kwargs)),
                ) if isgeneratorfunction(func) else Func(
                    id,
                    name,
                    func,
                    args,
                    kwargs,
                    "local",
                )

                if task:
                    assert not self.task, "Task must not be set."
                    self.task = task
                    self.graph.root.transition(Enabled(Running(next)))
                else:
                    self.graph.root.transition(Enabled(Running(Pend(next))))

            case Resume(invoke=invoke), Init():
                self.apply(invoke)

            case Resume(id, result=result, task=task), _:
                if node := self.graph.find(lambda n: n.id == id):
                    node.transition(Enabled(Completed(node.value.value.value, result)))
                    self._unblock(node, "waiting_p", AWT(id, self.id))
                    self._unblock(node, "waiting_v", result)
                    self.task = task
                else:
                    # What should we do here?
                    raise NotImplementedError

            case Return(cont=cont), _:
                cont()

    def eval(self) -> Sequence[Request]:
        reqs: Sequence[Request] = []

        while self.runnable():
            for node in self.graph.traverse():
                reqs.extend(self._eval(node))

        if self.suspendable():
            print("!!!!!!! We are suspending", self.id)
            assert self.task, "Task must be set."
            self.task.complete()

        if self.promised:
            for future in self._fp:
                future.set_result(True)
            self._fp.clear()

        if self.completed:
            for future in self._fv:
                future.set_result(self.value())
            self._fv.clear()

        return reqs

    def _eval(self, node: Node[S, R]) -> Sequence[Request]:
        match node.value:
            case Enabled(Running(Pend(Func(id, name, args=args, kwargs=kwargs, type="local") | Coro(id, name, args=args, kwargs=kwargs) as next)) as exec):
                assert id == node.id, "Id must match node id."
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
                            self._transition,
                            node,
                            Running(next),
                        ),
                    ),
                ] if node == self.graph.root else [
                    Request(
                        functools.partial(
                            self.store.promises.create,
                            id=id,
                            ikey=id,
                            timeout=sys.maxsize,
                            tags={"resonate:scope": "local"},
                        ),
                        functools.partial(
                            self._transition,
                            node,
                            Running(next),
                        ),
                    ),
                ]

            case Enabled(Running(Pend(Func(id, name, args=args, kwargs=kwargs, type="remote") as next)) as exec):
                assert id == node.id, "Id must match node id."
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
                            self._transition,
                            node,
                            Suspended(next),
                        ),
                    ),
                ]

            case Enabled(Running(Func(id, name, func, args, kwargs, type, result) as f) as exec):
                assert id == node.id, "Id must match node id."
                assert type == "local", "Only local functions are runnable."
                assert func is not None, "Func is required for local function."
                node.transition(Blocked(exec))

                match result:
                    case None:
                        return [
                            Request(
                                lambda: func(*args, **kwargs),
                                lambda r: node.transition(Enabled(Running(Func(id, name, func, args, kwargs, type, r)))),
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
                                    self._transition,
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
                                    self._transition,
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
                    case LFI(id, func, args, kwargs), Running(Init()):
                        next = Coro(
                            id,
                            None,
                            func,
                            args,
                            kwargs,
                            Coroutine(id, self.id, func(self.ctx(id, self.registry), *args, **kwargs)),
                        ) if isgeneratorfunction(func) else Func(
                            id,
                            None,
                            func,
                            args,
                            kwargs,
                            "local",
                        )

                        child.transition(Enabled(Running(Pend(next))))

                        node.add_edge("spawned", child)
                        node.add_edge("waiting_p", child)
                        node.transition(Enabled(Suspended(c)))

                        return []

                    case RFI(id, func, args, kwargs), Running(Init()):
                        next = Func(
                            id,
                            func,
                            None,
                            args,
                            kwargs,
                            "remote",
                        )

                        child.transition(Enabled(Running(Pend(next))))

                        node.add_edge("spawned", child)
                        node.add_edge("waiting_p", child)
                        node.transition(Enabled(Suspended(c)))

                        return []

                    case LFI() | RFI(), Running(Pend()) | Suspended(Pend()):
                        node.add_edge("spawned", child)
                        node.add_edge("waiting_p", child)
                        node.transition(Enabled(Suspended(c)))
                        return []

                    case LFI(id) | RFI(id), _:
                        node.add_edge("spawned", child)
                        c.next = AWT(id, self.id)
                        return []

                    case AWT(id, cid), Completed(result=result):
                        assert cid == self.id, "Await id must match computation id."
                        c.next = result
                        return []

                    case AWT(id, cid), Suspended(Func(type="remote") as f):
                        assert cid == self.id, "Await id must match computation id."

                        callback_id = f"{self.id}:{id}"
                        node.add_edge("waiting_v", child)
                        node.transition(Enabled(Suspended(c)))

                        # nothing to do if there is already a callback
                        if callback_id in self.callbacks:
                            return []

                        child.transition(Blocked(Suspended(f)))

                        return [
                            Request(
                                functools.partial(
                                    self.store.promises.callback,
                                    id=callback_id,
                                    promise_id=id,
                                    root_promise_id=self.id,
                                    timeout=sys.maxsize,
                                    recv=self.pid,
                                ),
                                functools.partial(
                                    self._transition,
                                    child,
                                    Suspended(f),
                                ),
                            )
                        ]

                    case AWT(id, cid), _:
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge("waiting_v", child)
                        node.transition(Enabled(Suspended(c)))
                        return []

                    case TRM(id, result), _:
                        assert id == node.id, "Id must match node id."

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
                                            self._transition,
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
                                            self._transition,
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
        print("Computation", self.id)
        print("Task:")
        print("\t", self.task)

        print("Callbacks:")
        for callback in self.callbacks.values():
            print("\t", callback)

        print("Graph:")
        for node, level in self.graph.traverse_with_level():
            print(f"{'  ' * level}{node}")

    def _transition(
        self,
        node: Node[S, R],
        next: E,
        result: Result[DurablePromise] | Result[tuple[DurablePromise, Task | Callback | None]]
    ) -> None:
        match result:
            case Ok(v):

                # Extract promise
                match v:
                    case (promise, _) | promise:
                        assert node.id == promise.id, "Node id must match promise id."

                        match promise.pending:
                            case True:
                                self._unblock(node, "waiting_p", AWT(node.id, self.id))
                                node.transition(Enabled(next))
                            case False:
                                self._unblock(node, "waiting_p", AWT(node.id, self.id))
                                self._unblock(node, "waiting_v", promise.result)
                                node.transition(Enabled(Completed(next.value, promise.result)))

                # Extract task/callback
                match v:
                    case (_, Task() as task):
                        assert node == self.graph.root, "A task is only applicable on the root node."
                        assert not self.task, "Task must not be set."
                        self.task = task
                    case (_, Callback() as callback):
                        self.callbacks[callback.id] = callback

            case Ko():
                raise NotImplementedError

    def _unblock(self, node: Node[S, R], edge: R, next: AWT | Result) -> None:
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

        self.done = False
        self.skip = False
        self.next: type[None | AWT] | tuple[type[Result], ...] = type(None)
        self.unyielded: list[AWT | TRM] = []

    def __repr__(self) -> str:
        return f"Coroutine(done={self.done})"

    def send(self, value: None | AWT | Result) -> LFI | RFI | AWT | TRM:
        assert isinstance(value, self.next), "Promise must follow LFI/RFI. Value must follow AWT."

        if self.done:
            match self.unyielded:
                case [head, *tail]:
                    self.unyielded = tail
                    return head
                case _:
                    raise StopIteration

        try:
            match value, self.skip:
                case Ok(v), False:
                    yielded = self.gen.send(v)
                case Ko(e), False:
                    yielded = self.gen.throw(e)
                case awt, True:
                    assert isinstance(awt, AWT), "Skipped value must be an AWT."
                    self.skip = False
                    yielded = awt
                case _:
                    yielded = self.gen.send(value)

            match yielded:
                case LFI(id) | RFI(id, mode="attached"):
                    self.next = AWT
                    self.unyielded.append(AWT(id, self.cid))
                case LFC(id, func, args, kwargs, opts, name, task):
                    self.next = AWT
                    self.skip = True
                    yielded = LFI(id, func, args, kwargs, opts, name, task)
                case RFC(id, func, args, kwargs, opts):
                    self.next = AWT
                    self.skip = True
                    yielded = RFI(id, func, args, kwargs, opts)
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
