from __future__ import annotations

import functools
import queue
import sys
import threading
import uuid
from collections.abc import Callable, Generator, Sequence
from concurrent.futures import Future
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import Any, Final, Literal, Union

from resonate.context import Context
from resonate.dependencies import Dependencies
from resonate.graph import Graph, Node
from resonate.models.callback import Callback
from resonate.models.commands import Command, Function, Invoke, Network, Receive, Request, Resume, Return
from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
    CreateCallbackReq,
    CreateCallbackRes,
    CreatePromiseReq,
    CreatePromiseRes,
    CreatePromiseWithTaskReq,
    CreatePromiseWithTaskRes,
    RejectPromiseReq,
    RejectPromiseRes,
    ResolvePromiseReq,
    ResolvePromiseRes,
)
from resonate.models.context import (
    AWT,
    LFC,
    LFI,
    RFC,
    RFI,
    Contextual,
    Yieldable,
)
from resonate.models.durable_promise import DurablePromise
from resonate.models.result import Ko, Ok, Result
from resonate.models.store import Store
from resonate.models.task import Task
from resonate.registry import Registry
from resonate.stores import LocalStore

# Scheduler


class Scheduler:
    def __init__(
        self,
        cq: queue.Queue[Command | tuple[Command, tuple[Future, Future]] | None] | None = None,
        pid: str | None = None,
        ctx: type[Contextual] = Context,
        registry: Registry | None = None,
        deps: Dependencies | None = None,
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

        # global_dependencies
        self.deps = deps or Dependencies()

        # store
        # self.store = store or LocalStore()

        # scheduler thread
        self.thread = threading.Thread(target=self.loop, daemon=True)

    def start(self) -> None:
        if not self.thread.is_alive():
            self.thread.start()

    def stop(self) -> None:
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

    def step(self, time: int) -> Sequence[Request]:
        reqs = []

        try:
            if cqe := self.cq.get_nowait():
                match cqe:
                    case (cmd, futures):
                        reqs = self._step(cmd, futures)
                    case cmd:
                        reqs = self._step(cmd)
        except queue.Empty:
            pass

        return reqs

    def _step(self, cmd: Command, futures: tuple[Future, Future] | None = None) -> Sequence[Request]:
        computation = self.computations.setdefault(cmd.cid, Computation(cmd.cid, self.pid, self.ctx, self.registry, self.deps))

        # subscribe
        if futures:
            computation.subscribe(*futures)

        # apply
        computation.apply(cmd)

        # eval
        reqs = computation.eval()

        # print the computation
        computation.print()

        return reqs


#####################################################################
## Here We Go
#####################################################################

type State = Union[
    Enabled[Running[Init | Func | Coro]],
    Enabled[Suspended[Init | Func | Coro]],
    Enabled[Completed[Func | Coro]],
    Blocked[Running[Init | Func | Coro]],
    Blocked[Suspended[Func]],
]

@dataclass
class Enabled[T: Running[Init | Func | Coro] | Suspended[Init | Func | Coro] | Completed[Func | Coro]]:
    value: Final[T]

    def __repr__(self) -> str:
        return f"Enabled::{self.value}"

    def bind[U: Running | Suspended | Completed](self, func: Callable[[T], U]) -> Enabled[U]:
        return Enabled(func(self.value))

    @property
    def running(self) -> bool:
        return self.value.running

    @property
    def suspended(self) -> bool:
        return self.value.suspended

    @property
    def exec(self) -> T:
        return self.value

    @property
    def func(self) -> Init | Func | Coro:
        return self.value.value

@dataclass
class Blocked[T: Running[Init | Func | Coro] | Suspended[Func]]:
    value: Final[T]

    def __repr__(self) -> str:
        return f"Blocked::{self.value}"

    def bind[U: Running | Suspended](self, func: Callable[[T], U]) -> Blocked[U]:
        return Blocked(func(self.value))

    @property
    def running(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return False

    @property
    def exec(self) -> T:
        return self.value

    @property
    def func(self) -> Init | Func | Coro:
        return self.value.value

@dataclass
class Running[T: Init | Func | Coro]:
    value: Final[T]

    def __repr__(self) -> str:
        return f"Running::{self.value}"

    def bind[U: Init | Func | Coro](self, func: Callable[[T], U]) -> Running[U]:
        return Running(func(self.value))

    @property
    def running(self) -> bool:
        return True

    @property
    def suspended(self) -> bool:
        return False

    @property
    def exec(self) -> Running[T]:
        return self

    @property
    def func(self) -> T:
        return self.value

@dataclass
class Suspended[T: Init | Func | Coro]:
    value: Final[T]

    def __repr__(self) -> str:
        return f"Suspended::{self.value}"

    def bind[U: Init | Func | Coro](self, func: Callable[[T], U]) -> Suspended[U]:
        return Suspended(func(self.value))

    @property
    def running(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return True

    @property
    def exec(self) -> Suspended[T]:
        return self

    @property
    def func(self) -> T:
        return self.value

@dataclass
class Completed[T: Func | Coro]:
    value: Final[T]

    def __repr__(self) -> str:
        return f"Completed::{self.value}"

    def bind[U: Func | Coro](self, func: Callable[[T], U]) -> Completed[U]:
        return Completed(func(self.value))

    @property
    def running(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return True

    @property
    def exec(self) -> Completed[T]:
        return self

    @property
    def func(self) -> T:
        return self.value

@dataclass
class Init:
    next: Func | Coro | None = None
    suspends: list[Node[State]] = field(default_factory=list)

    @property
    def id(self) -> str:
        return self.next.id if self.next else ""

    def map(self, *, suspends: Node[State] | None = None) -> Init:
        if suspends:
            self.suspends.append(suspends)
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Init(id={self.id}, next={self.next}, suspends=[{s}])"

@dataclass
class Func:
    id: str
    type: Literal["local", "remote"]
    name: str | None
    func: Callable[..., Any] | None
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: dict[str, Any]

    promise: DurablePromise | None = None
    suspends: list[Node[State]] = field(default_factory=list)
    result: Result | None = None

    def map(self, *, promise: DurablePromise | None = None, suspends: Node[State] | None = None, result: Result | None = None) -> Func:
        if promise:
            self.promise = promise
        if suspends:
            self.suspends.append(suspends)
        if result:
            self.result = result
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Func(id={self.id}, type={self.type}, suspends=[{s}], result={self.result})"

@dataclass
class Coro:
    id: str
    type: Literal["local"]
    name: str | None
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    opts: dict[str, Any]

    coro: Coroutine
    next: None | AWT | Result = None

    promise: DurablePromise | None = None
    suspends: list[Node[State]] = field(default_factory=list)
    result: Result | None = None

    def map(self, *, coro: Coroutine | None = None, next: None | AWT | Result = None, promise: DurablePromise | None = None, suspends: Node[State] | None = None, result: Result | None = None) -> Coro:
        if coro:
            self.coro = coro
        if next:
            self.next = next
        if promise:
            self.promise = promise
        if suspends:
            self.suspends.append(suspends)
        if result:
            self.result = result
        return self

    def __repr__(self) -> str:
        s = ", ".join([s.value.func.id for s in self.suspends])
        return f"Coro(id={self.id}, next={self.next}, suspends=[{s}], result={self.result})"

class Computation:
    def __init__(self, id: str, pid: str, ctx: type[Contextual], registry: Registry, deps: Dependencies) -> None:
        self.id = id
        self.pid = pid
        self.ctx = ctx
        self.registry = registry
        self.deps = deps
        self.graph = Graph[State](id, Enabled(Suspended(Init())))

        self.task: Task | None = None
        self.callbacks: dict[str, Callback] = {}
        self.subscriptions: tuple[list[Future], list[Future]] = ([], [])

    def runnable(self) -> bool:
        return any(n.value.running for n in self.graph.traverse())

    def suspendable(self) -> bool:
        return all(n.value.suspended for n in self.graph.traverse())

    def subscribe(self, fp: Future, fv: Future) -> None:
        self.subscriptions[0].append(fp)
        self.subscriptions[1].append(fv)

    def promise(self) -> DurablePromise | None:
        match self.graph.root.value.func:
            case Func(promise=promise) | Coro(promise=promise):
                assert promise, "Promise must be set."
                return promise
            case _:
                return None

    def result(self) -> Result | None:
        match self.graph.root.value.exec:
            case Completed(Func(result=result) | Coro(result=result)):
                assert result, "Result must be set."
                return result
            case _:
                return None

    def apply(self, cmd: Command) -> None:
        print(cmd)

        match cmd, self.graph.root.value.func:
            case Invoke(id, name, func, args, kwargs, promise_and_task, opts), Init():
                next: Coro | Func
                if func is None:
                    next = Func(id, "remote", name, func, args, kwargs, opts)
                elif isgeneratorfunction(func):
                    next = Coro(id, "local", name, func, args, kwargs, opts, Coroutine(id, self.id, func(self.ctx(id, self.registry, self.deps), *args, **kwargs)))
                else:
                    next = Func(id, "local", name, func, args, kwargs, opts)

                if promise_and_task:
                    promise, task = promise_and_task
                    assert not self.task, "Task must not be set."
                    self.task = task
                    self.graph.root.transition(Enabled(Running(next.map(promise=promise))))
                else:
                    self.graph.root.transition(Enabled(Running(Init(next))))

            case Resume(invoke=invoke), Init():
                self.apply(invoke)

            case Resume(id, promise=promise, task=task), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply(node, (promise, task, None))

            case Return(id, res=res) | Receive(id, res=res), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply(node, res)

            case _:
                raise NotImplementedError

    def _apply(self, node: Node[State], data: Result | tuple[DurablePromise, Task | None, Callback | None]) -> None:
        match node.value, data:
            case Blocked(Running(Func(type="local") as f)), Ok() | Ko() as result:
                node.transition(Enabled(Running(f.map(result=result))))

            case Blocked(Running(Init(Func(type="local") | Coro(type="local") as next, suspends))), (promise, task, None) if promise.pending:
                assert not next.suspends, "Next suspends must be initially empty."
                node.transition(Enabled(Running(next.map(promise=promise))))

                if task:
                    assert node is self.graph.root, "Node must be the root node."
                    assert not self.task, "Task must not be set."
                    self.task = task

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id, self.id))

            case Blocked(Running(Init(Func(type="remote") as next, suspends))), (promise, None, None) if promise.pending:
                assert not next.suspends, "Next suspends must be initially empty."
                node.transition(Enabled(Suspended(next.map(promise=promise))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id, self.id))

            case Blocked(Running(Init(Func(type="local" | "remote") | Coro(type="local") as next, suspends))), (promise, None, None) if promise.completed:
                assert not next.suspends, "Next suspends must be initially empty."
                node.transition(Enabled(Completed(next.map(promise=promise, result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case Blocked(Running(Func(type="local", suspends=suspends) | Coro(type="local", suspends=suspends) as f)), (promise, None, None):
                assert promise.completed, "Promise must be completed."
                node.transition(Enabled(Completed(f.map(promise=promise, result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case Blocked(Suspended(Func(type="remote", suspends=suspends) as f)), (promise, None, None):
                assert promise.completed, "Promise must be completed."
                node.transition(Enabled(Suspended(f.map(promise=promise, result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case Blocked(Suspended(Func(id, type="remote") as f)), (promise, None, callback):
                assert promise.pending, "Promise must be pending."
                assert callback, "Callback must be set."

                self.callbacks[id] = callback
                node.transition(Enabled(Suspended(f)))

            case Enabled(Suspended(Func(type="remote", suspends=suspends) as f)), (promise, task, None):
                assert promise.completed, "Promise must be completed."
                assert task, "Task must be set."
                assert not task.completed, "Task must not be completed."

                self.task = task
                node.transition(Enabled(Completed(f.map(promise=promise, result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case Enabled(Completed()), _:
                pass

            case _:
                raise NotImplementedError

    def eval(self) -> Sequence[Request]:
        reqs: Sequence[Request] = []

        while self.runnable():
            for node in self.graph.traverse():
                reqs.extend(self._eval(node))

        if self.suspendable():
            assert self.task, "Task must be set."
            if not self.task.completed:
                self.task.complete()

        if promise := self.promise():
            for future in self.subscriptions[0]:
                future.set_result(promise)
            self.subscriptions[0].clear()

        if result := self.result():
            for future in self.subscriptions[1]:
                match result:
                    case Ok(v):
                        future.set_result(v)
                    case Ko(e):
                        future.set_exception(e)
            self.subscriptions[1].clear()

        return reqs

    def _eval(self, node: Node[State]) -> Sequence[Request]:
        match node.value:
            case Enabled(Running(Init(Func(id, "local", name, _, args, kwargs) | Coro(id, "local", name, _, args, kwargs))) as exec):
                assert id == node.id, "Id must match node id."
                node.transition(Blocked(exec))

                if node == self.graph.root:
                    req = CreatePromiseWithTaskReq(
                        id=id,
                        ikey=id,
                        timeout=sys.maxsize,
                        data={"func": name, "args": args, "kwargs": kwargs},
                        tags={"resonate:invoke": self.pid, "resonate:scope": "global"},
                        pid=self.pid,
                        ttl=sys.maxsize,
                    )
                else:
                    req = CreatePromiseReq(
                        id=id,
                        ikey=id,
                        timeout=sys.maxsize,
                        tags={"resonate:scope": "local"},
                    )

                return [
                    Network(id, self.id, req),
                ]

            case Enabled(Running(Init(Func(id, "remote", name, _, args, kwargs, opts))) as exec):
                assert id == node.id, "Id must match node id."
                assert name is not None, "Name is required for remote invocation."
                node.transition(Blocked(exec))

                return [
                    Network(id, self.id, CreatePromiseReq(
                        id=id,
                        ikey=id,
                        timeout=sys.maxsize,
                        data={"func": name, "args": args, "kwargs": kwargs},
                        tags={"resonate:invoke": opts.get("target", self.pid), "resonate:scope": "global"},
                    )),
                ]

            case Enabled(Running(Func(id, type, _, func, args, kwargs, result=result) as f)):
                assert id == node.id, "Id must match node id."
                assert type == "local", "Only local functions are runnable."
                assert func is not None, "Func is required for local function."
                node.transition(Blocked(Running(f)))

                match result:
                    case None:
                        return [
                            Function(id, self.id, lambda: func(*args, **kwargs)),
                        ]
                    case Ok(v):
                        return [
                            Network(id, self.id, ResolvePromiseReq(
                                id=id,
                                ikey=id,
                                data=v
                            )),
                        ]
                    case Ko(v):
                        # TODO: retry if there is retry budget
                        return [
                            Network(id, self.id, RejectPromiseReq(
                                id=id,
                                ikey=id,
                                data=v
                            )),
                        ]

            case Enabled(Running(Coro(coro=coro, next=next) as c)):
                cmd = coro.send(next)
                print(cmd)
                child = self.graph.find(lambda n: n.id == cmd.id) or Node(cmd.id, Enabled(Suspended(Init())))

                match cmd, child.value:
                    case LFI(id, func, args, kwargs, opts), Enabled(Suspended(Init(next=None))):
                        next = Coro(
                            id,
                            "local",
                            None,
                            func,
                            args,
                            kwargs,
                            opts,
                            Coroutine(id, self.id, func(self.ctx(id, self.registry, self.deps), *args, **kwargs)),
                        ) if isgeneratorfunction(func) else Func(
                            id,
                            "local",
                            None,
                            func,
                            args,
                            kwargs,
                            opts
                        )

                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))

                        return []

                    case RFI(id, func, args, kwargs, opts), Enabled(Suspended(Init(next=None))):
                        next = Func(
                            id,
                            "remote",
                            func,
                            None,
                            args,
                            kwargs,
                            opts
                        )

                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))

                        return []

                    case LFI() | RFI(), Enabled(Running(Init() as f)):
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(f.map(suspends=node))))
                        return []

                    case LFI() | RFI(), Blocked(Running(Init() as f)):
                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Blocked(Running(f.map(suspends=node))))
                        return []

                    case LFI(id) | RFI(id), _:
                        node.add_edge(child)
                        node.transition(Enabled(Running(c.map(next=AWT(id, self.id)))))
                        return []

                    case AWT(id, cid), Enabled(Running(f)):
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(f.map(suspends=node))))
                        return []

                    case AWT(id, cid), Blocked(Running(f)):
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Blocked(Running(f.map(suspends=node))))
                        return []

                    case AWT(id, cid), Enabled(Suspended(Func(type="local") | Coro(type="local") as f)):
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Suspended(f.map(suspends=node))))
                        return []

                    case AWT(id, cid), Blocked(Suspended(Func(type="local") | Coro(type="local") as f)):
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Blocked(Suspended(f.map(suspends=node))))
                        return []

                    case AWT(id, cid), Enabled(Suspended(Func(type="remote") as f)):
                        assert cid == self.id, "Await id must match computation id."
                        callback_id = f"{self.id}:{id}"

                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))

                        # no more to do if already a callback
                        if callback_id in self.callbacks:
                            child.transition(Enabled(Suspended(f.map(suspends=node))))
                            return []

                        child.transition(Blocked(Suspended(f.map(suspends=node))))

                        return [
                            Network(id, self.id, CreateCallbackReq(
                                id=callback_id,
                                promise_id=id,
                                root_promise_id=self.id,
                                timeout=sys.maxsize,
                                recv=self.pid,
                            ))
                        ]

                    case AWT(id, cid), Blocked(Suspended(Func(type="remote") as f)):
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Blocked(Suspended(f.map(suspends=node))))
                        return []

                    case AWT(id, cid), Enabled(Completed(Func(result=result) | Coro(result=result))):
                        assert cid == self.id, "Await id must match computation id."
                        assert result is not None, "Completed result must be set."
                        node.transition(Enabled(Running(c.map(next=result))))
                        return []

                    case TRM(id, result), _:
                        assert id == node.id, "Id must match node id."
                        node.transition(Blocked(Running(c)))

                        match result:
                            case Ok(v):
                                return [
                                    Network(id, self.id, ResolvePromiseReq(
                                        id=id,
                                        ikey=id,
                                        data=v
                                    )),
                                ]
                            case Ko(v):
                                # TODO: retry if there is retry budget
                                return [
                                    Network(id, self.id, RejectPromiseReq(
                                        id=id,
                                        ikey=id,
                                        data=v
                                    )),
                                ]

                    case _:
                        raise NotImplementedError

            case Enabled(Suspended(Init() | Func(type="remote") | Coro(type="local")) | Completed()) | Blocked(Running(Init() | Func(type="local") | Coro(type="local")) | Suspended(Func(type="remote"))):
                # valid states
                return []

            case _:
                # invalid states
                raise NotImplementedError

    def _unblock(self, nodes: list[Node[State]], next: AWT | Result) -> None:
        for node in nodes:
            match node.value:
                case Enabled(Suspended(Coro() as c)):
                    node.transition(Enabled(Running(c.map(next=next))))
                case _:
                    raise NotImplementedError

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
                case LFC(id, func, args, kwargs, opts):
                    self.next = AWT
                    self.skip = True
                    yielded = LFI(id, func, args, kwargs, opts)
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
