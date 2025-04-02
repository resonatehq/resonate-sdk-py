from __future__ import annotations

import contextlib
import queue
import sys
import threading
import uuid
from concurrent.futures import Future
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Final, Literal

from resonate.errors import ResonateStoreError
from resonate.graph import Graph, Node
from resonate.logging import logger
from resonate.models.commands import (
    CancelPromiseRes,
    Command,
    CreateCallbackReq,
    CreateCallbackRes,
    CreatePromiseReq,
    CreatePromiseRes,
    CreatePromiseWithTaskReq,
    CreatePromiseWithTaskRes,
    Delayed,
    Function,
    Invoke,
    Network,
    Noop,
    Receive,
    RejectPromiseReq,
    RejectPromiseRes,
    Request,
    ResolvePromiseReq,
    ResolvePromiseRes,
    Resume,
    Retry,
    Return,
)
from resonate.models.context import (
    AWT,
    LFC,
    LFI,
    RFC,
    RFI,
    Context,
    Yieldable,
)
from resonate.models.result import Ko, Ok, Result

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence

    from resonate.models.callback import Callback
    from resonate.models.durable_promise import DurablePromise
    from resonate.models.options import Options
    from resonate.models.task import Task


class Info:
    def __init__(self) -> None:
        self._attempt = 1

    @property
    def attempt(self) -> int:
        return self._attempt

    def increment_attempt(self) -> None:
        self._attempt += 1


# Scheduler
class Scheduler:
    def __init__(
        self,
        ctx: Callable[[str, Info], Context],
        cq: queue.Queue[Command | tuple[Command, tuple[Future, Future]] | None] | None = None,
        pid: str | None = None,
        unicast: str | None = None,
        anycast: str | None = None,
    ) -> None:
        # ctx
        self.ctx = ctx

        # cq
        self.cq = cq or queue.Queue[Command | tuple[Command, tuple[Future, Future]] | None]()

        # pid
        self.pid = pid or uuid.uuid4().hex

        # unicast / anycast
        self.unicast = unicast or f"poller://default/{pid}"
        self.anycast = anycast or f"poller://default/{pid}"

        # computations
        self.computations: dict[str, Computation] = {}

        # scheduler thread
        self.thread = threading.Thread(target=self.loop, daemon=True)

    def __repr__(self) -> str:
        return f"Scheduler(pid={self.pid}, computations={list(self.computations.values())})"

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
        try:
            if cqe := self.cq.get_nowait():
                match cqe:
                    case (cmd, futures):
                        return self._step(cmd, futures)
                    case cmd:
                        return self._step(cmd)
        except queue.Empty:
            pass

        return []

    def _step(self, cmd: Command, futures: tuple[Future, Future] | None = None) -> Sequence[Request]:
        # skip noop
        if isinstance(cmd, Noop):
            return []

        computation = self.computations.setdefault(cmd.cid, Computation(cmd.cid, self.ctx, self.pid, self.unicast, self.anycast))

        # subscribe
        if futures:
            computation.subscribe(*futures)

        # apply
        computation.apply(cmd)

        # eval
        return computation.eval()


#####################################################################
## Here We Go
#####################################################################

type State = Enabled[Running[Init | Func | Coro]] | Enabled[Suspended[Init | Func | Coro]] | Enabled[Completed[Func | Coro]] | Blocked[Running[Init | Func | Coro]] | Blocked[Suspended[Func]]


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
    opts: Options
    info: Info

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
    opts: Options
    info: Info
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


class PoppableList[T](list[T]):
    def spop(self) -> T | None:
        return self.pop() if self else None


class Computation:
    def __init__(self, id: str, ctx: Callable[[str, Info], Context], pid: str, unicast: str, anycast: str) -> None:
        self.id = id
        self.ctx = ctx
        self.pid = pid

        self.unicast = unicast
        self.anycast = anycast

        self.graph = Graph[State](id, Enabled(Suspended(Init())))
        self.futures: tuple[PoppableList[Future], PoppableList[Future]] = (PoppableList(), PoppableList())

        self.task: Task | None = None
        self.callbacks: dict[str, Callback] = {}

        self.history: list[Command | Request | tuple[str, None | AWT | Result, LFI | RFI | AWT | TRM]] = []

    def __repr__(self) -> str:
        return f"Computation(id={self.id}, runnable={self.runnable()}, suspendable={self.suspendable()})"

    def runnable(self) -> bool:
        return any(n.value.running for n in self.graph.traverse())

    def suspendable(self) -> bool:
        return all(n.value.suspended for n in self.graph.traverse())

    def subscribe(self, fp: Future, fv: Future) -> None:
        self.futures[0].append(fp)
        self.futures[1].append(fv)

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
        self.history.append(cmd)

        match cmd, self.graph.root.value.func:
            case Invoke(id, name, func, args, kwargs, opts, promise_and_task=None), Init(next=None):
                assert not self.task, "Task must not be set."

                info = Info()
                next: Coro | Func
                if func is None:
                    next = Func(id, "remote", name, func, args, kwargs, opts, info)
                elif isgeneratorfunction(func):
                    next = Coro(id, "local", name, func, args, kwargs, opts, info, Coroutine(id, self.id, func(self.ctx(id, info), *args, **kwargs)))
                else:
                    next = Func(id, "local", name, func, args, kwargs, opts, info)

                self.graph.root.transition(Enabled(Running(Init(next=next))))

            case Invoke(id, name, func, args, kwargs, opts, promise_and_task=(promise, task)), Init():
                assert not self.task, "Task must not be set."
                assert promise.pending, "Promise must be pending."

                info = Info()
                next: Coro | Func
                if func is None:
                    next = Func(id, "remote", name, func, args, kwargs, opts, info)
                elif isgeneratorfunction(func):
                    next = Coro(id, "local", name, func, args, kwargs, opts, info, Coroutine(id, self.id, func(self.ctx(id, info), *args, **kwargs)))
                else:
                    next = Func(id, "local", name, func, args, kwargs, opts, info)

                self.task = task
                self.graph.root.transition(Enabled(Running(next.map(promise=promise))))

            case Invoke(), _:
                # first invoke "wins", computation will be joined
                pass

            case Resume(invoke=invoke), Init(next=None):
                self.apply(invoke)

            case Resume(id, promise=promise, task=task), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_receive(node, (promise, task, None))

            case Return(id, res=res), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_return(node, res)

            case Receive(id, res=CreatePromiseWithTaskRes(promise, task)), _:
                assert id == self.id == self.graph.root.id, "Create promise with task is only applicable to root node."

                self._apply_receive(self.graph.root, (promise, task, None))

            case Receive(id, res=CreatePromiseRes(promise) | ResolvePromiseRes(promise) | RejectPromiseRes(promise) | CancelPromiseRes(promise)), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_receive(node, (promise, None, None))

            case Receive(id, res=CreateCallbackRes(promise, callback)), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_receive(node, (promise, None, callback))

            case Retry(id, _), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                match node.value:
                    case Blocked(Running(Init(n))):
                        assert isinstance(n, Coro)
                        assert n.promise is not None
                        node.transition(Enabled(Running(n)))

            case _:
                raise NotImplementedError

    def _apply_return(self, node: Node[State], data: Result) -> None:
        match node.value, data:
            case Blocked(Running(Func(type="local") as f)), result:
                node.transition(Enabled(Running(f.map(result=result))))
            case _:
                raise NotImplementedError

    def _apply_receive(self, node: Node[State], data: tuple[DurablePromise, Task | None, Callback | None]) -> None:
        match (self.task, node.value), data:
            case (None, Blocked(Running(Init(Func(type="local") | Coro(type="local") as next, suspends)))), (promise, task, None) if promise.pending and task and not task.completed:
                assert node == self.graph.root, "Node must be root."
                assert not suspends, "Suspended must be empty for root."
                assert not next.suspends, "Next suspends must be initially empty."

                self.task = task
                node.transition(Enabled(Running(next.map(promise=promise))))

            case (None, _), _:
                # TODO(dfarr): determine these specific cases
                pass

            case (task, Blocked(Running(Init(Func(type="local") | Coro(type="local") as next, suspends)))), (promise, None, None) if promise.pending and not task.completed:
                assert not next.suspends, "Next suspends must be initially empty."
                node.transition(Enabled(Running(next.map(promise=promise))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id, self.id))

            case (task, Blocked(Running(Init(Func(type="remote") as next, suspends)))), (promise, None, None) if promise.pending and not task.completed:
                assert not next.suspends, "Next suspends must be initially empty."
                assert promise.tags.get("resonate:scope") == "global", "Scope must be global."
                node.transition(Enabled(Suspended(next.map(promise=promise))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id, self.id))

            case (task, Blocked(Running(Init(Func(type="local" | "remote") | Coro(type="local") as next, suspends)))), (promise, None, None) if promise.completed and not task.completed:
                assert not next.suspends, "Next suspends must be initially empty."
                node.transition(Enabled(Completed(next.map(promise=promise, result=promise.result))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id, self.id))

            case (task, Blocked(Running(Func(type="local", suspends=suspends) | Coro(type="local", suspends=suspends) as f))), (promise, None, None) if promise.completed and not task.completed:
                node.transition(Enabled(Completed(f.map(promise=promise, result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case (task, Blocked(Suspended(Func(type="remote", suspends=suspends) as f))), (promise, None, None) if promise.completed and not task.completed:
                node.transition(Enabled(Completed(f.map(promise=promise, result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case (task, Blocked(Suspended(Func(id, type="remote") as f))), (promise, None, callback) if promise.pending and not task.completed:
                assert callback, "Callback must be set."

                self.callbacks[id] = callback
                node.transition(Enabled(Suspended(f)))

            case (old_task, Enabled(Suspended(Func(type="remote", suspends=suspends) as f))), (promise, new_task, None) if (
                promise.completed and old_task.completed and new_task and not new_task.completed
            ):
                self.task = new_task
                node.transition(Enabled(Completed(f.map(promise=promise, result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case _:
                # TODO(dfarr): determine these specific cases
                pass

    def eval(self) -> Sequence[Request]:
        reqs: Sequence[Request] = []

        while self.runnable():
            # assert (self.task and not self.task.completed) or isinstance(self.graph.root.value.func, Init)

            for node in self.graph.traverse():
                r = self._eval(node)
                reqs.extend(r)
                self.history.extend(r)

        # It is possible for an invoke command to correspond to an already completed promise,
        # in this case there would either be:
        # 1. No task to complete
        # 2. A previously completed task

        if self.suspendable() and self.task and not self.task.completed:
            # It is possible for the task to have expired, in this case ignore the error
            with contextlib.suppress(ResonateStoreError):
                self.task.complete()

        if promise := self.promise():
            while future := self.futures[0].spop():
                future.set_result(promise)

        if result := self.result():
            while future := self.futures[1].spop():
                match result:
                    case Ok(v):
                        future.set_result(v)
                    case Ko(e):
                        future.set_exception(e)

        return reqs

    def _eval(self, node: Node[State]) -> Sequence[Request]:
        match node.value:
            case Enabled(Running(Init(Func(id, "local", name, _, args, kwargs, opts) | Coro(id, "local", name, _, args, kwargs, opts))) as exec):
                assert id == node.id, "Id must match node id."

                if opts.durable:
                    node.transition(Blocked(exec))

                    if node == self.graph.root:
                        req = CreatePromiseWithTaskReq(
                            id=id,
                            ikey=id,
                            timeout=sys.maxsize,
                            data={"func": name, "args": args, "kwargs": kwargs},
                            tags={"resonate:invoke": self.anycast, "resonate:scope": "global", **opts.tags},
                            pid=self.pid,
                            ttl=sys.maxsize,
                        )
                    else:
                        req = CreatePromiseReq(id=id, ikey=id, timeout=sys.maxsize, tags={"resonate:scope": "local", **opts.tags})

                    return [
                        Network(id, self.id, req),
                    ]
                # TODO(tperez): What happens if not durable
                raise NotImplementedError

            case Enabled(Running(Init(Func(id, "remote", name, _, args, kwargs, opts))) as exec):
                assert id == node.id, "Id must match node id."
                assert name is not None, "Name is required for remote invocation."
                if opts.durable:
                    node.transition(Blocked(exec))

                    return [
                        Network(
                            id,
                            self.id,
                            CreatePromiseReq(
                                id=id,
                                ikey=id,
                                timeout=sys.maxsize,
                                data={"func": name, "args": args, "kwargs": kwargs},
                                tags={"resonate:invoke": opts.send_to, "resonate:scope": "global", **opts.tags},
                            ),
                        ),
                    ]
                # TODO(tperez): What happens if not durable
                raise NotImplementedError

            case Enabled(Running(Func(id, type, _, func, args, kwargs, opts, info, result=result) as f)):
                assert id == node.id, "Id must match node id."
                assert type == "local", "Only local functions are runnable."
                assert func is not None, "Func is required for local function."
                if opts.durable:
                    node.transition(Blocked(Running(f)))

                    match result:
                        case None:
                            return [
                                Function(id, self.id, lambda: func(self.ctx(id, info), *args, **kwargs)),
                            ]
                        case Ok(v):
                            return [
                                Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, data=v)),
                            ]
                        case Ko(v):
                            if (delay := opts.retry_policy.next(info.attempt)) is not None:
                                info.increment_attempt()
                                return [Delayed(Function(id, self.id, lambda: func(self.ctx(id, info), *args, **kwargs)), delay)]

                            return [
                                Network(id, self.id, RejectPromiseReq(id=id, ikey=id, data=v)),
                            ]
                # TODO(tperez): What happens if not durable
                raise NotImplementedError

            case Enabled(Running(Coro() as c)):
                cmd = c.coro.send(c.next)
                child = self.graph.find(lambda n: n.id == cmd.id) or Node(cmd.id, Enabled(Suspended(Init())))
                self.history.append((c.coro.id, c.next, cmd))

                match cmd, child.value:
                    case LFI(id, func, args, kwargs, opts), Enabled(Suspended(Init(next=None))):
                        info = Info()
                        next: Coro | Func
                        if isgeneratorfunction(func):
                            next = Coro(id, "local", None, func, args, kwargs, opts, info, Coroutine(id, self.id, func(self.ctx(id, info), *args, **kwargs)))
                        else:
                            next = Func(id, "local", None, func, args, kwargs, opts, info)

                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))
                        return []

                    case RFI(id, func, args, kwargs, opts), Enabled(Suspended(Init(next=None))):
                        next = Func(id, "remote", func, None, args, kwargs, opts, Info())

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
                            Network(
                                id,
                                self.id,
                                CreateCallbackReq(
                                    id=callback_id,
                                    promise_id=id,
                                    root_promise_id=self.id,
                                    timeout=sys.maxsize,
                                    recv=self.anycast,
                                ),
                            )
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
                        if c.opts.durable:
                            match result:
                                case Ok(v):
                                    node.transition(Blocked(Running(c)))
                                    return [
                                        Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, data=v)),
                                    ]
                                case Ko(v):
                                    if (delay := c.opts.retry_policy.next(c.info.attempt)) is not None:
                                        assert c.promise is not None
                                        c.info.increment_attempt()
                                        node.transition(Blocked(Running(Init(next=c.map(coro=Coroutine(id, self.id, c.func(self.ctx(id, c.info), *c.args, **c.kwargs)))))))
                                        return [Delayed(Retry(id, self.id), delay)]

                                    node.transition(Blocked(Running(c)))
                                    return [
                                        Network(id, self.id, RejectPromiseReq(id=id, ikey=id, data=v)),
                                    ]
                        # TODO(tperez): What happens if not durable
                        raise NotImplementedError

                    case _:
                        raise NotImplementedError

            case Enabled(Suspended(Init() | Func(type="remote") | Coro(type="local")) | Completed()) | Blocked(
                Running(Init() | Func(type="local") | Coro(type="local")) | Suspended(Func(type="remote"))
            ):
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
        logger.info("=" * 40)
        logger.info("Computation: %s", self.id)
        logger.info("=" * 40)

        logger.info("\nTask:")
        logger.info("  %s", self.task)

        logger.info("\nCallbacks:")
        if self.callbacks:
            logger.info("  %s", "\n  ".join(str(cb) for cb in self.callbacks.values()))
        else:
            logger.info("  None")

        logger.info("\nGraph:")
        for node, level in self.graph.traverse_with_level():
            logger.info("%s  %s", "  " * level, node)

        logger.info("\nHistory:")
        if self.history:
            logger.info("  %s", "\n  ".join(str(event) for event in self.history))
        else:
            logger.info("  None")


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
