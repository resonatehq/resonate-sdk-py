from __future__ import annotations

import sys
import uuid
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import TYPE_CHECKING, Any, Final, Literal

from resonate.graph import Graph, Node
from resonate.logging import logger
from resonate.models.commands import (
    CancelPromiseRes,
    Command,
    CreateCallbackReq,
    CreatePromiseReq,
    CreatePromiseRes,
    Delayed,
    Function,
    Invoke,
    Network,
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
    from concurrent.futures import Future

    from resonate.models.durable_promise import DurablePromise
    from resonate.models.options import Options


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
        pid: str | None = None,
        unicast: str | None = None,
        anycast: str | None = None,
    ) -> None:
        # ctx
        self.ctx = ctx

        # pid
        self.pid = pid or uuid.uuid4().hex

        # unicast / anycast
        self.unicast = unicast or f"poller://default/{pid}"
        self.anycast = anycast or f"poller://default/{pid}"

        # computations
        self.computations: dict[str, Computation] = {}

    def __repr__(self) -> str:
        return f"Scheduler(pid={self.pid}, computations={list(self.computations.values())})"

    def step(self, cmd: Command, futures: tuple[Future, Future] | None = None) -> More | Done:
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

    def __post_init__(self) -> None:
        assert self.value.result is not None, "Result must be set."
        self.value.suspends = []

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
    info: Info = field(default_factory=Info)

    suspends: list[Node[State]] = field(default_factory=list)
    result: Result | None = None

    def map(self, *, suspends: Node[State] | None = None, result: Result | None = None) -> Func:
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

    cid: str
    ctx: Callable[[str, Info], Context]
    info: Info = field(default_factory=Info)

    coro: Coroutine = field(init=False)
    next: None | AWT | Result = None

    suspends: list[Node[State]] = field(default_factory=list)
    result: Result | None = None

    def __post_init__(self) -> None:
        self.coro = Coroutine(self.id, self.cid, self.func(self.ctx(self.id, self.info), *self.args, **self.kwargs))

    def reset(self) -> None:
        self.next = None
        self.coro = Coroutine(self.id, self.cid, self.func(self.ctx(self.id, self.info), *self.args, **self.kwargs))

    def map(self, *, next: None | AWT | Result = None, suspends: Node[State] | None = None, result: Result | None = None) -> Coro:
        if next:
            self.next = next
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

@dataclass
class More:
    reqs: Sequence[Request]

@dataclass
class Done:
    reqs: Sequence[Request]

class Computation:
    def __init__(self, id: str, ctx: Callable[[str, Info], Context], pid: str, unicast: str, anycast: str) -> None:
        self.id = id
        self.ctx = ctx
        self.pid = pid

        self.unicast = unicast
        self.anycast = anycast

        self.graph = Graph[State](id, Enabled(Suspended(Init())))
        self.futures: tuple[PoppableList[Future], PoppableList[Future]] = (PoppableList(), PoppableList())
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
            case Invoke(id, name, func, args, kwargs, opts), Init(next=None):
                next: Coro | Func
                if isgeneratorfunction(func):
                    next = Coro(id, "local", name, func, args, kwargs, opts, self.id, self.ctx)
                elif func is None:
                    next = Func(id, "remote", name, func, args, kwargs, opts)
                else:
                    next = Func(id, "local", name, func, args, kwargs, opts)

                # initialize root node of graph
                self.graph.root.transition(Enabled(Running(next)))

            case Invoke(), _:
                # first invoke "wins", computation will be joined
                pass

            case Resume(invoke=Invoke() as invoke), Init(next=None):
                self.apply(invoke)

            case Resume(id, promise=promise), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_receive(node, promise)

            case Return(id, res=res), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_return(node, res)

            case Receive(id, res=CreatePromiseRes(promise) | ResolvePromiseRes(promise) | RejectPromiseRes(promise) | CancelPromiseRes(promise)), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_receive(node, promise)

            case Retry(id), _:
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Node must exist."

                self._apply_retry(node)

            case _:
                raise NotImplementedError

    def _apply_return(self, node: Node[State], result: Result) -> None:
        match node.value:
            case Blocked(Running(Func(type="local") as f)):
                node.transition(Enabled(Running(f.map(result=result))))
            case _:
                raise NotImplementedError

    def _apply_receive(self, node: Node[State], promise: DurablePromise) -> None:
        match node.value, promise:
            case Blocked(Running(Init(Func(type="local") | Coro(type="local") as next, suspends))), promise if promise.pending:
                assert not next.suspends, "Suspends must be initially empty."
                node.transition(Enabled(Running(next)))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id, self.id))

            case Blocked(Running(Init(Func(type="remote") as next, suspends))), promise if promise.pending:
                assert not next.suspends, "Next suspends must be initially empty."
                assert promise.tags.get("resonate:scope") == "global", "Scope must be global."
                node.transition(Enabled(Suspended(next)))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id, self.id))

            case Blocked(Running(Init(Func(type="local" | "remote") | Coro(type="local") as next, suspends))), promise if promise.completed:
                assert not next.suspends, "Suspends must be initially empty."
                node.transition(Enabled(Completed(next.map(result=promise.result))))

                # unblock waiting[p]
                self._unblock(suspends, AWT(next.id, self.id))

            case Blocked(Running(Func(type="local", suspends=suspends) | Coro(type="local", suspends=suspends) as f)), promise if promise.completed:
                node.transition(Enabled(Completed(f.map(result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case Blocked(Suspended(Func(type="remote", suspends=suspends) as f)), promise if promise.completed:
                node.transition(Enabled(Completed(f.map(result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case Enabled(Suspended(Func(type="remote", suspends=suspends) as f)), promise if promise.completed:
                node.transition(Enabled(Completed(f.map(result=promise.result))))

                # unblock waiting[v]
                self._unblock(suspends, promise.result)

            case _:
                raise NotImplementedError

    def _apply_retry(self, node: Node[State]) -> None:
        match node.value:
            case Blocked(Running(Coro() as c)):
                assert c.next is None, "Next must be None."
                node.transition(Enabled(Running(c)))
            case _:
                raise NotImplementedError

    def eval(self) -> More | Done:
        reqs: Sequence[Request] = []

        while self.runnable():
            for node in self.graph.traverse():
                r = self._eval(node)
                reqs.extend(r)
                self.history.extend(r)

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

        if self.suspendable():
            callbacks: list[Network] = []

            for node in self.graph.filter(lambda n: isinstance(n.value.func, Func) and len(n.value.func.suspends) > 0):
                assert isinstance(node.value, Enabled)
                assert isinstance(node.value.exec, Suspended)
                assert isinstance(node.value.func, Func)
                assert node is not self.graph.root
                assert node.value.func.type == "remote"

                callbacks.append(Network(node.id, self.id, CreateCallbackReq(
                    f"{self.id}:{node.id}",
                    node.id,
                    self.id,
                    sys.maxsize,
                    self.anycast,
                )))

            return Done(callbacks)

        return More(reqs)

    def _eval(self, node: Node[State]) -> Sequence[Request]:
        match node.value:
            case Enabled(Running(Init(Func(id, "local", name, _, args, kwargs, opts) | Coro(id, "local", name, _, args, kwargs, opts))) as exec):
                assert id == node.id, "Id must match node id."
                assert node is not self.graph.root, "Node must not be root node."
                node.transition(Blocked(exec))

                return [
                    Network(
                        id,
                        self.id,
                        CreatePromiseReq(
                            id=id,
                            ikey=id,
                            timeout=opts.timeout,
                            tags={**opts.tags, "resonate:scope": "local"},
                        ),
                    ),
                ]

            case Enabled(Running(Init(Func(id, "remote", name, _, args, kwargs, opts))) as exec):
                assert id == node.id, "Id must match node id."
                assert name is not None, "Name is required for remote invocation."
                node.transition(Blocked(exec))

                return [
                    Network(
                        id,
                        self.id,
                        CreatePromiseReq(
                            id=id,
                            ikey=id,
                            timeout=opts.timeout,
                            data={"func": name, "args": args, "kwargs": kwargs},
                            tags={**opts.tags, "resonate:invoke": opts.send_to, "resonate:scope": "global"},
                        ),
                    ),
                ]

            case Enabled(Running(Func(id, type, _, func, args, kwargs, opts, info, result=result) as f)):
                assert id == node.id, "Id must match node id."
                assert type == "local", "Only local functions are runnable."
                assert func is not None, "Func is required for local function."
                node.transition(Blocked(Running(f)))

                match result, opts.retry_policy.next(info.attempt):
                    case None, _:
                        return [
                            Function(id, self.id, lambda: func(self.ctx(id, info), *args, **kwargs)),
                        ]

                    case Ok(v), _:
                        return [
                            Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, data=v)),
                        ]

                    case Ko(v), None:
                        return [
                            Network(id, self.id, RejectPromiseReq(id=id, ikey=id, data=v)),
                        ]

                    case Ko(), delay:
                        info.increment_attempt()
                        return [
                            Delayed(Function(id, self.id, lambda: func(self.ctx(id, info), *args, **kwargs)), delay),
                        ]

            case Enabled(Running(Coro(id=id, coro=coro, next=next, opts=opts, info=info) as c)):
                cmd = coro.send(next)
                child = self.graph.find(lambda n: n.id == cmd.id) or Node(cmd.id, Enabled(Suspended(Init())))
                self.history.append((id, next, cmd))

                match cmd, child.value:
                    case LFI(id, func, args, kwargs, opts), Enabled(Suspended(Init(next=None))):
                        next = Coro(id, "local", None, func, args, kwargs, opts, self.id, self.ctx) if isgeneratorfunction(func) else Func(id, "local", None, func, args, kwargs, opts)

                        node.add_edge(child)
                        node.add_edge(child, "waiting[p]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Running(Init(next, suspends=[node]))))
                        return []

                    case RFI(id, func, args, kwargs, opts), Enabled(Suspended(Init(next=None))):
                        next = Func(id, "remote", func, None, args, kwargs, opts)

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
                        node.add_edge(child, "waiting[v]")
                        node.transition(Enabled(Suspended(c)))
                        child.transition(Enabled(Suspended(f.map(suspends=node))))
                        return []

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

                        match result, opts.retry_policy.next(info.attempt):
                            case Ok(v), _:
                                return [
                                    Network(id, self.id, ResolvePromiseReq(id=id, ikey=id, data=v)),
                                ]

                            case Ko(v), None:
                                return [
                                    Network(id, self.id, RejectPromiseReq(id=id, ikey=id, data=v)),
                                ]

                            case Ko(), delay:
                                c.reset()
                                info.increment_attempt()

                                return [
                                    Delayed(Retry(id, self.id), delay),
                                ]
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
        format = """
========================================
Computation: %s
========================================

Graph:
  %s

History:
  %s
"""

        logger.info(
            format,
            self.id,
            "\n  ".join(f"{'  ' * level}{node}" for node, level in self.graph.traverse_with_level()),
            "\n  ".join(str(event) for event in self.history) if self.history else "None",
        )


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
