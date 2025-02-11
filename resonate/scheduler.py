from __future__ import annotations

from logging import root
import queue
import sys
import threading
import uuid
from collections.abc import Callable, Generator
from concurrent.futures import Future
from dataclasses import dataclass, field
from inspect import isgeneratorfunction
from typing import Any, Literal

from resonate.context import Context
from resonate.graph import Graph, Node
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
from resonate.models.enqueuable import Enqueueable
from resonate.models.message import Mesg
from resonate.models.result import Ko, Ok, Result
from resonate.models.store import Store
from resonate.models.task import Task
from resonate.processor import Processor
from resonate.registry import Registry
from resonate.stores import LocalStore
from resonate.stores.local import Recv

# Sender


class LocalSender:
    def __init__(self, pid: str, ttl: int,registry: Registry, store: Store, cq: Enqueueable[Invoke | Resume | Notify]) -> None:
        self._store = store
        self._q = cq
        self._pid = pid
        self._ttl = ttl
        self._registry = registry

    def send(self, recv: Recv, mesg: Mesg) -> None:
        print(mesg)
        assert False, "We are here"
        # translates a msg into a cmd
        match mesg:
            case {"type": "invoke", "task": task_mesg}:
                root, leaf = self._store.tasks.claim(task_mesg["id"], task_mesg["counter"], self._pid, self._ttl)
                assert leaf is None
                self._registry.get(root.params)
                self._q.enqueue(Invoke(id=roo))
            case {"type": "resume", "task": task_mesg}:
                self._q.enqueue((task_mesg["id"], task_mesg["counter"]))
            case {"type": "notify", "task": task_mesg}:
                raise NotImplementedError


# Commands

type Command = Invoke | Resume | Return | Listen | Notify


@dataclass
class Invoke:
    id: str
    name: str
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    promise: DurablePromise | None = None
    task: Task | None = None

    @property
    def cid(self) -> str:
        return self.id


@dataclass
class Resume:
    id: str
    cid: str
    promise: DurablePromise
    task: Task
    invoke: Invoke


@dataclass
class Return:
    id: str
    cid: str
    result: Result


@dataclass
class Listen:
    id: str

    @property
    def cid(self) -> str:
        return self.id


@dataclass
class Notify:
    id: str
    promise: DurablePromise

    @property
    def cid(self) -> str:
        return self.id


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
        if isinstance(self.store, LocalStore):
            self.store.add_sender(self.pid, LocalSender(self.store, self))

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

        # apply the command
        computation.apply(cmd)

        # run generators until blocked
        for f in computation.run_until_blocked():
            match f:
                case LFX(id, func, args, kwargs):
                    # enqueue the function
                    # note: bind arguments to lambdas to avoid late binding issues
                    self.processor.enqueue(
                        lambda func=func, args=args, kwargs=kwargs: func(*args, **kwargs),
                        lambda r, id=id: self.enqueue(Return(id, computation.id, r)),
                    )
                case RFX(id):
                    pass

        # subscribe to the computation
        if future:
            computation.subscribe(future)

        # resolve handles is computation has a result
        if result := computation.result():
            for f in computation.subscriptions:
                match result:
                    case Ok(r):
                        f.set_result(r)
                    case Ko(e):
                        f.set_exception(e)

            # clear the subscriptions because futures cannot be set more than once
            computation.subscriptions.clear()

        # print the computation
        computation.print()


# Computation

type V = Init | Done | Internal | External | RunnableFunc | RunnableCoro | AwaitingCoro

type E = Literal["spawned", "waiting"]


@dataclass
class Init:
    pass


@dataclass
class Done:
    result: Result


@dataclass
class Internal:
    promise: DurablePromise


@dataclass
class External:
    promise: DurablePromise


@dataclass
class RunnableFunc:
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    promise: DurablePromise


@dataclass
class RunnableCoro:
    coro: Coroutine
    next: Result
    promise: DurablePromise


@dataclass
class AwaitingCoro:
    coro: Coroutine
    promise: DurablePromise


class Computation:
    def __init__(self, id: str, pid: str, ctx: type[Contextual], registry: Registry, store: LocalStore) -> None:
        self.id = id
        self.pid = pid
        self.ctx = ctx
        self.registry = registry
        self.store = store
        self.graph = Graph[V, E](Node(id, Init()), "spawned")

        self.callback: Any | None = None
        self.subscriptions: list[Future] = []

        self.task: Task | None = None

    def done(self) -> bool:
        return isinstance(self.graph.root.value, Done)

    def blocked(self) -> bool:
        return not self.graph.find(lambda n: isinstance(n.value, (RunnableCoro, RunnableFunc)))

    def done_or_blocked_on_external(self) -> bool:
        for node in self.graph.traverse():
            if isinstance(node.value, (Internal, RunnableCoro, RunnableFunc)):
                return False

        return True

    def subscribe(self, future: Future) -> None:
        self.subscriptions.append(future)

    def result(self) -> Result | None:
        match self.graph.root.value:
            case Done(result):
                return result
            case _:
                return None

    def apply(self, cmd: Command) -> None:
        print(cmd)

        match cmd, self.graph.root.value:
            case Invoke(id, name, func, args, kwargs, promise, task), Init():
                assert id == cmd.cid == self.graph.root.id, "Invoke, computation, and root ids must all be the same."

                if task:
                    assert promise, "Promise must be set if task is set."
                    self.task = task  # task is pre-claimed
                else:
                    # create promise and task
                    promise, self.task = self.store.promises.create_with_task(
                        id=id,
                        timeout=sys.maxsize,
                        pid=self.pid,
                        ttl=0,
                        ikey=id,
                        data={"func": name, "args": args, "kwargs": kwargs},
                        tags={"resonate:invoke": self.pid, "resonate:scope": "global"},
                    )

                    # If the promise was created on the local event loop it could cause
                    # deadlock, so throw an assertion error for now.
                    assert promise.tags.get("resonate:scope") == "global", "Scope must be global."

                match promise.pending, isgeneratorfunction(func):
                    case True, True:
                        # Init -> RunnableCoro
                        self.graph.root.transition(RunnableCoro(Coroutine(id, cmd.id, func(self.ctx(id, self.registry), *args, **kwargs)), Ok(None), promise))
                    case True, False:
                        # Init -> RunnableFunc
                        self.graph.root.transition(RunnableFunc(func, args, kwargs, promise))
                    case _:
                        # Init -> Done
                        self.graph.root.transition(Done(promise.result))

            case Resume(invoke=invoke), Init():
                # restart
                self.apply(invoke)

            case Resume(id, cid, promise, task), _:
                assert id == promise.id, "Resume id must match promise id."
                assert promise.completed, "Promise must be completed."

                # set task, task is pre-claimed
                self.task = task

                # a resume command "wins" because we know the promise is complete
                if node := self.graph.find(lambda n: n.id == id and not isinstance(n.value, Done)):
                    assert isinstance(node.value, (Internal, External, AwaitingCoro)), "Resumed node must be internal, external or awaiting."

                    # Internal/External/AwaitingCoro -> Done
                    node.transition(Done(promise.result))

                    # AwaitingCoro -> RunnableCoro
                    self.unblock(node)

            case Return(id, cid, result), _:
                assert cid == self.graph.root.id, "Computation and root id must be the same."

                # a return command can only apply to an internal node
                node = self.graph.find(lambda n: n.id == id)
                assert node, "Returned node must exist."
                assert isinstance(node.value, Internal), "Returned node must be internal."

                # complete promise
                assert id == node.value.promise.id, "Return id must match promise id."

                match result:
                    case Ok(r):
                        node.value.promise = self.store.promises.resolve(id=id, ikey=id, data=r)
                    case Ko(e):
                        node.value.promise = self.store.promises.reject(id=id, ikey=id, data=e)

                assert node.value.promise.completed, "Promise must be completed."

                # RunnableCoro -> Done
                node.transition(Done(node.value.promise.result))

                # AwaitingCoro -> RunnableCoro
                self.unblock(node)

            case Listen(id), Init():
                assert id == cmd.cid == self.graph.root.id, "Listen, computation, and root ids must all be the same."

                if not self.callback:
                    # TODO: call subscribe
                    self.callback = True

            case Notify(id, promise), Init():
                assert id == cmd.cid == promise.id == self.graph.root.id, "Notify, computation, promise and root ids must all be the same."
                assert promise.completed, "Promise must be completed."

                # Init -> Done
                self.graph.root.transition(Done(promise.result))

    def run_until_blocked(self) -> list[LFX | RFX]:  # TODO: should we really return RFX here?
        commands = []

        while not self.blocked():
            for node in self.graph.traverse():
                commands.extend(self._run_until_blocked(node))

        # complete task
        if self.done_or_blocked_on_external() and self.task:
            self.store.tasks.complete(id=self.task.id, counter=self.task.counter)
            self.task = None

        return commands

    def _run_until_blocked(self, node: Node[V, E]) -> list[LFX | RFX]:
        commands = []

        while True:
            match node.value:
                case RunnableCoro(coro, next, promise):
                    assert promise.pending, "Promise must be pending."

                    cmd = coro.send(next)
                    print(cmd)

                    child = self.graph.find(lambda n: n.id == cmd.id) or Node[V, E](cmd.id, Init())

                    match cmd, child.value:
                        case LFI(id, func, args, kwargs), Init():
                            # create promise
                            child_promise = self.store.promises.create(id=id, ikey=id, timeout=sys.maxsize, tags={"resonate:scope": "local"})

                            match child_promise.pending, isgeneratorfunction(func):
                                case True, True:
                                    # Init -> RunnableCoro
                                    child.transition(RunnableCoro(Coroutine(id, self.id, func(self.ctx(id, self.registry), *args, **kwargs)), Ok(None), child_promise))

                                    # RunnableCoro -> RunnableCoro
                                    node.transition(RunnableCoro(coro, Ok(AWT(id, self.id)), promise))
                                    node.add_edge("spawned", child)
                                case True, False:
                                    # Init -> Internal
                                    child.transition(Internal(child_promise))

                                    # RunnableCoro -> RunnableCoro
                                    node.transition(RunnableCoro(coro, Ok(AWT(id, self.id)), promise))
                                    node.add_edge("spawned", child)

                                    # add command
                                    commands.append(LFX(id, func, args, kwargs))
                                case _:
                                    # Init -> Done
                                    child.transition(Done(child_promise.result))

                                    # RunnableCoro -> RunnableCoro
                                    node.transition(RunnableCoro(coro, Ok(AWT(id, self.id)), promise))
                                    node.add_edge("spawned", child)

                        case LFC(id, func, args, kwargs), Init():
                            # create promise
                            child_promise = self.store.promises.create(id=id, ikey=id, timeout=sys.maxsize, tags={"resonate:scope": "local"})

                            match child_promise.pending, isgeneratorfunction(func):
                                case True, True:
                                    # Init -> RunnableCoro
                                    child.transition(RunnableCoro(Coroutine(id, self.id, func(self.ctx(id, self.registry), *args, **kwargs)), Ok(None), child_promise))

                                    # RunnableCoro -> AwaitingCoro
                                    node.transition(AwaitingCoro(coro, promise))
                                    node.add_edge("spawned", child)
                                    node.add_edge("waiting", child)
                                case True, False:
                                    # Init -> Internal
                                    child.transition(Internal(child_promise))

                                    # RunnableCoro -> AwaitingCoro
                                    node.transition(AwaitingCoro(coro, promise))
                                    node.add_edge("spawned", child)
                                    node.add_edge("waiting", child)

                                    # add command
                                    commands.append(LFX(id, func, args, kwargs))
                                case _:
                                    # Init -> Done
                                    child.transition(Done(child_promise.result))

                                    # RunnableCoro -> RunnableCoro
                                    node.transition(RunnableCoro(coro, child_promise.result, promise))
                                    node.add_edge("spawned", child)

                        case RFI(id, func, args, kwargs), Init():
                            # create promise
                            child_promise = self.store.promises.create(
                                id=id,
                                ikey=id,
                                timeout=sys.maxsize,
                                data={"func": func, "args": args, "kwargs": kwargs},
                                tags={"resonate:invoke": self.pid, "resonate:scope": "global"},
                            )

                            # If the promise was created on the local event loop it could cause
                            # deadlock, so throw an assertion error for now.
                            assert child_promise.tags.get("resonate:scope") == "global", "Scope must be global."

                            match child_promise.pending:
                                case True:
                                    # Init -> External
                                    child.transition(External(child_promise))

                                    # RunnableCoro -> RunnableCoro
                                    node.transition(RunnableCoro(coro, Ok(AWT(id, self.id)), promise))
                                    node.add_edge("spawned", child)

                                    # add command
                                    commands.append(RFX(id, func, args, kwargs))

                                case False:
                                    # Init -> Done
                                    child.transition(Done(child_promise.result))

                                    # RunnableCoro -> RunnableCoro
                                    node.transition(RunnableCoro(coro, Ok(AWT(id, self.id)), promise))
                                    node.add_edge("spawned", child)

                        case RFC(id, func, args, kwargs), Init():
                            # create promise
                            child_promise = self.store.promises.create(
                                id=id,
                                ikey=id,
                                timeout=promise.timeout,
                                data={"func": func, "args": args, "kwargs": kwargs},
                                tags={"resonate:invoke": self.pid, "resonate:scope": "global"},
                            )

                            # If the promise was created on the local event loop it could cause
                            # deadlock, so throw an assertion error for now.
                            assert child_promise.tags.get("resonate:scope") == "global", "Scope must be global."

                            match child_promise.pending:
                                case True:
                                    # create callback
                                    child_promise, callback = self.store.promises.callback(
                                        id=f"{self.id}.{id}",
                                        promise_id=id,
                                        root_promise_id=self.id,
                                        timeout=promise.timeout,
                                        recv=self.pid,
                                    )

                                    match child_promise.pending:
                                        case True:
                                            assert callback, "Callback must be set."

                                            # Init -> External
                                            child.transition(External(child_promise))

                                            # RunnableCoro -> AwaitingCoro
                                            node.transition(AwaitingCoro(coro, promise))
                                            node.add_edge("spawned", child)
                                            node.add_edge("waiting", child)

                                            # add command
                                            commands.append(RFX(id, func, args, kwargs))
                                        case False:
                                            # Init -> Done
                                            child.transition(Done(child_promise.result))

                                            # RunnableCoro -> RunnableCoro
                                            node.transition(RunnableCoro(coro, child_promise.result, promise))
                                            node.add_edge("spawned", child)

                                case False:
                                    # Init -> Done
                                    child.transition(Done(child_promise.result))

                                    # RunnableCoro -> RunnableCoro
                                    node.transition(RunnableCoro(coro, child_promise.result, promise))
                                    node.add_edge("spawned", child)

                        case LFI(id) | RFI(id), _:
                            # RunnableCoro -> RunnableCoro
                            node.transition(RunnableCoro(coro, Ok(AWT(id, self.id)), promise))
                            node.add_edge("spawned", child)

                        case LFC() | RFC(), Done(result):
                            # RunnableCoro -> RunnableCoro
                            node.transition(RunnableCoro(coro, result, promise))
                            node.add_edge("spawned", child)

                        case LFC() | RFC(), _:
                            # RunnableCoro -> AwaitingCoro
                            node.transition(AwaitingCoro(coro, promise))
                            node.add_edge("spawned", child)
                            node.add_edge("waiting", child)

                        case AWT(id, cid), External():
                            assert cid == self.id, "Await id must match computation id."

                            # create callback
                            child_promise, callback = self.store.promises.callback(
                                id=f"{self.id}.{id}",
                                promise_id=id,
                                root_promise_id=self.id,
                                timeout=promise.timeout,
                                recv=self.pid,
                            )

                            match child_promise.pending:
                                case True:
                                    assert callback, "Callback must be set."

                                    # RunnableCoro -> AwaitingCoro
                                    node.transition(AwaitingCoro(coro, promise))
                                    node.add_edge("waiting", child)
                                case False:
                                    # External -> Done
                                    child.transition(Done(child_promise.result))

                                    # RunnableCoro -> RunnableCoro
                                    node.transition(RunnableCoro(coro, child_promise.result, promise))

                        case AWT(id, cid), Done(result):
                            assert cid == self.id, "Await id must match computation id."

                            # RunnableCoro -> RunnableCoro
                            node.transition(RunnableCoro(coro, result, promise))

                        case AWT(id, cid), _:
                            assert cid == self.id, "Await id must match computation id."
                            assert not isinstance(child.value, Init), "Child must not be init."

                            # RunnableCoro -> AwaitingCoro
                            node.transition(AwaitingCoro(coro, promise))
                            node.add_edge("waiting", child)

                        case TRM(id, result), _:
                            assert id == node.id, "Terminate id must match node id."
                            assert id == promise.id, "Terminate id must match promise id."

                            match result:
                                case Ok(r):
                                    promise = self.store.promises.resolve(id=id, ikey=id, data=r)
                                case Ko(e):
                                    promise = self.store.promises.reject(id=id, ikey=id, data=e)

                            assert promise.completed, "Promise must be completed."

                            # RunnableCoro -> Done
                            node.transition(Done(promise.result))

                            # AwaitingCoro -> RunnableCoro
                            self.unblock(node)

                        case _, _:
                            print("************")
                            print("This we could not handle")
                            print(cmd, child)
                            print("************")
                            assert False, "no way jose"

                case RunnableFunc(func, args, kwargs, promise):
                    assert node.id == self.id, "Node id must match computation id."

                    # create new command
                    commands.append(LFX(node.id, func, args, kwargs))  # TODO: map to different command

                    # RunnableCoro -> Internal
                    node.transition(Internal(promise))

                case _:
                    break

        # Detect cycle

        waiting = node.get_edge("waiting")
        assert len(waiting) <= 1, "Must have at most a single waiting edge."

        if waiting:
            path = [n.id for n in waiting[0].traverse("waiting")]
            assert node.id not in path, f"Cycle detected: {node.id} -> {' -> '.join(path)}."

        return commands

    def unblock(self, node: Node) -> None:
        assert isinstance(node.value, Done), "Node must be done."

        for blocked in self.graph.filter(lambda n: n.has_edge("waiting", node)):
            assert isinstance(blocked.value, AwaitingCoro), "Blocked must be awaiting."

            # remove waiting edge
            blocked.rmv_edge("waiting", node)

            # AwaitingCoro -> RunnableCoro
            blocked.transition(RunnableCoro(blocked.value.coro, node.value.result, blocked.value.promise))

    def print(self) -> None:
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
        self.unyielded: list[AWT | TRM] = []

    def __repr__(self):
        return f"Coroutine(done={self.done})"

    def send(self, value: Result) -> Yieldable | TRM:
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

            match yielded:
                case LFI(id) | RFI(id, mode="attached"):
                    self.unyielded.append(AWT(id, self.cid))
                case AWT(id):
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
