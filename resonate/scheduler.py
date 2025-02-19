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
from resonate.models.commands import CallbackEffect, Command, Invoke, Listen, Network, Function, Notify, PromiseEffect, Request, Resume, Return, Return
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
        # if isinstance(self.store, LocalStore):
        #     self.store.add_sender(self.pid, LocalSender(self, self.registry, self.store))

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

        reqs = [
            *computation.apply(cmd),
            *computation.run_until_blocked(),
        ]

        for req in reqs:
            self.processor.enqueue(
                req.func,
                lambda r, cmd=cmd, cont=req.cont: self.enqueue(Return(cmd.id, cmd.cid, cont, r)),
            )

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

type V = Init | Done | Internal | External | Func | Coro

type E = Literal["spawned", "waiting_p", "waiting_v"]

@dataclass
class Init:
    @property
    def runnable(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return False

@dataclass
class Done:
    result: Result

    @property
    def runnable(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return True

@dataclass
class Coro:
    coro: Coroutine
    next: Result | None = None
    term: bool = False

    @property
    def runnable(self) -> bool:
        return self.next is not None

    @property
    def suspended(self) -> bool:
        return self.next is None and not self.term

@dataclass
class Func:
    func: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]

    @property
    def runnable(self) -> bool:
        return True

    @property
    def suspended(self) -> bool:
        return False

@dataclass
class Internal:
    @property
    def runnable(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return False

@dataclass
class External:
    awaited: bool = False

    @property
    def runnable(self) -> bool:
        return False

    @property
    def suspended(self) -> bool:
        return True

class Computation:
    def __init__(self, id: str, pid: str, ctx: type[Contextual], registry: Registry, store: Store) -> None:
        self.id = id
        self.pid = pid
        self.ctx = ctx
        self.registry = registry
        self.store = store
        self.graph = Graph[V, E](Node(id, Init()), "spawned")

        self.task: Task | None = None
        self.subscriptions: list[Future] = []

        self.invoked = False
        self.subscribed = False

    def done(self) -> bool:
        return isinstance(self.graph.root.value, Done)

    def runnable(self) -> bool:
        return self.graph.find(lambda n: n.value.runnable) is not None

    def suspendable(self) -> bool:
        return self.graph.find(lambda n: n.value.suspended) is not None

    def subscribe(self, future: Future) -> None:
        self.subscriptions.append(future)

    def result(self) -> Result | None:
        match self.graph.root.value:
            case Done(result):
                return result
            case _:
                return None

    def apply(self, cmd: Command) -> Sequence[Request]:
        print(cmd)

        match cmd, self.invoked:
            case Invoke(id, name, func, args, kwargs), False:
                self.invoked = True
                assert isinstance(self.graph.root.value, Init), "Graph root must be init"

                return [
                    Network(
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
                        self.transition_coro_or_func(
                            self.graph.root,
                            func,
                            args,
                            kwargs,
                        )
                    ),
                ]

            case Resume(invoke=invoke), False:
                # restart
                return self.apply(invoke)

            case Resume(id, cid, result), _:
                # assert id == promise.id, "Resume id must match promise id."
                # assert promise.completed, "Promise must be completed."

                # set task, task is pre-claimed
                # self.task = task

                # a resume command "wins" because we know the promise is complete
                if node := self.graph.find(lambda n: n.id == id and not isinstance(n.value, Done)):
                    assert isinstance(node.value, (Coro, Internal, External)), "Resumed node must be coro, internal, or external."

                    node.transition(Done(result))
                    self.unblock(node, "waiting_v", result)

                    return []

            case Return(id, cid, cont, result), _:
                assert cid == self.graph.root.id, "Computation and root id must be the same."
                return cont(result)

            # case Listen(id), Init():
            #     assert id == cmd.cid == self.graph.root.id, "Listen, computation, and root ids must all be the same."

            #     if not self.callback:
            #         # TODO: call subscribe
            #         self.callback = True

            # case Notify(id, promise), Init():
                # assert id == cmd.cid == promise.id == self.graph.root.id, "Notify, computation, promise and root ids must all be the same."
                # assert promise.completed, "Promise must be completed."

                # # Init -> Done
                # self.graph.root.transition(Done(promise.result))

            case _:
                raise NotImplementedError

        return []

    def run_until_blocked(self) -> Sequence[Request]:
        requests: Sequence[Request] = []

        while self.runnable():
            for node in self.graph.traverse():
                next, reqs = self._run_once(node)
                node.transition(next)
                requests.extend(reqs)

            # look for cicular dependencies here

        # complete task
        if self.suspendable() and self.task:
            self.store.tasks.complete(id=self.task.id, counter=self.task.counter)
            self.task = None

        return requests

    def _run_once(self, node: Node[V, E]) -> tuple[V, Sequence[Request]]:
        match node.value:
            case Coro(coro, next, term=False) if next is not None:
                cmd = coro.send(next)
                print(cmd)

                match cmd, self.graph.find(lambda n: n.id == cmd.id):
                    case LFI(id, func, args, kwargs) | LFC(id, func, args, kwargs), None:
                        child = Node[V, E](id, Init())
                        node.add_edge("spawned", child)
                        node.add_edge("waiting_p" if isinstance(cmd, LFI) else "waiting_v", child)

                        return Coro(coro), [
                            Network(
                                functools.partial(
                                    self.store.promises.create,
                                    id=id,
                                    ikey=id,
                                    timeout=sys.maxsize,
                                    tags={"resonate:scope": "local"},
                                ),
                                self.transition_coro_or_func(
                                    child,
                                    func,
                                    args,
                                    kwargs,
                                ),
                            ),
                        ]

                    case RFI(id, func, args, kwargs), None:
                        child = Node[V, E](id, Init())
                        node.add_edge("spawned", child)
                        node.add_edge("waiting_p", child)

                        return Coro(coro), [
                            Network(
                                functools.partial(
                                    self.store.promises.create,
                                    id=id,
                                    ikey=id,
                                    timeout=sys.maxsize,
                                    data={"func": func, "args": args, "kwargs": kwargs},
                                    tags={"resonate:scope": "global"},
                                ),
                                self.transition(
                                    child,
                                    External(awaited=False),
                                ),
                            ),
                        ]

                    case RFC(id, func, args, kwargs), None:
                        child = Node[V, E](id, Init())
                        node.add_edge("spawned", child)
                        node.add_edge("waiting_v", child)

                        return Coro(coro), [
                            Network(
                                functools.partial(
                                    self.store.promises.create,
                                    id=id,
                                    ikey=id,
                                    timeout=sys.maxsize,
                                    data={"func": func, "args": args, "kwargs": kwargs},
                                    tags={"resonate:scope": "global"},
                                ),
                                lambda _: [
                                    Network(
                                        functools.partial(
                                            self.store.promises.callback,
                                            id=f"{self.id}.{id}",
                                            promise_id=id,
                                            root_promise_id=self.id,
                                            timeout=sys.maxsize,
                                            recv=self.pid,
                                        ),
                                        self.transition(
                                            child,
                                            External(awaited=True),
                                        ),
                                    )
                                ]
                            ),
                        ]

                    case LFI() | LFC() | RFI() | RFC(), Node(value=Init()) as child:
                        node.add_edge("spawned", child)
                        node.add_edge("waiting_p" if isinstance(cmd, LFI) else "waiting_v", child)
                        return Coro(coro), []

                    case LFC() | RFC(), Node(value=Done(result)) as child:
                        node.add_edge("spawned", child)
                        return Coro(coro, result), []

                    case LFI(id) | RFI(id), child:
                        node.add_edge("spawned", child)
                        return Coro(coro, Ok(AWT(id, self.id))), []

                    case LFC() | RFC(), child:
                        node.add_edge("spawned", child)
                        node.add_edge("waiting_v", child)
                        return Coro(coro), []

                    case AWT(id, cid), None | Node(value=Init()):
                        assert cid == self.id, "Await id must match computation id."
                        raise NotImplementedError

                    case AWT(id, cid), Node(value=External(awaited=False)) as child:
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge("waiting_v", child)
                        return Coro(coro), [
                            Network(
                                functools.partial(
                                    self.store.promises.callback,
                                    id=f"{self.id}.{id}",
                                    promise_id=id,
                                    root_promise_id=self.id,
                                    timeout=sys.maxsize,
                                    recv=self.pid,
                                ),
                                self.transition(
                                    child,
                                    External(awaited=True),
                                ),
                            )
                        ]

                    case AWT(id, cid), Node(value=Done(result)):
                        assert cid == self.id, "Await id must match computation id."
                        return Coro(coro, result), []

                    case AWT(id, cid), child:
                        assert cid == self.id, "Await id must match computation id."
                        node.add_edge("waiting_v", child)
                        return Coro(coro), []

                    case TRM(id, result), _:
                        assert id == node.id, "Terminate id must match node id."

                        return Coro(coro, term=True), [
                            Network(
                                functools.partial(
                                    self.store.promises.resolve if isinstance(result, Ok) else self.store.promises.reject,
                                    id=id,
                                    ikey=id,
                                    data=result.value
                                ),
                                self.transition(
                                    node,
                                ),
                            ),
                        ]

                    case _:
                        raise NotImplementedError

            case Func(func, args, kwargs):
                return Internal(), [
                    Function(
                        lambda: func(*args, **kwargs),
                        lambda result: [
                            Network(
                                functools.partial(
                                    self.store.promises.resolve if isinstance(result, Ok) else self.store.promises.reject,
                                    id=node.id,
                                    ikey=node.id,
                                    data=result.value
                                ),
                                self.transition(
                                    node,
                                ),
                            ),
                        ]
                    ),
                ]

        return node.value, []

        # Detect cycle

        # waiting = node.get_edge("waiting")
        # assert len(waiting) <= 1, "Must have at most a single waiting edge."

        # if waiting:
        #     path = [n.id for n in waiting[0].traverse("waiting")]
        #     assert node.id not in path, f"Cycle detected: {node.id} -> {' -> '.join(path)}."

    def transition_coro_or_func(
        self,
        node: Node[V, E], func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Callable[[Result[DurablePromise] | Result[tuple[DurablePromise, Callback | Task | None]]], Sequence[Request]]:
        match isgeneratorfunction(func):
            case True:
                return self.transition(node, Coro(Coroutine(node.id, self.id, func(self.ctx(node.id, self.registry), *args, **kwargs)), Ok(None)))
            case False:
                return self.transition(node, Func(func, args, kwargs))

    def transition(
        self,
        node: Node[V, E],
        value: V | None = None,
    ) -> Callable[[Result[DurablePromise] | Result[tuple[DurablePromise, Callback | Task | None]]], Sequence[Request]]:
        def _transition(result: Result[DurablePromise] | Result[tuple[DurablePromise, Callback | Task | None]]) -> Sequence[Request]:
            match result:
                case Ok((promise, _)) | Ok(promise):
                    match promise.pending:
                        case True:
                            assert value, "Value must be provided unless completing a promise."
                            self.unblock(node, "waiting_p", Ok(AWT(node.id, self.id)))
                            node.transition(value)
                            return []
                        case False:
                            self.unblock(node, "waiting_p", Ok(AWT(node.id, self.id)))
                            self.unblock(node, "waiting_v", promise.result)
                            node.transition(Done(promise.result))
                            return []
                case Ko():
                    raise NotImplementedError
        return _transition

    def unblock(self, node: Node, edge: E, next: Result) -> None:
        for blocked in self.graph.filter(lambda n: n.has_edge(edge, node)):
            assert isinstance(blocked.value, Coro), "Blocked must be coro."
            blocked.rmv_edge(edge, node)
            blocked.transition(Coro(blocked.value.coro, next))

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

    def __repr__(self) -> str:
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
