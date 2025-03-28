from __future__ import annotations

import queue
import threading
import uuid
from typing import TYPE_CHECKING, Any

from resonate.models.commands import (
    CancelPromiseReq,
    CancelPromiseRes,
    CreateCallbackReq,
    CreateCallbackRes,
    CreatePromiseReq,
    CreatePromiseRes,
    CreatePromiseWithTaskReq,
    CreatePromiseWithTaskRes,
    Function,
    Invoke,
    Listen,
    Network,
    NetworkReq,
    Notify,
    Receive,
    RejectPromiseReq,
    RejectPromiseRes,
    ResolvePromiseReq,
    ResolvePromiseRes,
    Resume,
    Return,
)
from resonate.models.durable_promise import DurablePromise
from resonate.models.message import InvokeMesg, Mesg, NotifyMesg, ResumeMesg
from resonate.models.options import Options
from resonate.models.result import Ko, Ok, Result
from resonate.models.task import Task
from resonate.scheduler import Scheduler

if TYPE_CHECKING:
    from collections.abc import Callable
    from concurrent.futures import Future

    from resonate.models.commands import Command
    from resonate.models.message_source import MessageSource
    from resonate.models.store import Store
    from resonate.registry import Registry
    from resonate.resonate import Context


class Bridge:
    def __init__(
        self,
        ctx: Callable[[str], Context],
        store: Store,
        message_source: MessageSource,
        registry: Registry,
        gid: str = "default",
        pid: str | None = None,
        scheduler: Scheduler | None = None,
    ) -> None:
        self.store = store
        self.message_src = message_source
        self.registry = registry
        self.cq: queue.Queue[Command | tuple[Command, tuple[Future, Future]]] = queue.Queue()
        self.mq: queue.Queue[Mesg]

        self.pid = pid or uuid.uuid4().hex
        self.scheduler = scheduler or Scheduler(ctx=ctx, gid=gid, pid=self.pid)

        self.messages_thread = threading.Thread(target=self._process_msgs, name="bridge_msg_processor", daemon=True)
        self.bridge_thread = threading.Thread(target=self._process_cq, name="bridge_thread", daemon=True)

    def enqueue(self, cmd: Invoke | Listen, futures: tuple[Future, Future] | None = None) -> None:
        if futures is None:
            self.cq.put_nowait(cmd)
        else:
            self.cq.put_nowait((cmd, futures))

    def start(self) -> None:
        if not self.messages_thread.is_alive():
            self.messages_thread.start()

        if not self.bridge_thread.is_alive():
            self.bridge_thread.start()

    def _process_cq(self) -> None:
        while True:
            item = self.cq.get()
            requests = []
            match item:
                case (cmd, futures):
                    requests = self.scheduler._step(cmd, futures)  # noqa: SLF001
                case cmd:
                    requests = self.scheduler._step(cmd)  # noqa: SLF001

            for req in requests:
                match req:
                    case Network(id, cid, n_req):
                        # TODO(avillega): probaly put this in a try/except and handle errors better
                        self.cq.put_nowait(self._handle_network_request(id, cid, n_req))
                    case Function(id, cid, func):
                        self.cq.put_nowait(Return(id, cid, self._handle_function(func)))

    def _process_msgs(self) -> None:
        self.message_src.start(MesgQueueAdapter(self.mq), self.pid)

        def _invoke(root: DurablePromise, task: Task) -> Invoke:
            return Invoke(
                root.id,
                root.param.data["func"],
                self.registry.get(root.param.data["func"])[0],
                root.param.data["args"],
                root.param.data["kwargs"],
                Options(),
                (root, task),
            )

        while True:
            msg = self.mq.get()
            match msg:
                case InvokeMesg(task=task_msg):
                    task = Task.from_dict(self.store, task_msg.__dict__)
                    root, _ = self.store.tasks.claim(id=task.id, counter=task.counter, pid=self.pid, ttl=10000)
                    # TODO(avillega): Start hearthbeating!
                    self.cq.put_nowait(_invoke(root, task))

                case ResumeMesg(task=task_msg):
                    task = Task.from_dict(self.store, task_msg.__dict__)
                    root, leaf = self.store.tasks.claim(id=task.id, counter=task.counter, pid=self.pid, ttl=10000)
                    # TODO(avillega): Start hearthbeating!
                    assert leaf is not None, "leaf must not be None"
                    cmd = Resume(
                        id=leaf.id,
                        cid=root.id,
                        promise=leaf,
                        task=task,
                        invoke=_invoke(root, task),
                    )
                    self.cq.put_nowait(cmd)

                case NotifyMesg(promise=_promise):
                    # TODO(avillega): Create the durable promise properly
                    promise = DurablePromise.from_dict(self.store, _promise, _promise["param"], _promise["value"])
                    self.cq.put_nowait(Notify(promise.id, promise))

    def _handle_network_request(self, cmd_id: str, cid: str, req: NetworkReq) -> Command:
        match req:
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
                return Receive(cid, cmd_id, CreatePromiseRes(promise))

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
                return Receive(cid, cmd_id, CreatePromiseWithTaskRes(promise, task))

            case ResolvePromiseReq(id, ikey, strict, headers, data):
                promise = self.store.promises.resolve(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                return Receive(cid, cmd_id, ResolvePromiseRes(promise))

            case RejectPromiseReq(id, ikey, strict, headers, data):
                promise = self.store.promises.reject(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                return Receive(cid, cmd_id, RejectPromiseRes(promise))

            case CancelPromiseReq(id, ikey, strict, headers, data):
                promise = self.store.promises.cancel(
                    id=id,
                    ikey=ikey,
                    strict=strict,
                    headers=headers,
                    data=data,
                )
                return Receive(cid, cmd_id, CancelPromiseRes(promise))

            case CreateCallbackReq(id, promise_id, root_promise_id, timeout, recv):
                # TODO(avillega): Handle when callback is None which means to put a resume cmd right away.
                promise, callback = self.store.promises.callback(
                    id=id,
                    promise_id=promise_id,
                    root_promise_id=root_promise_id,
                    timeout=timeout,
                    recv=recv,
                )
                return Receive(cid, cmd_id, CreateCallbackRes(promise, callback))
            case _:
                raise NotImplementedError

    def _handle_function(self, func: Callable[[], Any]) -> Result:
        try:
            r = func()
            return Ok(r)
        except Exception as e:
            return Ko(e)


class MesgQueueAdapter:
    def __init__(self, mq: queue.Queue[Mesg]) -> None:
        self.mq = mq

    def enqueue(self, mesg: Mesg) -> None:
        self.mq.put(mesg)
