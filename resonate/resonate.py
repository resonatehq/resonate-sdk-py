from __future__ import annotations

import copy
import functools
import inspect
import logging
import random
import time
import uuid
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any, Concatenate, Literal, ParamSpec, TypeVar, TypeVarTuple, overload

from resonate.bridge import Bridge
from resonate.conventions import Base, Local, Remote, Sleep
from resonate.coroutine import LFC, LFI, RFC, RFI, Promise
from resonate.dependencies import Dependencies
from resonate.loggers import ContextLogger
from resonate.message_sources import LocalMessageSource, Poller
from resonate.models.handle import Handle
from resonate.models.message_source import MessageSource
from resonate.models.store import Store
from resonate.options import Options
from resonate.registry import Registry
from resonate.stores import LocalStore, RemoteStore

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Sequence

    from resonate.models.context import Info
    from resonate.models.encoder import Encoder
    from resonate.models.logger import Logger
    from resonate.models.retry_policy import RetryPolicy
    from resonate.models.store import PromiseStore, ScheduleStore

ALLOWED_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


class Resonate:
    """Resonate client."""

    def __init__(
        self,
        *,
        dependencies: Dependencies | None = None,
        group: str = "default",
        log_level: int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = logging.NOTSET,
        message_source: MessageSource | None = None,
        pid: str | None = None,
        registry: Registry | None = None,
        store: Store | None = None,
        ttl: int = 10,
        workers: int | None = None,
    ) -> None:
        """Create a Resonate client."""
        # dependencies
        if dependencies is not None and not isinstance(dependencies, Dependencies):
            msg = f"dependencies must be `Dependencies | None`, got {type(dependencies).__name__}"
            raise TypeError(msg)

        # group
        if not isinstance(group, str):
            msg = f"group must be `str`, got {type(group).__name__}"
            raise TypeError(msg)

        # log level
        if not isinstance(log_level, (int, str)):
            msg = f"log_level must be an int or a str, got {type(log_level).__name__}"
            raise TypeError(msg)
        if isinstance(log_level, str) and log_level not in ALLOWED_LOG_LEVELS:
            msg = f"string log_level must be one of {ALLOWED_LOG_LEVELS}, got {log_level!r}"
            raise ValueError(msg)

        # message source
        if message_source is not None and not isinstance(message_source, MessageSource):
            msg = f"message_source must be `MessageSource | None`, got {type(dependencies).__name__}"
            raise TypeError(msg)

        # pid
        if pid is not None and not isinstance(pid, str):
            msg = f"pid must be `str | None`, got {type(pid).__name__}"
            raise TypeError(msg)

        # registry
        if registry is not None and not isinstance(registry, Registry):
            msg = f"registry must be `Registry | None`, got {type(registry).__name__}"
            raise TypeError(msg)

        # store
        if store is not None and not isinstance(store, Store):
            msg = f"store must be `Store | None`, got {type(registry).__name__}"
            raise TypeError(msg)
        if isinstance(store, LocalStore) and not isinstance(message_source, LocalMessageSource):
            msg = "message source must be LocalMessageSource when store is LocalStore"
            raise TypeError(msg)
        if isinstance(store, RemoteStore) and isinstance(message_source, LocalMessageSource):
            msg = "message source must not be LocalMessageSource when store is RemoteStore"
            raise TypeError(msg)

        # ttl
        if not isinstance(ttl, int):
            msg = f"ttl must be `int`, got {type(ttl).__name__}"
            raise TypeError(msg)

        # store / message source
        if (store is None) != (message_source is None):
            msg = "store and message source must both be set or both be unset"
            raise ValueError(msg)

        self._dependencies = dependencies or Dependencies()
        self._group = group
        self._log_level = log_level
        self._opts = Options()
        self._pid = pid or uuid.uuid4().hex
        self._registry = registry or Registry()
        self._started = False
        self._ttl = ttl
        self._workers = workers

        if store and message_source:
            self._store = store
            self._message_source = message_source
        else:
            self._store = LocalStore()
            self._message_source = self._store.message_source(self._group, self._pid)

        self._bridge = Bridge(
            ctx=lambda id, cid, info: Context(id, cid, info, self._registry, self._dependencies, ContextLogger(cid, id, self._log_level)),
            pid=self._pid,
            ttl=self._ttl,
            opts=self._opts,
            workers=self._workers,
            unicast=self._message_source.unicast,
            anycast=self._message_source.anycast,
            registry=self._registry,
            store=self._store,
            message_source=self._message_source,
        )

    @classmethod
    def local(
        cls,
        dependencies: Dependencies | None = None,
        group: str = "default",
        log_level: int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = logging.INFO,
        pid: str | None = None,
        registry: Registry | None = None,
        ttl: int = 10,
        workers: int | None = None,
    ) -> Resonate:
        """Create a local Resonate client instance.

        This method configures a Resonate client that stores all state in memory,
        without external persistence or network I/O. The in-memory store implements
        the same API as the remote store, making it ideal for rapid development,
        local testing, and experimentation.

        Args:
            dependencies (Dependencies | None): Optional dependency injection container.
            group (str): Worker group name. Defaults to `"default"`.
            log_level (int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
                Logging verbosity level. Defaults to `logging.INFO`.
            pid (str | None): Optional process identifier for the worker.
            registry (Registry | None): Optional registry to manage in-memory objects.
            ttl (int): Time-to-live (in seconds) for claimed tasks. Defaults to `10`.
            workers (int | None): Optional number of worker threads or processes for task execution.

        Returns:
            Resonate: A configured local Resonate client instance.

        Example:
            ```python
            client = Resonate.local(ttl=30, log_level="DEBUG")
            client.run()
            ```

        """
        pid = pid or uuid.uuid4().hex
        store = LocalStore()

        return cls(
            pid=pid,
            ttl=ttl,
            group=group,
            registry=registry,
            dependencies=dependencies,
            log_level=log_level,
            store=store,
            message_source=store.message_source(group=group, id=pid),
            workers=workers,
        )

    @classmethod
    def remote(
        cls,
        auth: tuple[str, str] | None = None,
        dependencies: Dependencies | None = None,
        group: str = "default",
        host: str | None = None,
        log_level: int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = logging.INFO,
        message_source_port: str | None = None,
        pid: str | None = None,
        registry: Registry | None = None,
        store_port: str | None = None,
        ttl: int = 10,
        workers: int | None = None,
    ) -> Resonate:
        """Create a Resonate client with remote configuration.

        This method configures a Resonate client that persists state to a remote
        store, enabling durability, coordination, and recovery across distributed
        processes. The remote store implementation provides fault-tolerant scheduling,
        consistent state management, and reliable operation for production workloads.

        Args:
            auth (tuple[str, str] | None): Optional authentication credentials for
                connecting to the remote store. Expected format is `(username, password)`.
            dependencies (Dependencies | None): Optional dependency injection container.
            group (str): Client group name. Defaults to `"default"`.
            host (str | None): Host address of the remote store service.
            log_level (int | Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
                Logging verbosity level. Defaults to `logging.INFO`.
            message_source_port (str | None): Port used for message source communication.
            pid (str | None): Optional process identifier for the client.
            registry (Registry | None): Optional registry to manage remote object mappings.
            store_port (str | None): Port used for the remote store service.
            ttl (int): Time-to-live (in seconds) for claimed tasks. Defaults to `10`.
            workers (int | None): Optional number of worker threads or processes for task execution.

        Returns:
            Resonate: A configured Resonate client instance connected to a remote store.

        Example:
            ```python
            client = Resonate.remote(
                host="store.resonate.io",
                auth=("user", "password"),
                ttl=60,
                log_level="INFO",
            )
            client.run()
            ```

        """
        pid = pid or uuid.uuid4().hex

        return cls(
            pid=pid,
            ttl=ttl,
            group=group,
            registry=registry,
            dependencies=dependencies,
            log_level=log_level,
            store=RemoteStore(host=host, port=store_port, auth=auth),
            message_source=Poller(group=group, id=pid, host=host, port=message_source_port, auth=auth),
            workers=workers,
        )

    @property
    def promises(self) -> PromiseStore:
        """Promises low level client."""
        return self._store.promises

    @property
    def schedules(self) -> ScheduleStore:
        """Schedules low level client."""
        return self._store.schedules

    def start(self) -> None:
        """Explicitly start Resonate threads.

        This happens automatically when calling .run() or .rpc(),
        but is generally recommended for all workers that have
        functions registered with Resonate.
        """
        if not self._started:
            self._bridge.start()

    def stop(self) -> None:
        """Stop resonate."""
        self._started = False
        self._bridge.stop()

    def options(
        self,
        *,
        encoder: Encoder[Any, str | None] | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        tags: dict[str, str] | None = None,
        target: str | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Resonate:
        """Configure options for the function.

        - encoder: Configure your own data encoder.
        - idempotency_key: Define the idempotency key invocation or a function that
            receives the promise id and creates an idempotency key.
        - retry_policy: Define the retry policy: exponential | constant | linear | never
        - tags: Add custom tags to the durable promise representing the invocation.
        - target: Target to distribute the invocation.
        - timeout: Number of seconds before the invocation timesout.
        - version: Version of the function to invoke.
        """
        copied: Resonate = copy.copy(self)
        copied._opts = self._opts.merge(
            encoder=encoder,
            idempotency_key=idempotency_key,
            retry_policy=retry_policy,
            tags=tags,
            target=target,
            timeout=timeout,
            version=version,
        )

        return copied

    @overload
    def register[**P, R](
        self,
        func: Callable[Concatenate[Context, P], R],
        /,
        *,
        name: str | None = None,
        version: int = 1,
    ) -> Function[P, R]: ...
    @overload
    def register[**P, R](
        self,
        *,
        name: str | None = None,
        version: int = 1,
    ) -> Callable[[Callable[Concatenate[Context, P], R]], Function[P, R]]: ...
    def register[**P, R](
        self,
        *args: Callable[Concatenate[Context, P], R] | None,
        name: str | None = None,
        version: int = 1,
    ) -> Function[P, R] | Callable[[Callable[Concatenate[Context, P], R]], Function[P, R]]:
        """Register a function with Resonate for execution and version control.

        This method makes a function available for top-level execution under a
        specific name and version. It can be used either directly by passing the
        target function, or as a decorator for more concise registration.

        When used as a decorator without arguments, the function is registered
        automatically using its own name and the default version. Providing explicit
        `name` or `version` values allows precise control over function identification
        and versioning for repeatable, distributed invocation.

        Args:
            *args (Callable[Concatenate[Context, P], R] | None): The function to register,
                        or `None` when used as a decorator.
            name (str | None): Optional explicit name for the registered function.
                Defaults to the function's own name.
            version (int): Version number for the registered function. Defaults to `1`.

        Returns:
            Function[P, R] | Callable[[Callable[Concatenate[Context, P], R]], Function[P, R]]:
                If called directly, returns a `Function` wrapper for the registered
                function. If used as a decorator, returns a decorator that registers
                the target function upon definition.

        Example:
            Registering a function directly:
            ```python
            def greet(ctx: Context, name: str) -> str:
                return f"Hello, {name}!"

            client.register(greet, name="greet_user", version=2)
            ```

            Using as a decorator:
            ```python
            @resonate.register(name="process_data")
            def process(ctx: Context, data: dict) -> dict:
                ...
            ```

        """
        if name is not None and not isinstance(name, str):
            msg = f"name must be `str | None`, got {type(name).__name__}"
            raise TypeError(msg)

        if not isinstance(version, int):
            msg = f"version must be `int`, got {type(version).__name__}"
            raise TypeError(msg)

        def wrapper(func: Callable[..., Any]) -> Function[P, R]:
            if not callable(func):
                msg = "func must be Callable"
                raise TypeError(msg)

            if isinstance(func, Function):
                func = func.func

            self._registry.add(func, name, version)
            return Function(self, name or func.__name__, func, self._opts.merge(version=version))

        if args and args[0] is not None:
            return wrapper(args[0])

        return wrapper

    @overload
    def run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...
    @overload
    def run(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...
    def run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Run a registered function through Resonate and waits for its result.

        This method executes a registered function by its identifier or reference
        and returns the computed result. If a durable promise with the same `id`
        already exists, Resonate will reuse the existing result or subscribe to its
        completion rather than executing the function again. This ensures
        idempotent and deterministic behavior across distributed executions.

        Resonate guarantees that duplicate executions for the same `id` are prevented.

        Notes:
            - The target function must be registered with Resonate.
            - All function arguments and keyword arguments must be serializable.
            - This is a **blocking** operation that waits for completion.

        Args:
            id (str): Unique identifier for this function invocation. Used to
                deduplicate and resume durable results.
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str):
                The function to execute, either as a callable or the registered name of the function.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            R: The result of the executed function.

        Example:
            Running a function directly:
            ```python
            result = client.run("task-42", my_function, 10, flag=True)
            ```

            Running by registered name:
            ```python
            result = client.run("task-42", "process_data", records)
            ```

        """
        return self.begin_run(id, func, *args, **kwargs).result()

    @overload
    def begin_run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]: ...
    @overload
    def begin_run(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Handle[Any]: ...
    def begin_run[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        """Begin execution of a registered function through Resonate.

        This method starts the execution of a registered function and returns a
        `Handle` that can be used to track progress, await completion, or retrieve
        results later. If a durable promise with the same `id` already exists,
        Resonate will reuse the existing execution state or subscribe to its result
        rather than starting a new run. This ensures idempotent and fault-tolerant
        execution across distributed systems.

        Resonate guarantees that duplicate executions for the same `id` are prevented.

        Notes:
            - The target function must be registered with Resonate.
            - All function arguments and keyword arguments must be serializable.
            - This operation is **non-blocking**; it returns immediately with a handle.

        Args:
            id (str): Unique identifier for this function invocation. Used to
                deduplicate and resume durable results.
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str):
                The function to execute, either as a callable or the registered name of the function.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            Handle[R]: A handle object that can be used to monitor, await, or
            retrieve the function's result.

        Example:
            Starting a function run asynchronously:
            ```python
            handle = resonate.begin_run("job-123", process_data, records)
            # Do other work...
            result = handle.result()
            ```

            Using a registered function name:
            ```python
            handle = resonate.begin_run("job-123", "aggregate_metrics", dataset)
            ```

        """
        # id
        if not isinstance(id, str):
            msg = f"id must be `str`, got {type(id).__name__}"
            raise TypeError(msg)
        # func
        if not (callable(func) or isinstance(func, str)):
            msg = f"func must be `Callable | str`, got {type(func).__name__}"
            raise TypeError(msg)
        # tuple
        if not isinstance(args, tuple):
            msg = f"args must be `tuple`, got {type(args).__name__}"
            raise TypeError(args)
        # dict
        if not isinstance(kwargs, dict):
            msg = f"kwargs must be `dict`, got {type(kwargs).__name__}"
            raise TypeError(kwargs)

        self.start()
        future = Future[R]()

        name, func, version = self._registry.get(func, self._opts.version)
        opts = self._opts.merge(version=version)

        self._bridge.run(Remote(id, id, id, name, args, kwargs, opts), func, args, kwargs, opts, future)
        return Handle(id, future, self._bridge.subscribe)

    @overload
    def rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...
    @overload
    def rpc(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...
    def rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        """Execute a registered function remotely through Resonate and waits for its result.

        This method invokes a registered function on a remote Resonate worker and
        blocks until the result is available. If a durable promise with the same `id`
        already exists, Resonate will reuse the existing execution or subscribe to its
        completion instead of executing it again, ensuring idempotent and consistent
        remote execution.

        Resonate guarantees that duplicate remote executions for the same `id` are prevented.

        Notes:
            - The target function must be registered with Resonate.
            - All function arguments and keyword arguments must be serializable.
            - This is a **blocking** remote operation that waits for completion.

        Args:
            id (str): Unique identifier for this function invocation. Used to
                deduplicate and resume durable remote results.
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str):
                The function to execute remotely, either as a callable or the
                registered name of the function.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            R: The result of the remote function execution.

        Example:
            Running a function remotely by reference:
            ```python
            result = resonate.rpc("remote-task-7", my_function, data)
            ```

            Running a registered function by name:
            ```python
            result = resonate.rpc("remote-task-7", "process_order", order_id)
            ```

        """
        return self.begin_rpc(id, func, *args, **kwargs).result()

    @overload
    def begin_rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]: ...
    @overload
    def begin_rpc(
        self,
        id: str,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> Handle[Any]: ...
    def begin_rpc[**P, R](
        self,
        id: str,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Handle[R]:
        """Begin remote execution of a registered function through Resonate.

        This method initiates a remote execution on a Resonate worker and returns a
        `Handle` that can be used to track progress, await completion, or retrieve
        the result later. If a durable promise with the same `id` already exists,
        Resonate will reuse the existing execution state or subscribe to its result
        instead of starting a new one. This ensures consistent, idempotent, and
        fault-tolerant behavior in distributed environments.

        Resonate guarantees that duplicate remote executions for the same `id` are prevented.

        Notes:
            - The target function must be registered with Resonate.
            - All function arguments and keyword arguments must be serializable.
            - This operation is **non-blocking** and returns immediately with a handle.

        Args:
            id (str): Unique identifier for this remote invocation. Used to
                deduplicate and resume durable results.
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R] | str):
                The function to execute remotely, either as a callable or the
                registered name of the function.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            Handle[R]: A handle object representing the remote execution,
            which can be used to monitor, await, or retrieve results.

        Example:
            Starting a remote run asynchronously:
            ```python
            handle = resonate.begin_rpc("job-987", process_data, records)
            # Do other work...
            result = handle.result()
            ```

            Using a registered function name:
            ```python
            handle = resonate.begin_rpc("job-987", "aggregate_metrics", dataset)
            ```

        """
        # id
        if not isinstance(id, str):
            msg = f"id must be `str`, got {type(id).__name__}"
            raise TypeError(msg)
        # func
        if not (callable(func) or isinstance(func, str)):
            msg = f"func must be `Callable | str`, got {type(func).__name__}"
            raise TypeError(msg)
        # tuple
        if not isinstance(args, tuple):
            msg = f"args must be `tuple`, got {type(args).__name__}"
            raise TypeError(args)
        # dict
        if not isinstance(kwargs, dict):
            msg = f"kwargs must be `dict`, got {type(kwargs).__name__}"
            raise TypeError(kwargs)

        self.start()
        future = Future[R]()

        if isinstance(func, str):
            name = func
            version = self._registry.latest(func)
        else:
            name, _, version = self._registry.get(func, self._opts.version)

        opts = self._opts.merge(version=version)
        self._bridge.rpc(Remote(id, id, id, name, args, kwargs, opts), opts, future)
        return Handle(id, future, self._bridge.subscribe)

    def get(self, id: str) -> Handle[Any]:
        """Retrieve or subscribe to an existing execution by ID.

        This method attaches to an existing durable promise identified by `id`.
        If the associated execution is still in progress, it returns a `Handle`
        that can be used to await or observe its completion. If the execution has
        already completed, the handle is resolved immediately with the stored result.

        Notes:
            - A durable promise with the given `id` must already exist.
            - This operation is **non-blocking**; awaiting the handle will block only if the execution is still running.

        Args:
            id (str): Unique identifier of the target execution or durable promise.

        Returns:
            Handle[Any]: A handle representing the existing execution, which can
            be used to await or retrieve the result.

        Example:
            ```python
            handle = resonate.get("job-42")
            result = handle.result()
            ```

        """
        # id
        if not isinstance(id, str):
            msg = f"id must be `str`, got {type(id).__name__}"
            raise TypeError(msg)

        self.start()
        future = Future()

        self._bridge.get(id, self._opts, future)
        return Handle(id, future, self._bridge.subscribe)

    def set_dependency(self, name: str, obj: Any) -> None:
        """Register a named dependency for use within function execution contexts.

        This method stores a dependency object that will be made available to all
        registered functions through their `Context`. Dependencies are typically
        shared resources such as database clients, configuration objects, or
        service interfaces that functions can access during execution.

        Args:
            name (str): The name under which the dependency is registered. This
                name is used to retrieve the dependency within a function's `Context`.
            obj (Any): The dependency instance to register.

        Returns:
            None

        Example:
            ```python
            client.set_dependency("db", DatabaseClient())

            @client.register()
            def fetch_user(ctx: Context, user_id: str):
                db = ctx.dependencies["db"]
                return db.get_user(user_id)
            ```

        """
        # name
        if not isinstance(name, str):
            msg = f"name must be `str`, got {type(name).__name__}"
            raise TypeError(msg)

        self._dependencies.add(name, obj)


class Context:
    def __init__(self, id: str, cid: str, info: Info, registry: Registry, dependencies: Dependencies, logger: Logger) -> None:
        self._id = id
        self._cid = cid
        self._info = info
        self._registry = registry
        self._dependencies = dependencies
        self._logger = logger
        self._random = Random(self)
        self._time = Time(self)
        self._counter = 0

    def __repr__(self) -> str:
        return f"Context(id={self._id}, cid={self._cid}, info={self._info})"

    @property
    def id(self) -> str:
        """Id for the current execution."""
        return self._id

    @property
    def info(self) -> Info:
        """Information for the current execution."""
        return self._info

    @property
    def logger(self) -> Logger:
        return self._logger

    @property
    def random(self) -> Random:
        """Deterministic random methods."""
        return self._random

    @property
    def time(self) -> Time:
        """Deterministic time methods."""
        return self._time

    def get_dependency[T](self, key: str, default: T = None) -> Any | T:
        """Retrieve a registered dependency by name.

        This method returns the dependency object registered under the given key.
        If no dependency exists for that key, the provided `default` value is returned.
        A `TypeError` is raised if `key` is not a string.

        Args:
            key (str): The name of the dependency to retrieve.
            default (T, optional): The value to return if no dependency is found.
                Defaults to `None`.

        Returns:
            Any | T: The registered dependency if it exists, otherwise the
            specified `default` value.

        Raises:
            TypeError: If `key` is not a string.

        Example:
            ```python
            db = client.get_dependency("db")
            cache = client.get_dependency("cache", default=NullCache())
            ```

        """
        if not isinstance(key, str):
            msg = f"key must be `str`, got {type(key).__name__}"
            raise TypeError(msg)

        return self._dependencies.get(key, default)

    def run[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC[R]:
        """Schedules a function for local execution and awaits its result.

        This method executes the given function within the current process context.
        It serves as an alias for `ctx.lfc`, providing a simplified interface for
        scheduling local, durable function calls. By default, execution is durable,
        but non-durable behavior can be configured if desired.

        Args:
            func (Callable[Concatenate[Context, P], Generator[Any, Any, R] | R]):
                The function to execute locally.
            *args (P.args): Positional arguments to pass to the function.
            **kwargs (P.kwargs): Keyword arguments to pass to the function.

        Returns:
            LFC[R]: A local function call handle representing the scheduled execution
            and its eventual result.

        Example:
            ```python
            def compute(ctx: Context, x: int, y: int) -> int:
                return x + y

            result = client.run(compute, 5, 10)
            print(result.await_result())  # 15
            ```

        """
        return self.lfc(func, *args, **kwargs)

    def begin_run[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI[R]:
        """Schedule a function for local execution. (Alias for ctx.lfi).

        The function is executed in the current process, and the returned promise
        can be awaited for the final result.

        By default, execution is durable; non durable behavior can be configured if needed.
        """
        return self.lfi(func, *args, **kwargs)

    @overload
    def rpc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC[R]: ...
    @overload
    def rpc(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC: ...
    def rpc(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC:
        """Schedule a function for remote execution and await its result. (Alias for ctx.rfc).

        The function is scheduled on the global event loop and potentially executed
        in a different process.

        - Function must be registered
        - Function args and kwargs must be serializable
        """
        return self.rfc(func, *args, **kwargs)

    @overload
    def begin_rpc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI[R]: ...
    @overload
    def begin_rpc(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI: ...
    def begin_rpc(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI:
        """Schedule a function for remote execution. (Alias for ctx.rfi).

        The function is scheduled on the global event loop and potentially executed
        in a different process, the returned promise can be awaited for the final
        result.

        - Function must be registered
        - Function args and kwargs must be serializable
        """
        return self.rfi(func, *args, **kwargs)

    def lfi[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFI[R]:
        """Schedule a function for local execution.

        The function is executed in the current process, and the returned promise
        can be awaited for the final result.

        By default, execution is durable; non durable behavior can be configured if needed.
        """
        if isinstance(func, Function):
            func = func.func

        if not inspect.isfunction(func):
            msg = "provided callable must be a function"
            raise ValueError(msg)

        opts = Options(version=self._registry.latest(func))
        return LFI(Local(self._next(), self._cid, self._id, opts), func, args, kwargs, opts)

    def lfc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> LFC[R]:
        """Schedule a function for local execution and await its result.

        The function is executed in the current process.

        By default, execution is durable; non durable behavior can be configured if needed.
        """
        if isinstance(func, Function):
            func = func.func

        if not inspect.isfunction(func):
            msg = "provided callable must be a function"
            raise ValueError(msg)

        opts = Options(version=self._registry.latest(func))
        return LFC(Local(self._next(), self._cid, self._id, opts), func, args, kwargs, opts)

    @overload
    def rfi[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI[R]: ...
    @overload
    def rfi(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI: ...
    def rfi(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI:
        """Schedule a function for remote execution.

        The function is scheduled on the global event loop and potentially executed
        in a different process, the returned promise can be awaited for the final
        result.

        - Function must be registered
        - Function args and kwargs must be serializable
        """
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFI(Remote(self._next(), self._cid, self._id, name, args, kwargs, Options(version=version)))

    @overload
    def rfc[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFC[R]: ...
    @overload
    def rfc(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC: ...
    def rfc(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFC:
        """Schedule a function for remote execution and await its result.

        The function is scheduled on the global event loop and potentially executed
        in a different process.

        - Function must be registered
        - Function args and kwargs must be serializable
        """
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFC(Remote(self._next(), self._cid, self._id, name, args, kwargs, Options(version=version)))

    @overload
    def detached[**P, R](
        self,
        func: Callable[Concatenate[Context, P], Generator[Any, Any, R] | R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> RFI[R]: ...
    @overload
    def detached(
        self,
        func: str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI: ...
    def detached(
        self,
        func: Callable | str,
        *args: Any,
        **kwargs: Any,
    ) -> RFI:
        """Schedule a function for remote (detached) execution.

        The function is scheduled on the global event loop and potentially executed
        in a different process, and the returned promise can be awaited for the final
        result.

        - Function must be registered
        - Function args and kwargs must be serializable
        """
        name, _, version = (func, None, self._registry.latest(func)) if isinstance(func, str) else self._registry.get(func)
        return RFI(Remote(self._next(), self._cid, self._id, name, args, kwargs, Options(version=version)), mode="detached")

    @overload
    def typesafe[T](self, cmd: LFI[T] | RFI[T]) -> Generator[LFI[T] | RFI[T], Promise[T], Promise[T]]: ...
    @overload
    def typesafe[T](self, cmd: LFC[T] | RFC[T] | Promise[T]) -> Generator[LFC[T] | RFC[T] | Promise[T], T, T]: ...
    def typesafe(self, cmd: LFI | RFI | LFC | RFC | Promise) -> Generator[LFI | RFI | LFC | RFC | Promise, Any, Any]:
        """Optionally provide type safety for your coroutine definition."""
        return (yield cmd)

    def sleep(self, secs: float) -> RFC[None]:
        """Sleep inside a function.

        There is no limit to how long you can sleep.
        The sleep method accepts a float value in seconds.
        """
        if not isinstance(secs, int | float):
            msg = f"secs must be `float`, got {type(secs).__name__}"
            raise TypeError(msg)

        return RFC(Sleep(self._next(), secs))

    def promise(
        self,
        *,
        id: str | None = None,
        timeout: float | None = None,
        idempotency_key: str | None = None,
        data: Any = None,
        tags: dict[str, str] | None = None,
    ) -> RFI:
        """Get or create a promise that can be awaited on.

        If no ID is provided, one is generated and a new promise is created. If an ID
        is provided and a promise already exists with that ID, then the existing promise
        is returned (if the idempotency keys match).

        This is very useful for HITL (Human-In-The-Loop) use cases where you want to block
        progress until a human has taken an action or provided data. It works well in
        conjunction with Resonate's .promise.resolve() method.
        """
        if id is not None and not isinstance(id, str):
            msg = f"id must be `str | None`, got {type(id).__name__}"
            raise TypeError(msg)

        if timeout is not None and isinstance(timeout, int | float):
            msg = f"timeout must be `float`, got {type(timeout).__name__}"
            raise TypeError(msg)

        if idempotency_key is not None and not isinstance(idempotency_key, str):
            msg = f"idempotency_key must be `str | None`, got {type(idempotency_key).__name__}"
            raise TypeError(msg)

        if tags is not None and not isinstance(tags, dict):
            msg = f"tags must be `dict | None`, got {type(tags).__name__}"
            raise TypeError(tags)

        default_id = self._next()
        id = id or default_id

        return RFI(
            Base(
                id,
                timeout or 31536000,
                idempotency_key or id,
                data,
                tags,
            ),
        )

    def _next(self) -> str:
        self._counter += 1
        return f"{self._id}.{self._counter}"


class Time:
    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def strftime(self, format: str) -> LFC[str]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:time", time).strftime(format))

    def time(self) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:time", time).time())


class Random:
    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx

    def betavariate(self, alpha: float, beta: float) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).betavariate(alpha, beta))

    def choice[T](self, seq: Sequence[T]) -> LFC[T]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).choice(seq))

    def expovariate(self, lambd: float = 1) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).expovariate(lambd))

    def getrandbits(self, k: int) -> LFC[int]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).getrandbits(k))

    def randint(self, a: int, b: int) -> LFC[int]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).randint(a, b))

    def random(self) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).random())

    def randrange(self, start: int, stop: int | None = None, step: int = 1) -> LFC[int]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).randrange(start, stop, step))

    def triangular(self, low: float = 0, high: float = 1, mode: float | None = None) -> LFC[float]:
        return self._ctx.lfc(lambda _: self._ctx.get_dependency("resonate:random", random).triangular(low, high, mode))


class Function[**P, R]:
    __name__: str
    __type_params__: tuple[TypeVar | ParamSpec | TypeVarTuple, ...] = ()

    def __init__(self, resonate: Resonate, name: str, func: Callable[Concatenate[Context, P], R], opts: Options) -> None:
        # updates the following attributes:
        # __module__
        # __name__
        # __qualname__
        # __doc__
        # __annotations__
        # __type_params__
        # __dict__
        functools.update_wrapper(self, func)

        self._resonate = resonate
        self._name = name
        self._func = func
        self._opts = opts

    @property
    def name(self) -> str:
        return self._name

    @property
    def func(self) -> Callable[Concatenate[Context, P], R]:
        return self._func

    def __call__(self, ctx: Context, *args: P.args, **kwargs: P.kwargs) -> R:
        return self._func(ctx, *args, **kwargs)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Function):
            return self._func == other._func
        if callable(other):
            return self._func == other
        return NotImplemented

    def __hash__(self) -> int:
        # Helpful for ensuring proper registry lookups, a function and an instance of Function
        # that wraps the same function has the same identity.
        return self._func.__hash__()

    def options(
        self,
        *,
        encoder: Encoder[Any, str | None] | None = None,
        idempotency_key: str | Callable[[str], str] | None = None,
        retry_policy: RetryPolicy | Callable[[Callable], RetryPolicy] | None = None,
        tags: dict[str, str] | None = None,
        target: str | None = None,
        timeout: float | None = None,
        version: int | None = None,
    ) -> Function[P, R]:
        """Configure options for the function.

        - encoder: Configure your own data encoder.
        - idempotency_key: Define the idempotency key invocation or a function that
            receives the promise id and creates an idempotency key.
        - retry_policy: Define the retry policy exponential | constant | linear | never
        - tags: Add custom tags to the durable promise representing the invocation.
        - target: Target to distribute the invocation.
        - timeout: Number of seconds before the invocation timesout.
        - version: Version of the function to invoke.
        """
        self._opts = self._opts.merge(
            encoder=encoder,
            idempotency_key=idempotency_key,
            retry_policy=retry_policy,
            tags=tags,
            target=target,
            timeout=timeout,
            version=version,
        )
        return self

    def run[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> T:
        """Run a function with Resonate and wait for the result.

        If a durable promise with the same id already exists, the method
        will subscribe to its result or return the value immediately if
        the promise has been completed.

        Resonate will prevent duplicate executions for the same id.

        - Function must be registered
        - Function args and kwargs must be serializable
        - This is a blocking operation
        """
        return self.begin_run(id, *args, **kwargs).result()

    def begin_run[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[T]:
        """Run a function with Resonate.

        If a durable promise with the same id already exists, the method
        will subscribe to its result or return the value immediately if
        the promise has been completed.

        Resonate will prevent duplicate executions for the same id.

        - Function must be registered
        - Function args and kwargs must be serializable
        """
        resonate = self._resonate.options(
            encoder=self._opts.encoder,
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            tags=self._opts.tags,
            target=self._opts.target,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.begin_run(id, self._func, *args, **kwargs)

    def rpc[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> T:
        """Run a function with Resonate remotely and wait for the result.

        If a durable promise with the same id already exists, the method
        will subscribe to its result or return the value immediately if
        the promise has been completed.

        Resonate will prevent duplicate executions for the same id.

        - Function must be registered
        - Function args and kwargs must be serializable
        - This is a blocking operation
        """
        return self.begin_rpc(id, *args, **kwargs).result()

    def begin_rpc[T](self: Function[P, Generator[Any, Any, T] | T], id: str, *args: P.args, **kwargs: P.kwargs) -> Handle[T]:
        """Run a function with Resonate remotely.

        If a durable promise with the same id already exists, the method
        will subscribe to its result or return the value immediately if
        the promise has been completed.

        Resonate will prevent duplicate executions for the same id.

        - Function must be registered
        - Function args and kwargs must be serializable
        """
        resonate = self._resonate.options(
            encoder=self._opts.encoder,
            idempotency_key=self._opts.idempotency_key,
            retry_policy=self._opts.retry_policy,
            tags=self._opts.tags,
            target=self._opts.target,
            timeout=self._opts.timeout,
            version=self._opts.version,
        )
        return resonate.begin_rpc(id, self._func, *args, **kwargs)
