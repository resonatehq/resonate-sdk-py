from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Self

import msgspec

from resonate import now_ms
from resonate.info import Info
from resonate.types import PromiseCreateReq, TaskData, Value

if TYPE_CHECKING:
    import asyncio

    from resonate import DependencyMap
    from resonate.effects import Effects


DEFAULT_TIMEOUT: Final[timedelta] = timedelta(days=1)

TargetResolver = Callable[[str | None], str]


class SpawnedLocal(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    handle: asyncio.Task[Any]


class Context(msgspec.Struct, frozen=True, kw_only=True):
    id: str
    origin_id: str
    branch_id: str
    parent_id: str
    func_name: str
    timeout_at: int
    seq: int
    effects: Effects
    target_resolver: TargetResolver
    spawned_remote: list[str]
    spawned_locals: list[SpawnedLocal]
    deps: DependencyMap

    @classmethod
    def root(
        cls,
        id: str,
        timeout_at: int,
        func_name: str,
        effects: Effects,
        target_resolver: TargetResolver,
        deps: DependencyMap,
    ) -> Self:
        return cls(
            id=id,
            origin_id=id,
            branch_id=id,
            parent_id="",
            func_name=func_name,
            timeout_at=timeout_at,
            seq=0,
            effects=effects,
            target_resolver=target_resolver,
            spawned_locals=[],
            spawned_remote=[],
            deps=deps,
        )

    def child(self, id: str, func_name: str, timeout_at: int) -> Context:
        assert self.timeout_at >= timeout_at, (
            "child timeout_at must be bounded by parents timeout_at"
        )
        return Context(
            id=id,
            origin_id=self.origin_id,
            branch_id=id,
            parent_id=self.parent_id,
            func_name=func_name,
            timeout_at=timeout_at,
            seq=0,
            effects=self.effects,
            target_resolver=self.target_resolver,
            spawned_locals=[],
            spawned_remote=[],
            deps=self.deps,
        )

    def child_info(self, id: str, func_name: str, timeout_at: int) -> Info:
        assert self.timeout_at >= timeout_at, (
            "child timeout_at must be bounded by parents timeout_at"
        )
        return Info(
            id=id,
            parent_id=self.id,
            origin_id=self.origin_id,
            branch_id=id,
            timeout_at=timeout_at,
            func_name=func_name,
            tags={},
            deps=self.deps,
        )

    def get_dependency[T](self, type: type[T]) -> T:
        return self.deps.get(type)

    def next_id(self) -> str:
        self.seq += 1
        return f"{self.id}.{self.seq}"

    def child_timeout(self, requested: timedelta | None) -> int:
        now = now_ms()
        timeout = requested if requested is not None else DEFAULT_TIMEOUT
        return min(now + int(timeout.total_seconds() * 1000), self.timeout_at)

    def info(self) -> Info:
        return Info(
            id=self.id,
            parent_id=self.parent_id,
            origin_id=self.origin_id,
            branch_id=self.branch_id,
            timeout_at=self.timeout_at,
            func_name=self.func_name,
            tags={},
            deps=self.deps,
        )

    def local_create_req(
        self, id: str, args: Any, timeout: timedelta | None
    ) -> PromiseCreateReq:
        return PromiseCreateReq(
            id=id,
            timeout_at=self.child_timeout(timeout),
            param=Value.from_serializable(args),
            tags={
                "resonate:scope": "local",
                "resonate:branch": self.branch_id,
                "resonate:parent": self.id,
                "resonate:origin": self.origin_id,
            },
        )

    def remote_create_req(
        self,
        id: str,
        func_name: str,
        args: Any,
        timeout: timedelta | None,
        target_override: str | None,
    ) -> PromiseCreateReq:
        return PromiseCreateReq(
            id=id,
            timeout_at=self.child_timeout(timeout),
            param=TaskData.into_value(func=func_name, args=args),
            tags={
                "resonate:scope": "global",
                "resonate:target": self.target_resolver(target_override),
                "resonate:branch": id,
                "resonate:parent": self.id,
                "resonate:origin": self.origin_id,
            },
        )

    def promise_create_req(
        self, id: str, timeout: timedelta | None
    ) -> PromiseCreateReq:
        return PromiseCreateReq(
            id=id,
            timeout_at=self.child_timeout(timeout),
            param=Value(),
            tags={
                "resonate:scope": "global",
                "resonate:branch": id,
                "resonate:parent": self.id,
                "resonate:origin": self.origin_id,
            },
        )

    def sleep_create_req(self, id: str, duration: timedelta) -> PromiseCreateReq:
        return PromiseCreateReq(
            id=id,
            timeout_at=self.child_timeout(duration),
            param=Value(),
            tags={
                "resonate:scope": "global",
                "resonate:branch": id,
                "resonate:parent": self.id,
                "resonate:origin": self.origin_id,
                "resonate:timer": "true",
            },
        )
