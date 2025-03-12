from concurrent.futures import Future
from typing import Callable, Generator
import pytest
from typing_extensions import Any
from resonate.models.commands import Invoke
from resonate.registry import Registry
from resonate import Resonate, Context
from unittest.mock import MagicMock

def foo(ctx: Context, a: int, b: int) -> int: ...
def bar(a: int, b: int) -> int: ...
def baz(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]: ...

# Helper to set up mocked scheduler with unblocking side effect
def setup_mock_scheduler(resonate: Resonate) -> MagicMock:
    mock_scheduler = MagicMock()
    resonate._scheduler = mock_scheduler

    def enqueue_side_effect(*args, **kwargs):
        if futures := kwargs.get("futures"):
            futures[0].set_result(None)  # Unblock future

    mock_scheduler.enqueue.side_effect = enqueue_side_effect
    return mock_scheduler

# Helper to validate Invoke parameters
def assert_invoke_cmd(
    mock_scheduler: MagicMock,
    func: Callable,
    expected_func: Callable | None,
    expected_args: tuple,
    expected_kwargs: dict,
    expected_opts: dict
) -> None:
    mock_scheduler.enqueue.assert_called_once()
    args, kwargs = mock_scheduler.enqueue.call_args
    invoke = args[0]

    assert invoke.id == func.__name__
    assert invoke.name == func.__name__
    assert invoke.func is expected_func
    assert invoke.args == expected_args
    assert invoke.kwargs == expected_kwargs
    assert invoke.opts == expected_opts

# Parametrized tests
@pytest.mark.parametrize("func", [foo, bar, baz])
def test_run_and_register_as_method_call(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = setup_mock_scheduler(resonate)

    resonate.register(func)
    resonate.run(func.__name__, func, a=1, b=2)

    assert_invoke_cmd(mock_scheduler, func, func, tuple(), {"a": 1, "b": 2}, {})

@pytest.mark.parametrize("func", [foo, bar, baz])
def test_rpc_and_register_as_method_call(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = setup_mock_scheduler(resonate)

    resonate.register(func)
    resonate.rpc(func.__name__, func, 1, 2)

    assert_invoke_cmd(mock_scheduler, func, None, (1, 2), {}, {"target":"default"})

@pytest.mark.parametrize("func", [foo, bar, baz])
def test_run_and_register_as_decorator(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = setup_mock_scheduler(resonate)

    registered_func = resonate.register(func)
    registered_func.run(func.__name__, a=1, b=2)

    assert_invoke_cmd(mock_scheduler, func, func, tuple(), {"a": 1, "b": 2}, {})

@pytest.mark.parametrize("func", [foo, bar, baz])
def test_rpc_and_register_as_decorator(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = setup_mock_scheduler(resonate)

    registered_func = resonate.register(func)
    registered_func.rpc(func.__name__, 1, 2)

    assert_invoke_cmd(mock_scheduler, func, None, (1, 2), {}, {"target": "default"})


@pytest.mark.parametrize("func", [foo, bar, baz])
def test_run_and_register_as_decorator_with_options(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = setup_mock_scheduler(resonate)

    registered_func = resonate.register(func)
    registered_func.options(send_to="not-default").run(func.__name__, a=1, b=2)

    assert_invoke_cmd(mock_scheduler, func, func, tuple(), {"a": 1, "b": 2}, {})

@pytest.mark.parametrize("func", [foo, bar, baz])
def test_rpc_and_register_as_decorator_with_options(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = setup_mock_scheduler(resonate)

    registered_func = resonate.register(func)
    registered_func.options(send_to="not-default").rpc(func.__name__, 1, 2)

    assert_invoke_cmd(mock_scheduler, func, None, (1, 2), {}, {"target":"not-default"})

@pytest.mark.parametrize("func", [foo, bar, baz])
def test_run_and_register_as_method_call_with_options(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = setup_mock_scheduler(resonate)

    resonate.register(func)
    resonate.options(send_to="not-default").run(func.__name__, func, a=1, b=2)

    assert_invoke_cmd(mock_scheduler, func, func, tuple(), {"a": 1, "b": 2}, {})

@pytest.mark.parametrize("func", [foo, bar, baz])
def test_rpc_and_register_as_method_call_with_options(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = setup_mock_scheduler(resonate)

    resonate.register(func)
    resonate.options(send_to="not-default").rpc(func.__name__, func, 1, 2)

    assert_invoke_cmd(mock_scheduler, func, None, (1, 2), {}, {"target":"not-default"})
