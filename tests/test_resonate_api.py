from concurrent.futures import Future
from typing import Callable, Generator
import pytest
from typing_extensions import Any
from resonate.registry import Registry
from resonate import Resonate, Context
from unittest.mock import MagicMock

def foo(ctx: Context, a: int, b: int) -> int:...
def bar(a: int, b: int) -> int:...
def baz(ctx: Context, a: int, b: int) -> Generator[Any, Any, int]:...


@pytest.mark.parametrize("func", [foo, bar, baz])
def test_run_and_register_as_method_call(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = MagicMock()
    resonate._scheduler = mock_scheduler

    # Define a side effect to set the result on `fp` to unblock the run method
    def enqueue_side_effect(*args, **kwargs):
        futures = kwargs.get("futures", ())
        if futures:
            # Set a result on the first future (`fp`) to unblock
            futures[0].set_result(None)  # Use a dummy value like `None`

    mock_scheduler.enqueue.side_effect = enqueue_side_effect

    resonate.register(func)
    resonate.run(func.__name__, func, a=1, b=2)  # This will now unblock

    # Verify the enqueue method was called once
    mock_scheduler.enqueue.assert_called_once()

    # Extract the Invoke instance and futures from the call
    args, kwargs = mock_scheduler.enqueue.call_args
    invoke = args[0]
    futures = kwargs.get("futures")

    # Assert Invoke parameters
    assert invoke.id == func.__name__
    assert invoke.name == func.__name__
    assert invoke.func is func
    assert invoke.args == tuple()
    assert invoke.kwargs == {"a": 1, "b":2}


@pytest.mark.parametrize("func", [foo, bar, baz])
def test_rpc_and_register_as_method_call(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = MagicMock()
    resonate._scheduler = mock_scheduler

    # Define a side effect to set the result on `fp` to unblock the run method
    def enqueue_side_effect(*args, **kwargs):
        futures = kwargs.get("futures", ())
        if futures:
            # Set a result on the first future (`fp`) to unblock
            futures[0].set_result(None)  # Use a dummy value like `None`

    mock_scheduler.enqueue.side_effect = enqueue_side_effect

    resonate.register(func)
    resonate.rpc(func.__name__, func, 1, 2)  # This will now unblock

    # Verify the enqueue method was called once
    mock_scheduler.enqueue.assert_called_once()

    # Extract the Invoke instance and futures from the call
    args, kwargs = mock_scheduler.enqueue.call_args
    invoke = args[0]
    futures = kwargs.get("futures")

    # Assert Invoke parameters
    assert invoke.id == func.__name__
    assert invoke.name == func.__name__
    assert invoke.func is None
    assert invoke.args == (1, 2)
    assert invoke.kwargs == {}


@pytest.mark.parametrize("func", [foo, bar, baz])
def test_run_and_register_as_decorator(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = MagicMock()
    resonate._scheduler = mock_scheduler


    # Define a side effect to set the result on `fp` to unblock the run method
    def enqueue_side_effect(*args, **kwargs):
        futures = kwargs.get("futures", ())
        if futures:
            # Set a result on the first future (`fp`) to unblock
            futures[0].set_result(None)  # Use a dummy value like `None`

    mock_scheduler.enqueue.side_effect = enqueue_side_effect

    fn = resonate.register(func)
    fn.run(func.__name__, a=1, b=2)  # This will now unblock

    # Verify the enqueue method was called once
    mock_scheduler.enqueue.assert_called_once()

    # Extract the Invoke instance and futures from the call
    args, kwargs = mock_scheduler.enqueue.call_args
    invoke = args[0]
    futures = kwargs.get("futures")

    # Assert Invoke parameters
    assert invoke.id == func.__name__
    assert invoke.name == func.__name__
    assert invoke.func is func
    assert invoke.args == tuple()
    assert invoke.kwargs == {"a": 1, "b":2}


@pytest.mark.parametrize("func", [foo, bar, baz])
def test_rpc_and_register_as_decorator(func: Callable) -> None:
    resonate = Resonate()
    mock_scheduler = MagicMock()
    resonate._scheduler = mock_scheduler

    # Define a side effect to set the result on `fp` to unblock the run method
    def enqueue_side_effect(*args, **kwargs):
        futures = kwargs.get("futures", ())
        if futures:
            # Set a result on the first future (`fp`) to unblock
            futures[0].set_result(None)  # Use a dummy value like `None`

    mock_scheduler.enqueue.side_effect = enqueue_side_effect

    fn = resonate.register(func)
    fn.rpc(func.__name__, 1, 2)  # This will now unblock

    # Verify the enqueue method was called once
    mock_scheduler.enqueue.assert_called_once()

    # Extract the Invoke instance and futures from the call
    args, kwargs = mock_scheduler.enqueue.call_args
    invoke = args[0]
    futures = kwargs.get("futures")

    # Assert Invoke parameters
    assert invoke.id == func.__name__
    assert invoke.name == func.__name__
    assert invoke.func is None
    assert invoke.args == (1, 2)
    assert invoke.kwargs == {}
