from __future__ import annotations

import pytest

from resonate import Context, Resonate
from resonate.models.schedules import Schedule


@pytest.fixture
def resonate() -> Resonate:
    """Create a local Resonate instance for testing."""
    return Resonate()



def sample_function(ctx: Context, value: int) -> int:
    """Return the value multiplied by 2."""
    return value * 2


def test_schedule_with_callable(resonate: Resonate) -> None:
    """Test creating a schedule with a callable function."""
    resonate.register(sample_function)

    schedule = resonate.schedule(
        "test_schedule_1",
        sample_function,
        "*/5 * * * *",
        42,
    )

    assert isinstance(schedule, Schedule)
    assert schedule.id == "test_schedule_1"
    assert schedule.cron == "*/5 * * * *"
    assert "sample_function" in schedule.promise_id

    # Cleanup
    schedule.delete()


def test_schedule_with_registered_name(resonate: Resonate) -> None:
    """Test creating a schedule with a registered function name."""
    resonate.register(sample_function)

    schedule = resonate.schedule(
        "test_schedule_2",
        "sample_function",
        "0 * * * *",
        100,
    )

    assert isinstance(schedule, Schedule)
    assert schedule.id == "test_schedule_2"
    assert schedule.cron == "0 * * * *"

    # Cleanup
    schedule.delete()


def test_schedule_with_kwargs(resonate: Resonate) -> None:
    """Test creating a schedule with keyword arguments."""

    def func_with_kwargs(ctx: Context, name: str, age: int) -> str:
        return f"{name} is {age} years old"

    resonate.register(func_with_kwargs)

    schedule = resonate.schedule(
        "test_schedule_3",
        func_with_kwargs,
        "0 9 * * *",
        name="Alice",
        age=30,
    )

    assert isinstance(schedule, Schedule)
    assert schedule.id == "test_schedule_3"

    # Cleanup
    schedule.delete()


def test_schedule_with_options(resonate: Resonate) -> None:
    """Test creating a schedule with custom options."""
    resonate.register(sample_function)

    schedule = resonate.options(
        timeout=3600,
        tags={"env": "test", "priority": "high"},
    ).schedule(
        "test_schedule_4",
        sample_function,
        "0 0 * * *",
        999,
    )

    assert isinstance(schedule, Schedule)
    assert schedule.id == "test_schedule_4"
    assert schedule.promise_tags is not None
    assert schedule.promise_tags.get("env") == "test"
    assert schedule.promise_tags.get("priority") == "high"

    # Cleanup
    schedule.delete()


def test_schedule_invalid_inputs(resonate: Resonate) -> None:
    """Verify that invalid inputs raise appropriate errors."""
    resonate.register(sample_function)

    # Invalid schedule ID (not a string)
    with pytest.raises(TypeError):
        resonate.schedule(123, sample_function, "* * * * *", 42)  # type: ignore[arg-type]

    # Invalid function (not callable or string)
    with pytest.raises(TypeError):
        resonate.schedule("test", 12345, "* * * * *", 42)  # type: ignore[arg-type]

    # Invalid cron (not a string)
    with pytest.raises(TypeError):
        resonate.schedule("test", sample_function, 12345, 42)  # type: ignore[arg-type]
