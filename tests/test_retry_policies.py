import pytest
from resonate.models.retry_policies import Never, RetryPolicy, never, constant, linear, exponential


@pytest.mark.parametrize(
    ("policy", "progression"),
    [
        (never(), None),
        (
            constant(delay=1, max_retries=2),
            [1, 1, None],
        ),
        (
            linear(delay=1, max_retries=2),
            [1, 2, None],
        ),
        (
            exponential(base_delay=1, factor=2, max_retries=5, max_delay=8),
            [2, 4, 8, 8, 8, None],
        ),
    ],
)
def test_delay_progression(policy: RetryPolicy, progression: list[float | None] | None) -> None:
    if isinstance(policy, Never):
        return

    attempt: int = 1
    delays: list[float | None] = []
    while True:
        delays.append(policy.calculate_delay(attempt))
        attempt += 1
        if delays[-1] is None:
            break

    assert delays == progression
