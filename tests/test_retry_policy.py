from __future__ import annotations

from resonate.retry_policy import exponential, fixed, linear


def test_exponential() -> None:
    policy = exponential(base_delay=1, factor=2, max_retries=3)
    assert policy.should_retry(attempt=1)
    assert policy.calculate_delay(attempt=1) == 2  # noqa: PLR2004
    assert policy.should_retry(attempt=2)
    assert policy.calculate_delay(attempt=2) == 4  # noqa: PLR2004
    assert policy.should_retry(attempt=3)
    assert policy.calculate_delay(attempt=3) == 8  # noqa: PLR2004
    assert not policy.should_retry(attempt=4)


def test_linear() -> None:
    policy = linear(delay=2, max_retries=3)
    assert policy.should_retry(attempt=1)
    assert policy.calculate_delay(attempt=1) == 2  # noqa: PLR2004
    assert policy.should_retry(attempt=2)
    assert policy.calculate_delay(attempt=2) == 4  # noqa: PLR2004
    assert policy.should_retry(attempt=3)
    assert policy.calculate_delay(attempt=3) == 6  # noqa: PLR2004
    assert not policy.should_retry(attempt=4)


def test_fixed() -> None:
    policy = fixed(delay=2, max_retries=3)
    assert policy.should_retry(attempt=1)
    assert policy.calculate_delay(attempt=1) == 2  # noqa: PLR2004
    assert policy.should_retry(attempt=2)
    assert policy.calculate_delay(attempt=2) == 2  # noqa: PLR2004
    assert policy.should_retry(attempt=3)
    assert policy.calculate_delay(attempt=3) == 2  # noqa: PLR2004
    assert not policy.should_retry(attempt=4)
