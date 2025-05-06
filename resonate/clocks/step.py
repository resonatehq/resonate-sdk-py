from __future__ import annotations


class StepClock:
    def __init__(self) -> None:
        self._clock = 0.0

    @property
    def clock(self) -> float:
        """Return the current time in seconds."""
        return self._clock

    @clock.setter
    def clock(self, value: float) -> None:
        assert value >= self._clock, "The arrow of time only flows forward."
        self._clock = value

    def time(self) -> float:
        """Return the current time in seconds."""
        return self._clock

    def strftime(self, format: str, /) -> str:
        raise NotImplementedError
