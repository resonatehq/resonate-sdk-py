"""A five-field cron evaluator, in UTC.

Schedules need to answer one question: *what is the next instant this expression
denotes, strictly after some time?* This module answers it for the standard
five-field format (minute, hour, day-of-month, month, day-of-week), supporting
``*``, lists, ranges, and steps.

**UTC, deliberately.** A schedule in a decentralized deployment is fired by
whichever process sweeps it first, and those processes need not share a
timezone. Interpreting the expression in local time would make the same schedule
fire at different instants depending on which machine woke up -- and would put
the TypeScript and Python SDKs at odds in a mixed fleet. The TypeScript side
pins ``tz: "UTC"`` for the same reason.

Day-of-month and day-of-week follow the usual cron rule: when both are
restricted the match is their *union*, not their intersection.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

#: Field bounds, in the order they appear in an expression.
_BOUNDS: tuple[tuple[int, int], ...] = (
    (0, 59),  # minute
    (0, 23),  # hour
    (1, 31),  # day of month
    (1, 12),  # month
    (0, 6),  # day of week, Sunday = 0
)

_MONTH_NAMES = {
    name: i + 1
    for i, name in enumerate(
        (
            "jan",
            "feb",
            "mar",
            "apr",
            "may",
            "jun",
            "jul",
            "aug",
            "sep",
            "oct",
            "nov",
            "dec",
        )
    )
}
_DAY_NAMES = {
    name: i for i, name in enumerate(("sun", "mon", "tue", "wed", "thu", "fri", "sat"))
}


class CronError(ValueError):
    """Raised when an expression cannot be parsed."""


def _parse_field(raw: str, index: int) -> frozenset[int]:
    low, high = _BOUNDS[index]
    allowed: set[int] = set()

    for part in raw.split(","):
        if not part:
            msg = f"empty field element in {raw!r}"
            raise CronError(msg)

        body, _, step_raw = part.partition("/")
        step = 1
        if step_raw:
            if not step_raw.isdigit() or int(step_raw) == 0:
                msg = f"invalid step {step_raw!r} in {raw!r}"
                raise CronError(msg)
            step = int(step_raw)

        if body in {"*", "?"}:
            start, end = low, high
        else:
            start_raw, sep, end_raw = body.partition("-")
            start = _parse_value(start_raw, index)
            end = _parse_value(end_raw, index) if sep else start
            if not sep and step_raw:
                # `5/15` means "from 5, stepping by 15" -- an open-ended range.
                end = high
            if start > end:
                msg = f"descending range {body!r} in {raw!r}"
                raise CronError(msg)

        if start < low or end > high:
            msg = f"value out of range in {raw!r}: expected {low}-{high}"
            raise CronError(msg)

        allowed.update(range(start, end + 1, step))

    # Sunday is both 0 and 7 in the day-of-week field.
    if index == 4 and 7 in allowed:
        allowed.discard(7)
        allowed.add(0)

    return frozenset(allowed)


def _parse_value(raw: str, index: int) -> int:
    text = raw.strip().lower()
    if index == 3 and text in _MONTH_NAMES:
        return _MONTH_NAMES[text]
    if index == 4 and text in _DAY_NAMES:
        return _DAY_NAMES[text]
    if text == "7" and index == 4:
        return 0
    try:
        return int(text)
    except ValueError as exc:
        msg = f"invalid value {raw!r}"
        raise CronError(msg) from exc


class CronExpression:
    """A parsed five-field cron expression."""

    __slots__ = ("_dom_restricted", "_dow_restricted", "_fields")

    def __init__(self, expression: str) -> None:
        parts = expression.split()
        if len(parts) != len(_BOUNDS):
            msg = (
                f"expected {len(_BOUNDS)} cron fields, got {len(parts)}: {expression!r}"
            )
            raise CronError(msg)
        # `7` normalizes to `0`, so compare the raw text to decide whether the
        # day fields are restricted -- that is what selects union matching.
        self._dom_restricted = parts[2] not in {"*", "?"}
        self._dow_restricted = parts[4] not in {"*", "?"}
        self._fields = tuple(_parse_field(part, i) for i, part in enumerate(parts))

    def month_matches(self, moment: datetime) -> bool:
        return moment.month in self._fields[3]

    def day_matches(self, moment: datetime) -> bool:
        # `datetime.weekday()` is Monday-based; cron is Sunday-based.
        weekday = (moment.weekday() + 1) % 7
        by_dom = moment.day in self._fields[2]
        by_dow = weekday in self._fields[4]

        if self._dom_restricted and self._dow_restricted:
            return by_dom or by_dow
        if self._dom_restricted:
            return by_dom
        if self._dow_restricted:
            return by_dow
        return True

    def hour_matches(self, moment: datetime) -> bool:
        return moment.hour in self._fields[1]

    def minute_matches(self, moment: datetime) -> bool:
        return moment.minute in self._fields[0]

    def matches(self, moment: datetime) -> bool:
        return (
            self.month_matches(moment)
            and self.day_matches(moment)
            and self.hour_matches(moment)
            and self.minute_matches(moment)
        )


def next_cron(expression: str, after_ms: int) -> int:
    """Return the next instant the expression denotes, strictly after ``after_ms``.

    Both the argument and the result are Unix milliseconds.
    """
    parsed = CronExpression(expression)
    # Cron has minute resolution, so start from the minute after `after_ms`.
    moment = datetime.fromtimestamp(after_ms / 1000, tz=UTC).replace(
        second=0, microsecond=0
    )
    moment += timedelta(minutes=1)
    horizon = moment + timedelta(days=4 * 366)

    # Skip by the coarsest field that does not match, so a sparse expression
    # (`0 0 29 2 *`) costs a few hundred steps rather than a few million.
    while moment < horizon:
        if not parsed.month_matches(moment):
            moment = _start_of_next_month(moment)
        elif not parsed.day_matches(moment):
            moment = (moment + timedelta(days=1)).replace(hour=0, minute=0)
        elif not parsed.hour_matches(moment):
            moment = (moment + timedelta(hours=1)).replace(minute=0)
        elif not parsed.minute_matches(moment):
            moment += timedelta(minutes=1)
        else:
            return int(moment.timestamp() * 1000)

    msg = f"cron expression {expression!r} has no occurrence within four years"
    raise CronError(msg)


def _start_of_next_month(moment: datetime) -> datetime:
    year, month = (
        (moment.year + 1, 1) if moment.month == 12 else (moment.year, moment.month + 1)
    )
    return moment.replace(year=year, month=month, day=1, hour=0, minute=0)


def cron_occurrences(
    expression: str, since_ms: int, now_ms: int, cap: int = 128
) -> list[int]:
    """Every instant the expression denotes in ``(since_ms, now_ms]``, in order.

    Bounded by ``cap`` so a schedule that has been asleep for a long time catches
    up over several sweeps rather than firing an unbounded burst in one.
    """
    out: list[int] = []
    cursor = since_ms
    while len(out) < cap:
        nxt = next_cron(expression, cursor)
        if nxt > now_ms:
            break
        out.append(nxt)
        cursor = nxt
    return out
