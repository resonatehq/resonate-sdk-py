from __future__ import annotations

from datetime import datetime

from croniter import croniter

base = datetime(2020, 1, 1, 1, 1)
iter = croniter("* * * * *", base)
print(iter.get_next(datetime))
print(iter.get_prev(datetime))
