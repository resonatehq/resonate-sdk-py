from __future__ import annotations

import time

from resonate import Resonate

resonate = Resonate.remote()
resonate.promises.create("foo", timeout=int(time.time() * 1000) + 86400000, ikey="foo")

for _ in range(10):
    h = resonate.get("foo")
    print(h)
