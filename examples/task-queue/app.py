from __future__ import annotations

import uuid

from resonate import Resonate

resonate = Resonate.remote(host="http://localhost")

if __name__ == "__main__":
    h = resonate.options(target="poll://math").rpc(f"task-queue-{uuid.uuid4().hex}", "add", 1, 2)
    v = h.result()
    assert v == 3
