# resonate-nats

NATS connector for Resonate.

`NatsConnection` is both a `Network` (request/reply to the Resonate server)
and a `Source` (subscriptions carrying `execute` / `unblock` messages), so it
can serve as a Resonate client's only connection.

```python
from resonate.resonate import Resonate
from resonate_nats import NatsConnection

conn = NatsConnection(servers=["nats://localhost:4222"], group="workers")
resonate = Resonate(network=conn, sources=[conn])
```

Install it with the SDK extra:

```bash
pip install "resonate-sdk[nats]"
```

The package itself depends only on [`resonate-base`](https://pypi.org/project/resonate-base/)
and `nats-py`, never on `resonate-sdk`.
