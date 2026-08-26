# resonate-base

The shared protocol layer behind the Resonate Python SDK and every Resonate
connector.

`resonate-base` holds the definitions a connector needs and nothing else: the
error vocabulary, injectable time (`Clock` / `Sleeper`), retry and backoff
policies, the observability event stream, the promise id format, the wire
records exchanged with the Resonate server, the `Network` and `Source`
protocols a connector implements, and the `Transport` that frames JSON over
them.

It depends only on `msgspec`. It never imports the SDK, so a connector built
on it stays independent of the SDK's release cadence.

```python
from resonate_base.connections import Network, Source
from resonate_base.error import ServerError
from resonate_base.retry import ExponentialBackoff
from resonate_base.timing import Sleeper, sleep


class MyConnection:
    """A Network implementation for some new substrate."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, req: str) -> str: ...
```

Application code should depend on [`resonate-sdk`](https://pypi.org/project/resonate-sdk/),
which depends on this package.
