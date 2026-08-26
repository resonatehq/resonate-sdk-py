# resonate-base

The shared protocol layer behind the Resonate Python SDK and every Resonate
connector.

`resonate-base` holds the small set of definitions a connector needs and
nothing else: the `Network` and `Source` protocols a connector implements, the
error vocabulary it raises, and the promise id format it routes by. Everything
that describes *executing durable functions* -- codec, context, core, retry,
timing, the observability stream, the wire records and the transport that
frames JSON over a connection -- stays in `resonate-sdk`.

It has no third-party dependencies and never imports the SDK, so a connector
built on it stays independent of the SDK's release cadence.

```python
from resonate_base.connections import Network, Source
from resonate_base.error import ServerError
from resonate_base.ids import origin_of


class MyConnection:
    """A Network implementation for some new substrate."""

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def send(self, req: str) -> str: ...
```

Application code should depend on [`resonate-sdk`](https://pypi.org/project/resonate-sdk/),
which depends on this package.
