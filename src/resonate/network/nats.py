from __future__ import annotations

import contextlib
import logging
import uuid
from typing import TYPE_CHECKING

from resonate.error import NatsError

if TYPE_CHECKING:
    from collections.abc import Callable

    import nats
    import nats.errors
    from nats.aio.client import Client
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription
else:
    try:
        import nats
        import nats.errors
    except ImportError:
        nats = None

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

#: Seconds to wait for a server reply to a request before giving up.
DEFAULT_REQUEST_TIMEOUT_SECS = 30.0

#: Subject the Resonate server listens on for request/reply API calls.
#: ponytail: must match the server's NATS message-source subject; override via
#: ``api_subject`` if the deployment uses a different one.
DEFAULT_API_SUBJECT = "resonate.requests"

#: Subject prefix the server publishes execute/unblock messages on. The
#: ``nats://uni@group/pid`` / ``nats://any@group/pid`` addresses map onto
#: ``{prefix}.{group}.{pid}`` (unicast) and ``{prefix}.{group}`` (anycast,
#: queue-subscribed on ``group`` so exactly one group member receives it).
#: ponytail: must match the server's address->subject mapping; override via
#: ``subject_prefix`` if the deployment differs.
DEFAULT_SUBJECT_PREFIX = "resonate.recv"


class NatsNetwork:
    """:class:`Network` implementation that talks to a Resonate server over NATS.

    - Requests are sent via NATS request/reply on ``api_subject``.
    - Incoming messages (execute/unblock) arrive on two subscriptions:
      a unicast subject ``{prefix}.{group}.{pid}`` and an anycast subject
      ``{prefix}.{group}`` queue-subscribed on ``group``.
    - Addresses use the ``nats://`` scheme: ``nats://uni@group/id`` and
      ``nats://any@group/id``.

    Reconnection is delegated to ``nats-py`` (built-in exponential backoff),
    so this class does not reimplement a retry loop the way
    :class:`~resonate.network.http.HttpNetwork` must for SSE.

    Requires the optional ``nats-py`` dependency (``pip install resonate-sdk[nats]``).
    """

    def __init__(
        self,
        url: str,
        pid: str | None = None,
        group: str | None = None,
        auth: str | None = None,
        *,
        api_subject: str = DEFAULT_API_SUBJECT,
        subject_prefix: str = DEFAULT_SUBJECT_PREFIX,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT_SECS,
    ) -> None:
        self._pid = pid if pid is not None else uuid.uuid4().hex
        self._group = group if group is not None else "default"
        self._unicast = f"nats://uni@{self._group}/{self._pid}"
        self._anycast = f"nats://any@{self._group}/{self._pid}"
        self._url = url
        self._auth = auth
        self._api_subject = api_subject
        self._uni_subject = f"{subject_prefix}.{self._group}.{self._pid}"
        self._any_subject = f"{subject_prefix}.{self._group}"
        self._request_timeout = request_timeout

        self._subscribers: list[Callable[[str], None]] = []
        self._nc: Client | None = None
        self._subs: list[Subscription] = []
        self._running = False

    def pid(self) -> str:
        return self._pid

    def group(self) -> str:
        return self._group

    def unicast(self) -> str:
        return self._unicast

    def anycast(self) -> str:
        return self._anycast

    async def start(self) -> None:
        """Connect to NATS and subscribe to the unicast and anycast subjects."""
        if nats is None:
            msg = "NatsNetwork requires 'nats-py': pip install resonate-sdk[nats]"
            raise NatsError(ImportError(msg))
        self._running = True
        self._nc = await nats.connect(self._url, token=self._auth)
        # Unicast: only this pid; anycast: queue group so one member wins.
        self._subs = [
            await self._nc.subscribe(self._uni_subject, cb=self._on_msg),
            await self._nc.subscribe(
                self._any_subject, queue=self._group, cb=self._on_msg
            ),
        ]
        logger.info(
            "NATS connected: %s (uni=%s any=%s)",
            self._url,
            self._uni_subject,
            self._any_subject,
        )

    async def stop(self) -> None:
        self._running = False
        for sub in self._subs:
            with contextlib.suppress(Exception):
                await sub.unsubscribe()
        self._subs.clear()
        if self._nc is not None:
            with contextlib.suppress(Exception):
                await self._nc.drain()
            self._nc = None
        self._subscribers.clear()

    async def send(self, req: str) -> str:
        """Send a request via NATS request/reply, returning the reply body.

        ``nats-py`` transparently reconnects, but a request issued while no
        server is subscribed (startup, restart) raises ``NoRespondersError``
        or times out. Once :meth:`stop` runs, any send raises
        :class:`NatsError` instead of touching a torn-down connection.
        """
        logger.debug("nats_network req: %s", req)
        if not self._running or self._nc is None:
            raise NatsError(RuntimeError("network has been stopped"))
        try:
            msg = await self._nc.request(
                self._api_subject,
                req.encode("utf-8"),
                timeout=self._request_timeout,
            )
        except (nats.errors.Error, TimeoutError) as exc:
            raise NatsError(exc) from exc
        resp_str = msg.data.decode("utf-8")
        logger.debug("nats_network res: %s", resp_str)
        return resp_str

    def recv(self, callback: Callable[[str], None]) -> None:
        """Register a callback for incoming messages."""
        self._subscribers.append(callback)

    def target_resolver(self, target: str) -> str:
        """Resolve a target name to a ``nats://`` anycast address."""
        return f"nats://any@{target}"

    # -- internals ------------------------------------------------------------

    async def _on_msg(self, msg: Msg) -> None:
        """NATS subscription callback: decode and fan out to subscribers."""
        try:
            data = msg.data.decode("utf-8")
        except UnicodeDecodeError:
            logger.warning("dropping non-utf8 NATS message on %s", msg.subject)
            return
        logger.debug("nats_network recv: %s", data)
        for cb in list(self._subscribers):
            cb(data)
