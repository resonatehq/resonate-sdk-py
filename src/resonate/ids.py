"""The promise id format, in one place.

The server treats a promise id as ``<origin>:<lineage>``: the **origin** is
everything before the first ``:``, and the lineage segments below it are
``.``-separated::

    root -> root:1 -> root:1.1 -> root:1.1.1

The origin is load-bearing. ``promise.register_callback`` and ``task.suspend``
require an awaiter and its awaited promise to share one, it selects the
origin-state partition a request is routed to, and ``promise.create`` rejects
an id that does not extend the ``resonate:origin`` / ``resonate:branch`` /
``resonate:parent`` it declares. So the SDK mints ids with :func:`join_id` and
reads them back with :func:`origin_of`, both of which mirror the server's own
rules (``src/types.rs``).

A root id is supplied by the caller and becomes the origin of its whole
lineage, so :func:`validate_root_id` keeps both separators out of it, exactly
as the server does for the origin tag itself.
"""

from __future__ import annotations

from resonate.error import InvalidIdError

#: Separates the origin from the lineage below it. A bare root joins its first
#: lineage segment with this.
ORIGIN_SEP = ":"

#: Separates lineage segments below the origin.
LINEAGE_SEP = "."


def join_id(ancestor: str, segment: str) -> str:
    """Append a lineage ``segment`` to ``ancestor``.

    A bare root joins its *first* segment with ``:``; an ancestor that already
    carries lineage joins deeper segments with ``.``, keeping the whole subtree
    under one origin::

        join_id("root", "1")     -> "root:1"
        join_id("root:1", "2")   -> "root:1.2"
        join_id("root:1.2", "3") -> "root:1.2.3"

    This is exactly the separator rule the server's ``resonate:branch`` /
    ``resonate:parent`` validation applies.
    """
    sep = LINEAGE_SEP if ORIGIN_SEP in ancestor else ORIGIN_SEP
    return f"{ancestor}{sep}{segment}"


def origin_of(id: str) -> str:
    """Return the lineage origin of ``id``: everything before the first ``:``.

    Mirrors the server's ``origin()``. An id with no lineage below it (a root)
    is its own origin.
    """
    return id.split(ORIGIN_SEP, 1)[0]


def validate_root_id(id: str) -> str:
    """Return a caller-supplied root id (``run`` / ``rpc`` / ``schedule``), or raise.

    Both separators are **reserved**: a root becomes the origin of its whole
    lineage, and the server rejects an origin containing either one outright
    (``dot_in_origin`` / ``colon_in_origin``). ``.`` because it separates
    lineage segments; ``:`` because the origin is everything before an id's
    *first* ``:``, so an origin holding one could never be split back out of
    any id.

    Raises:
        ~resonate.error.InvalidIdError: caught here, at the call site that named
            the workflow, rather than surfacing later as an opaque 400 from a
            background create.

    """
    if not id:
        msg = "id must not be empty"
        raise InvalidIdError(id, msg)
    if "\0" in id:
        msg = "id must not contain null bytes"
        raise InvalidIdError(id, msg)
    for sep in (LINEAGE_SEP, ORIGIN_SEP):
        if sep in id:
            msg = (
                f"id must not contain {sep!r}: it is reserved as a lineage"
                " separator in the ids the SDK mints below this one"
            )
            raise InvalidIdError(id, msg)
    return id
