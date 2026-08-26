"""The connector seam shared by the Resonate SDK and its connectors.

A connector implements :class:`~resonate_base.connections.Network` and/or
:class:`~resonate_base.connections.Source`, raises the errors in
:mod:`resonate_base.error`, and routes by the promise id format in
:mod:`resonate_base.ids`. Everything that describes *executing durable
functions* lives in :mod:`resonate` (the ``resonate-sdk`` distribution).
"""

from __future__ import annotations
