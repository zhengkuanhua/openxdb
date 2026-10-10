# -*- coding: utf-8 -*-
"""OpenXDB Python driver error hierarchy (PEP 249 style).

The wire protocol is a line-based text protocol: every response either
starts with ``ERR `` (server-side error), or carries a plain text result.
All server-side failures are surfaced as :class:`OperationalError`; client
side usage mistakes (bad parameter types, NULL literals, calls on a closed
connection) are surfaced as :class:`ProgrammingError`.
"""


class OpenXDBError(Exception):
    """Base class for all driver errors."""


class InterfaceError(OpenXDBError):
    """Client-side interface / usage error (connection state misuse)."""


class ProgrammingError(OpenXDBError):
    """SQL or parameter misuse on the client side."""


class OperationalError(OpenXDBError):
    """Server-side or network operational error (ERR response, socket loss)."""


class ConnectionClosedError(InterfaceError):
    """Operation attempted on a closed connection/cursor."""


class ParameterError(ProgrammingError):
    """Invalid parameter for the client-side escaping engine."""
