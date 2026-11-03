#
#  Copyright (c) 2026, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
"""Request statistics of the v2 clients.

Every client keeps a :class:`RequestStats` object in its ``request_stats``
attribute. Each HTTP request sent to the FirecREST API (retries included,
transfers to and from the staging area excluded) increments its counters
and is appended, as a :class:`RequestRecord`, to its bounded history. The
object can be inspected at any time, for example::

    client.request_stats.total
    client.request_stats.by_endpoint
    client.request_stats.records[-1].status_code
    client.request_stats.as_dict()

and it can be shared by several clients by passing the same object to their
constructors.
"""
from __future__ import annotations

import collections
import dataclasses
import threading
import time

from typing import Any, Callable, Deque, Dict, List, Optional, Union


@dataclasses.dataclass(frozen=True)
class RequestRecord:
    """One HTTP request (a single attempt) sent to the FirecREST API."""

    #: HTTP method of the request.
    method: str
    #: Path of the endpoint relative to the FirecREST URL, e.g.
    #: ``/status/systems``.
    endpoint: str
    #: Query parameters of the request.
    params: Dict[str, Any]
    #: Status code of the response, or ``None`` when the request failed
    #: with a transport error.
    status_code: Optional[int]
    #: Name of the exception class when the request failed with a transport
    #: error, otherwise ``None``.
    error: Optional[str]
    #: Seconds between sending the request and receiving the response (or
    #: the error).
    duration: float
    #: Time at which the request was sent, as returned by ``time.time()``.
    timestamp: float
    #: ``0`` for the first attempt of a call, ``1`` for its first retry and
    #: so on.
    attempt: int
    #: Value of the ``X-Request-ID`` header of the request, when known.
    request_id: Optional[str]
    #: Value of the ``X-Correlation-ID`` header of the request, when set.
    correlation_id: Optional[str]

    @property
    def key(self) -> str:
        """Method and endpoint, e.g. ``GET /status/systems``."""
        return f"{self.method} {self.endpoint}"

    @property
    def failed(self) -> bool:
        """``True`` for transport errors and for error status codes."""
        return self.status_code is None or self.status_code >= 400

    def as_dict(self) -> Dict[str, Any]:
        """The record as a JSON-serialisable dictionary."""
        return dataclasses.asdict(self)


class RequestStats:
    """Counters and history of the requests made to the FirecREST API.

    The counters are cumulative since the creation of the object or the
    last :meth:`reset`. The history keeps the last :attr:`max_records`
    requests. Recording is thread-safe.

    :param max_records: how many of the most recent requests are kept in
                        :attr:`records`; ``None`` keeps all of them.
    :param enabled: when ``False`` nothing is recorded.
    """

    def __init__(
        self, max_records: Optional[int] = 1000, enabled: bool = True
    ) -> None:
        #: When ``False`` the requests are not counted nor recorded.
        self.enabled: bool = enabled
        #: Callables invoked with every new :class:`RequestRecord`, right
        #: after it is recorded. Useful to forward the requests to a
        #: dashboard, a log or a metrics system. Exceptions raised by a
        #: listener are propagated to the caller of the client method.
        self.listeners: List[Callable[[RequestRecord], None]] = []
        self._lock = threading.Lock()
        self._records: Deque[RequestRecord] = collections.deque(
            maxlen=max_records
        )
        self._reset_counters()

    def _reset_counters(self) -> None:
        #: Number of requests sent to the API, retries included.
        self.total: int = 0
        #: Number of requests that were retries of a previous attempt.
        self.retries: int = 0
        #: Number of requests that failed with a transport error or an
        #: error status code.
        self.failed: int = 0
        #: Sum of the durations of all the requests, in seconds.
        self.total_duration: float = 0.0
        #: Number of requests per HTTP method.
        self.by_method: collections.Counter[str] = collections.Counter()
        #: Number of requests per method and endpoint, keyed like
        #: ``GET /status/systems``.
        self.by_endpoint: collections.Counter[str] = collections.Counter()
        #: Number of requests per status code. Transport errors are counted
        #: under the name of the exception class.
        self.by_status: collections.Counter[Union[int, str]] = (
            collections.Counter()
        )

    @property
    def max_records(self) -> Optional[int]:
        """Size of the history, ``None`` for unbounded."""
        return self._records.maxlen

    @property
    def records(self) -> List[RequestRecord]:
        """The most recent requests, oldest first."""
        with self._lock:
            return list(self._records)

    def reset(self) -> None:
        """Clear the counters and the history."""
        with self._lock:
            self._records.clear()
            self._reset_counters()

    def record(self, record: RequestRecord) -> None:
        """Count and store a request, then notify the :attr:`listeners`."""
        if not self.enabled:
            return

        with self._lock:
            self.total += 1
            if record.attempt > 0:
                self.retries += 1

            if record.failed:
                self.failed += 1

            self.total_duration += record.duration
            self.by_method[record.method] += 1
            self.by_endpoint[record.key] += 1
            status: Union[int, str] = (
                record.status_code
                if record.status_code is not None
                else (record.error or "error")
            )
            self.by_status[status] += 1
            self._records.append(record)

        for listener in list(self.listeners):
            listener(record)

    def add(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        attempt: int = 0,
        timestamp: Optional[float] = None,
        duration: float = 0.0,
        response: Any = None,
        exc: Optional[BaseException] = None,
        correlation_id: Optional[str] = None,
    ) -> Optional[RequestRecord]:
        """Build a :class:`RequestRecord` from the outcome of a request and
        :meth:`record` it. Either ``response`` (an ``httpx.Response``) or
        ``exc`` (the transport error) is expected. Returns the record, or
        ``None`` when the statistics are disabled.
        """
        if not self.enabled:
            return None

        request = None
        if response is not None:
            request = response.request
        elif exc is not None:
            try:
                # `httpx.RequestError.request` raises when it was never set
                request = getattr(exc, "request", None)
            except RuntimeError:
                request = None

        headers = request.headers if request is not None else {}
        record = RequestRecord(
            method=method,
            endpoint=endpoint,
            params={k: v for k, v in (params or {}).items() if v is not None},
            status_code=(
                response.status_code if response is not None else None
            ),
            error=type(exc).__name__ if exc is not None else None,
            duration=duration,
            timestamp=timestamp if timestamp is not None else time.time(),
            attempt=attempt,
            request_id=headers.get("X-Request-ID"),
            correlation_id=(
                headers.get("X-Correlation-ID") or correlation_id
            ),
        )
        self.record(record)
        return record

    def as_dict(self) -> Dict[str, Any]:
        """The counters and the history as a JSON-serialisable dictionary,
        ready to be returned by an MCP tool or rendered in a dashboard.
        """
        with self._lock:
            return {
                "total": self.total,
                "retries": self.retries,
                "failed": self.failed,
                "total_duration": self.total_duration,
                "by_method": dict(self.by_method),
                "by_endpoint": dict(self.by_endpoint),
                "by_status": {
                    str(k): v for k, v in self.by_status.items()
                },
                "records": [r.as_dict() for r in self._records],
            }

    def __repr__(self) -> str:
        return (
            f"RequestStats(total={self.total}, retries={self.retries}, "
            f"failed={self.failed}, enabled={self.enabled})"
        )
