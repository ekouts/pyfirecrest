#
#  Copyright (c) 2026, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
"""Retry policy of the v2 clients.

A :class:`RetryPolicy` is a table of :class:`RetryRule` objects keyed by an
HTTP status code or by an ``httpx`` exception class. The clients consult the
table after every request and, when a rule matches, sleep and repeat the
request until the rule's budget is exhausted. The table can be edited in
place, for example::

    client.retry_policy.rules[503].max_retries = 10
    client.retry_policy.rules[500] = RetryRule(
        message="exit status:124", idempotent_only=True
    )
    del client.retry_policy.rules[424]
"""
from __future__ import annotations

import copy
import dataclasses
import random
import re

from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union

import httpx

from firecrest.utilities import parse_retry_after


#: A key of the rule table: an HTTP status code or an exception class.
RuleKey = Union[int, Type[BaseException]]


def _no_log(level: int, msg: Any) -> None:
    pass


@dataclasses.dataclass
class RetryRule:
    """Describes if and how a request is retried after a given failure.

    A rule is selected by the key it is registered under in
    :attr:`RetryPolicy.rules` (status code or exception class) and applies
    only when its :attr:`idempotent_only` and :attr:`message` conditions are
    satisfied.
    """

    #: Maximum number of retries for failures matching this rule. ``None``
    #: means that the request is retried until a different response is
    #: received.
    max_retries: Optional[int] = 3
    #: Base of the exponential backoff, in seconds. The n-th retry (starting
    #: from 0) sleeps ``backoff_factor * 2**n`` seconds before the cap and
    #: jitter are applied.
    backoff_factor: float = 1.0
    #: Upper bound of a single sleep, in seconds.
    max_backoff: float = 30.0
    #: When ``True`` every sleep is multiplied by a random factor between 0.5
    #: and 1.5 (before the cap), to spread the retries of many clients.
    jitter: bool = True
    #: When ``True`` the rule applies only to requests that are safe to repeat
    #: (reads and idempotent writes), never to requests that submit a job or
    #: create something on the system.
    idempotent_only: bool = False
    #: Optional regular expression. When set, the rule applies only if it is
    #: found (with ``re.search``) in the ``message`` field of the JSON
    #: error response (or the raw response text), or in ``str(exc)`` for
    #: transport errors.
    message: Optional[str] = None
    #: When ``True`` and the response carries a ``Retry-After`` or
    #: ``RateLimit-Reset`` header, the header decides the sleep instead of
    #: the backoff.
    respect_retry_after: bool = True
    #: Sleep used, in seconds, when the header is present but cannot be
    #: parsed.
    retry_after_fallback: float = 10.0

    def applies(self, idempotent: bool, text: str) -> bool:
        """Whether the rule applies to a request that is (or is not)
        idempotent and failed with the given error text.
        """
        if self.idempotent_only and not idempotent:
            return False

        if self.message is not None and not re.search(self.message, text):
            return False

        return True

    def exhausted(self, retries_done: int) -> bool:
        """Whether the rule allows no more retries after ``retries_done``
        retries.
        """
        return self.max_retries is not None and retries_done >= self.max_retries

    def sleep_seconds(
        self,
        attempt: int,
        response: Optional[httpx.Response] = None,
        log: Callable[[int, Any], None] = _no_log,
    ) -> float:
        """How long to sleep before retry number ``attempt`` (0-based).

        :param attempt: number of retries already performed for this rule
        :param response: the failed response, used for the ``Retry-After``
                         and ``RateLimit-Reset`` headers
        :param log: logging callback ``(level, message)``
        """
        if self.respect_retry_after and response is not None:
            header = response.headers.get(
                "Retry-After", default=response.headers.get("RateLimit-Reset")
            )
            if header is not None:
                return parse_retry_after(header, log, self.retry_after_fallback)

        delay = self.backoff_factor * (2 ** attempt)
        if self.jitter:
            delay *= random.uniform(0.5, 1.5)

        return max(0.0, min(self.max_backoff, delay))


# Provisional defaults. The number of retries and the backoff of each entry
# are still to be agreed with the team; keep every tunable here so that the
# decision is a one-dictionary change.

#: The rules of a new :class:`RetryPolicy`. A status code is retried for
#: every request (including job submissions and transfers) only when the
#: server returns it before running any command on the cluster: 429, 503
#: (health check failed), 424 (SSH connection failed) and 502 (SSH
#: certificate service failed). Codes that can be returned after the command
#: has run, like 504 (command timed out), are restricted to idempotent
#: requests, so that a lost response never results in a duplicated job.
#: Connection errors are retried for every request; read timeouts only for
#: idempotent ones.
DEFAULT_RETRY_RULES: Dict[RuleKey, Union[RetryRule, List[RetryRule]]] = {
    # Rate limit: retried until a different status is received, sleeping as
    # instructed by the `Retry-After` header (10 s when the header is
    # missing). This is the behaviour the clients always had.
    429: RetryRule(
        max_retries=None,
        backoff_factor=10.0,
        max_backoff=10.0,
        jitter=False,
    ),
    # The health check of the system, the scheduler or the filesystem failed;
    # the request was rejected before reaching the cluster.
    503: RetryRule(),
    # The SSH connection to the cluster could not be established (or the
    # connection pool is full).
    424: RetryRule(),
    # The SSH certificate service failed; nothing was sent to the cluster.
    502: RetryRule(),
    # The command timed out on the cluster: it may have run, so only
    # idempotent requests are repeated.
    504: RetryRule(idempotent_only=True),
    # Transport errors raised by httpx before a response was received.
    httpx.ConnectError: RetryRule(),
    httpx.ConnectTimeout: RetryRule(),
    # The request may have reached the server; only repeat idempotent ones.
    httpx.ReadTimeout: RetryRule(idempotent_only=True),
    httpx.RemoteProtocolError: RetryRule(idempotent_only=True),
}


def _response_text(response: httpx.Response) -> str:
    """The ``message`` field of a JSON error response, or the raw text."""
    try:
        body = response.json()
        if isinstance(body, dict) and body.get("message") is not None:
            return str(body["message"])
    except ValueError:
        pass

    try:
        return response.text
    except Exception:
        return ""


@dataclasses.dataclass
class RetryPolicy:
    """The retry table of a client.

    Every request wrapper of the clients consults the policy after each
    attempt: a failed response is looked up by its status code and a
    transport error by its class. The first :class:`RetryRule` registered
    under the key that applies to the request is used, and the request is
    repeated until that rule's ``max_retries`` is reached. Each key has its
    own budget of retries.

    The clients create a new policy with a copy of
    :data:`firecrest.v2.retry.DEFAULT_RETRY_RULES`; edit :attr:`rules` in place or pass your
    own table to tune it.
    """

    #: The rule table. Keys are HTTP status codes or ``httpx`` exception
    #: classes; values are a :class:`RetryRule` or a list of rules that is
    #: searched in order.
    rules: Dict[RuleKey, Union[RetryRule, List[RetryRule]]] = dataclasses.field(
        default_factory=lambda: copy.deepcopy(DEFAULT_RETRY_RULES)
    )

    @classmethod
    def disabled(cls) -> "RetryPolicy":
        """A policy that only keeps the rate limit (429) rule, i.e. the
        behaviour of the clients before the retry policy was introduced.
        """
        return cls(rules={429: copy.deepcopy(DEFAULT_RETRY_RULES[429])})

    def rules_for(self, key: RuleKey) -> List[RetryRule]:
        """The rules registered under ``key``, always as a list."""
        value = self.rules.get(key)
        if value is None:
            return []

        return list(value) if isinstance(value, list) else [value]

    def _exception_keys(self, exc: BaseException) -> List[RuleKey]:
        keys: List[RuleKey] = [
            k for k in self.rules if isinstance(k, type) and isinstance(exc, k)
        ]
        # Prefer the exact class, then the dictionary order
        keys.sort(key=lambda k: 0 if k is type(exc) else 1)
        return keys

    def match(
        self,
        idempotent: bool,
        response: Optional[httpx.Response] = None,
        exc: Optional[BaseException] = None,
    ) -> Optional[Tuple[RuleKey, RetryRule]]:
        """Find the rule that applies to a failed attempt.

        :param idempotent: whether the request is safe to repeat
        :param response: the response of the attempt, when one was received
        :param exc: the transport error of the attempt, when one was raised
        :return: the ``(key, rule)`` pair of the first applicable rule, or
                 ``None`` when the attempt must not be retried
        """
        if response is not None:
            keys: List[RuleKey] = [response.status_code]
            text = (
                _response_text(response)
                if any(
                    rule.message is not None
                    for rule in self.rules_for(response.status_code)
                )
                else ""
            )
        elif exc is not None:
            keys = self._exception_keys(exc)
            text = str(exc)
        else:
            return None

        for key in keys:
            for rule in self.rules_for(key):
                if rule.applies(idempotent, text):
                    return key, rule

        return None

__all__ = ["DEFAULT_RETRY_RULES", "RetryPolicy", "RetryRule", "RuleKey"]
