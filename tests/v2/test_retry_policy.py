import httpx
import pytest

from context_v2 import AsyncFirecrest, Firecrest, RetryPolicy, RetryRule
from firecrest.v2 import DEFAULT_RETRY_RULES


def _response(status, headers=None, body=None):
    return httpx.Response(status, headers=headers, json=body)


def test_sleep_exponential_without_jitter():
    rule = RetryRule(backoff_factor=1.0, jitter=False, max_backoff=5)
    assert [rule.sleep_seconds(i) for i in range(4)] == [1, 2, 4, 5]


def test_sleep_jitter_stays_within_bounds():
    rule = RetryRule(backoff_factor=2.0, jitter=True, max_backoff=100)
    for _ in range(50):
        assert 1.0 <= rule.sleep_seconds(0) <= 3.0

    # The cap is applied after the jitter
    rule = RetryRule(backoff_factor=2.0, jitter=True, max_backoff=2.5)
    for _ in range(50):
        assert rule.sleep_seconds(0) <= 2.5


def test_sleep_retry_after_header_takes_precedence():
    rule = RetryRule(backoff_factor=1.0, jitter=False)
    assert rule.sleep_seconds(0, _response(503, {"Retry-After": "7"})) == 7
    assert rule.sleep_seconds(0, _response(429, {"RateLimit-Reset": "3"})) == 3
    # Unparsable header falls back to `retry_after_fallback`
    rule = RetryRule(jitter=False, retry_after_fallback=42)
    assert rule.sleep_seconds(0, _response(503, {"Retry-After": "soon"})) == 42
    # The header can be ignored
    rule = RetryRule(backoff_factor=1.0, jitter=False, respect_retry_after=False)
    assert rule.sleep_seconds(0, _response(503, {"Retry-After": "7"})) == 1
    # No header: backoff
    assert rule.sleep_seconds(1, _response(503)) == 2


def test_rule_exhausted():
    assert RetryRule(max_retries=2).exhausted(2)
    assert not RetryRule(max_retries=2).exhausted(1)
    assert not RetryRule(max_retries=None).exhausted(10 ** 6)
    assert RetryRule(max_retries=0).exhausted(0)


def test_match_status_and_idempotency():
    policy = RetryPolicy()
    assert policy.match(idempotent=False, response=_response(503)) == (
        503, policy.rules[503]
    )
    assert policy.match(idempotent=True, response=_response(504)) == (
        504, policy.rules[504]
    )
    assert policy.match(idempotent=False, response=_response(504)) is None
    assert policy.match(idempotent=True, response=_response(500)) is None
    assert policy.match(idempotent=True, response=_response(400)) is None
    assert policy.match(idempotent=True) is None


def test_match_message_and_rule_order():
    policy = RetryPolicy(rules={
        500: [
            RetryRule(message="exit status:124", max_retries=5),
            RetryRule(message="Slurm", idempotent_only=True),
        ]
    })
    timeout = _response(500, body={"message": "failed with exit status:124"})
    key, rule = policy.match(idempotent=False, response=timeout)
    assert (key, rule.max_retries) == (500, 5)
    slurm = _response(500, body={"message": "Slurm error"})
    assert policy.match(idempotent=False, response=slurm) is None
    assert policy.match(idempotent=True, response=slurm) == (
        500, policy.rules[500][1]
    )
    # Non-JSON bodies are matched against the raw text
    raw = httpx.Response(500, text="Slurm is down")
    assert policy.match(idempotent=True, response=raw) is not None


def test_match_exceptions_prefers_exact_class():
    policy = RetryPolicy(rules={
        httpx.TransportError: RetryRule(max_retries=1),
        httpx.ConnectError: RetryRule(max_retries=7),
    })
    key, rule = policy.match(idempotent=False, exc=httpx.ConnectError("x"))
    assert (key, rule.max_retries) == (httpx.ConnectError, 7)
    key, rule = policy.match(idempotent=False, exc=httpx.ReadError("x"))
    assert (key, rule.max_retries) == (httpx.TransportError, 1)
    assert policy.match(idempotent=False, exc=ValueError("x")) is None
    # The message pattern is matched against str(exc)
    policy = RetryPolicy(rules={httpx.ConnectError: RetryRule(message="refused")})
    assert policy.match(idempotent=True, exc=httpx.ConnectError("timed out")) is None
    assert policy.match(idempotent=True, exc=httpx.ConnectError("refused")) is not None


def test_default_rules_are_copied_per_policy():
    policy = RetryPolicy()
    policy.rules[503].max_retries = 99
    assert DEFAULT_RETRY_RULES[503].max_retries != 99
    assert RetryPolicy().rules[503].max_retries != 99


def test_disabled_policy_keeps_rate_limit_rule():
    policy = RetryPolicy.disabled()
    assert set(policy.rules) == {429}
    assert policy.rules[429].max_retries is None


@pytest.mark.parametrize("client_class", [AsyncFirecrest, Firecrest])
def test_num_retries_rate_limit_alias(client_class):
    client = client_class("http://localhost", authorization=None)
    assert client.num_retries_rate_limit is None
    client.num_retries_rate_limit = 3
    assert client.retry_policy.rules[429].max_retries == 3
    client.retry_policy.rules[429].max_retries = 5
    assert client.num_retries_rate_limit == 5
    # Without a 429 rule the setter creates one
    del client.retry_policy.rules[429]
    assert client.num_retries_rate_limit == 0
    client.num_retries_rate_limit = 2
    assert client.retry_policy.rules[429] == RetryRule(max_retries=2)


def test_client_accepts_policy_in_constructor():
    policy = RetryPolicy.disabled()
    assert Firecrest("http://localhost", None, retry_policy=policy).retry_policy is policy
    assert AsyncFirecrest("http://localhost", None, retry_policy=policy).retry_policy is policy
