import httpx
import pytest

from context_v2 import (
    AsyncFirecrest,
    Firecrest,
    RequestRecord,
    RequestStats,
    RetryPolicy,
    RetryRule,
    UnexpectedStatusException,
    correlation_id,
)
from handlers import fc_server, status_handler  # noqa


class ValidAuthorization:
    def get_access_token(self):
        return "VALID_TOKEN"


@pytest.fixture
def client(fc_server):
    client = Firecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=ValidAuthorization(),
        retry_policy=RetryPolicy.disabled(),
    )
    client.set_api_version("2.99.0")
    return client


@pytest.fixture
def async_client(fc_server):
    client = AsyncFirecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=ValidAuthorization(),
        retry_policy=RetryPolicy.disabled(),
    )
    client.set_api_version("2.99.0")
    return client


def _record(**kwargs):
    defaults = dict(
        method="GET",
        endpoint="/status/systems",
        params={},
        status_code=200,
        error=None,
        duration=0.1,
        timestamp=0.0,
        attempt=0,
        request_id="id",
        correlation_id=None,
    )
    defaults.update(kwargs)
    return RequestRecord(**defaults)


def test_stats_counters():
    stats = RequestStats(max_records=2)
    stats.record(_record())
    stats.record(_record(status_code=503))
    stats.record(_record(status_code=None, error="ConnectError", attempt=1))
    stats.record(_record(method="POST", endpoint="/compute/c/jobs", status_code=201))

    assert stats.total == 4
    assert stats.retries == 1
    assert stats.failed == 2
    assert stats.total_duration == pytest.approx(0.4)
    assert stats.by_method == {"GET": 3, "POST": 1}
    assert stats.by_endpoint == {"GET /status/systems": 3, "POST /compute/c/jobs": 1}
    assert stats.by_status == {200: 1, 503: 1, "ConnectError": 1, 201: 1}
    # Only the last `max_records` are kept
    assert [r.status_code for r in stats.records] == [None, 201]
    assert stats.max_records == 2

    d = stats.as_dict()
    assert d["total"] == 4
    assert d["by_status"] == {"200": 1, "503": 1, "ConnectError": 1, "201": 1}
    assert len(d["records"]) == 2
    assert d["records"][1]["endpoint"] == "/compute/c/jobs"

    stats.reset()
    assert stats.total == 0
    assert stats.records == []
    assert stats.by_endpoint == {}


def test_stats_disabled_and_listeners():
    stats = RequestStats(enabled=False)
    stats.record(_record())
    assert stats.total == 0
    assert stats.add("GET", "/x", response=httpx.Response(200)) is None

    stats.enabled = True
    seen = []
    stats.listeners.append(seen.append)
    stats.record(_record())
    assert seen == [_record()]


def test_add_builds_record_from_response_and_exception():
    stats = RequestStats()
    request = httpx.Request(
        "GET", "http://x/status/systems",
        headers={"X-Request-ID": "rid", "X-Correlation-ID": "cid"},
    )
    resp = httpx.Response(200, request=request)
    r = stats.add(
        "GET", "/status/systems", params={"a": 1, "b": None},
        timestamp=5.0, duration=0.5, response=resp
    )
    assert r == RequestRecord(
        method="GET", endpoint="/status/systems", params={"a": 1},
        status_code=200, error=None, duration=0.5, timestamp=5.0,
        attempt=0, request_id="rid", correlation_id="cid",
    )
    assert r.key == "GET /status/systems"
    assert not r.failed

    # Exception with a request attached
    exc = httpx.ConnectError("boom", request=request)
    r = stats.add("GET", "/status/systems", attempt=2, exc=exc)
    assert r.status_code is None
    assert r.error == "ConnectError"
    assert r.request_id == "rid"
    assert r.attempt == 2
    assert r.failed

    # Exception without a request: no crash, correlation id from the caller
    r = stats.add("GET", "/x", exc=httpx.ConnectError("boom"), correlation_id="c")
    assert r.request_id is None
    assert r.correlation_id == "c"

    assert stats.total == 3
    assert stats.retries == 1


def test_client_records_requests(client):
    client.systems()
    with correlation_id("my-corr"):
        client.partitions("cluster")

    stats = client.request_stats
    assert stats.total == 2
    assert stats.failed == 0
    assert stats.by_method == {"GET": 2}
    assert stats.by_status == {200: 2}
    assert stats.by_endpoint == {
        "GET /status/systems": 1,
        "GET /status/cluster/partitions": 1,
    }
    first, second = stats.records
    assert first.request_id is not None
    # Public methods always set a correlation ID, the context overrides it
    assert first.correlation_id is not None
    assert second.correlation_id == "my-corr"
    assert second.duration > 0
    assert second.timestamp > first.timestamp


def _fail_once(fc_server, path, method, status):
    fc_server.expect_oneshot_request(path, method=method).respond_with_json(
        {"message": f"failure {status}"}, status=status
    )


def test_client_records_failed_requests(client, fc_server):
    _fail_once(fc_server, "/status/systems", "GET", 404)
    with pytest.raises(UnexpectedStatusException):
        client.systems()

    stats = client.request_stats
    assert stats.total == 1
    assert stats.failed == 1
    assert stats.records[0].status_code == 404
    assert stats.records[0].failed


def test_client_records_retries(client, fc_server):
    client.retry_policy.rules[503] = RetryRule(
        max_retries=2, backoff_factor=0, jitter=False
    )
    for _ in range(3):
        _fail_once(fc_server, "/status/systems", "GET", 503)

    with pytest.raises(UnexpectedStatusException):
        client.systems()

    stats = client.request_stats
    assert stats.total == 3
    assert stats.retries == 2
    assert stats.by_status == {503: 3}
    assert [r.attempt for r in stats.records] == [0, 1, 2]
    assert len({r.request_id for r in stats.records}) == 3


def test_client_records_transport_errors():
    client = Firecrest(
        "http://localhost:1", ValidAuthorization(),
        retry_policy=RetryPolicy.disabled(),
    )
    with pytest.raises(httpx.ConnectError):
        client.systems()

    stats = client.request_stats
    assert stats.total == 1
    assert stats.failed == 1
    assert stats.by_status == {"ConnectError": 1}
    assert stats.records[0].status_code is None
    assert stats.records[0].request_id is not None


def test_stats_can_be_shared_and_disabled(fc_server):
    shared = RequestStats()
    c1 = Firecrest(fc_server.url_for("/"), ValidAuthorization(), request_stats=shared)
    c2 = Firecrest(fc_server.url_for("/"), ValidAuthorization(), request_stats=shared)
    assert c1.request_stats is shared and c2.request_stats is shared
    c1.systems()
    c2.systems()
    assert shared.total == 2

    shared.enabled = False
    c1.systems()
    assert shared.total == 2


@pytest.mark.asyncio
async def test_async_client_records_requests(async_client):
    await async_client.systems()
    await async_client.partitions("cluster")
    stats = async_client.request_stats
    assert stats.total == 2
    assert stats.by_endpoint == {
        "GET /status/systems": 1,
        "GET /status/cluster/partitions": 1,
    }
    assert all(r.status_code == 200 for r in stats.records)
