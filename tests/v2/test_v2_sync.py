import json
import pytest

from context_v2 import (ApiKeyAuth,
                       Firecrest,
                       correlation_id,
                       NotImplementedOnAPIversion,
                       UnexpectedStatusException)
from werkzeug.wrappers import Response
from werkzeug.wrappers import Request
from handlers import (fc_server,
                      filesystem_handler,
                      read_json_file,
                      status_handler,
                      submit_handler)


@pytest.fixture
def valid_client(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    client = Firecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=ValidAuthorization()
    )
    client.set_api_version("2.99.0") # Set to a high enough version for all tests
    return client

@pytest.fixture
def invalid_client(fc_server):
    class InvalidAuthorization:
        def get_access_token(self):
            return "INVALID_TOKEN"

    return Firecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=InvalidAuthorization()
    )


def test_systems(valid_client):
    data = read_json_file("v2/responses/systems.json")
    resp = valid_client.systems()
    assert resp == data["response"]["systems"]


def test_partitions(valid_client):
    data = read_json_file("v2/responses/partitions.json")
    resp = valid_client.partitions("cluster")
    assert resp == data["response"]["partitions"]


def test_nodes(valid_client):
    data = read_json_file("v2/responses/nodes.json")
    resp = valid_client.nodes("cluster")
    assert resp == data["response"]["nodes"]


def test_reservations(valid_client):
    data = read_json_file("v2/responses/reservations.json")
    resp = valid_client.reservations("cluster")
    assert resp == data["response"]["reservations"]


def test_userinfo(valid_client):
    data = read_json_file("v2/responses/userinfo.json")
    resp = valid_client.userinfo("cluster")
    assert resp == data["response"]


def test_head(valid_client):
    data = read_json_file("v2/responses/head.json")
    resp = valid_client.head("cluster", "/path/to/file")
    assert resp == data["response"]["output"]


def test_head_bytes(valid_client):
    data = read_json_file("v2/responses/head_bytes.json")
    resp = valid_client.head("cluster", "/path/to/file", num_bytes=8)
    assert resp == data["response"]["output"]


def test_head_bytes_exclude_trailing(valid_client):
    data = read_json_file("v2/responses/head_bytes_exclude_trailing.json")
    resp = valid_client.head("cluster", "/path/to/file",
                             num_bytes=8, exclude_trailing=True)
    assert resp == data["response"]["output"]


def test_head_lines(valid_client):
    data = read_json_file("v2/responses/head_lines.json")
    resp = valid_client.head("cluster", "/path/to/file",
                             num_lines=4)
    assert resp == data["response"]["output"]


def test_head_lines_exclude_trailing(valid_client):
    data = read_json_file("v2/responses/head_lines_exclude_trailing.json")
    resp = valid_client.head("cluster", "/path/to/file",
                             exclude_trailing=True, num_lines=4)
    assert resp == data["response"]["output"]

def test_head_lines_and_bytes(valid_client):
    with pytest.raises(ValueError) as excinfo:
        valid_client.head("cluster", "/path/to/file", num_bytes=8,
                          num_lines=4)

    assert str(excinfo.value) == (
        "You cannot specify both `num_bytes` and `num_lines`."
    )


def test_tail(valid_client):
    data = read_json_file("v2/responses/tail.json")
    resp = valid_client.tail("cluster", "/path/to/file")
    assert resp == data["response"]["output"]


def test_tail_bytes(valid_client):
    data = read_json_file("v2/responses/tail_bytes.json")
    resp = valid_client.tail("cluster", "/path/to/file", num_bytes=8)
    assert resp == data["response"]["output"]


def test_tail_bytes_exclude_beginning(valid_client):
    data = read_json_file("v2/responses/tail_bytes_exclude_beginning.json")
    resp = valid_client.tail("cluster", "/path/to/file",
                             num_bytes=8, exclude_beginning=True)
    assert resp == data["response"]["output"]


def test_tail_lines(valid_client):
    data = read_json_file("v2/responses/tail_lines.json")
    resp = valid_client.tail("cluster", "/path/to/file",
                             num_lines=4)
    assert resp == data["response"]["output"]


def test_tail_lines_exclude_beginning(valid_client):
    data = read_json_file("v2/responses/tail_lines_exclude_beginning.json")
    resp = valid_client.tail("cluster", "/path/to/file",
                             exclude_beginning=True, num_lines=4)
    assert resp == data["response"]["output"]

def test_tail_lines_and_bytes(valid_client):
    with pytest.raises(ValueError) as excinfo:
        valid_client.tail("cluster", "/path/to/file", num_bytes=8,
                          num_lines=4)

    assert str(excinfo.value) == (
        "You cannot specify both `num_bytes` and `num_lines`."
    )


def test_ls(valid_client):
    data = read_json_file("v2/responses/ls.json")
    resp = valid_client.list_files("cluster", "/home/user")
    assert resp == data["response"]["output"]


def test_ls_dereference(valid_client):
    data = read_json_file("v2/responses/ls_dereference.json")
    resp = valid_client.list_files("cluster", "/home/user",
                                   dereference=True)
    assert resp == data["response"]["output"]


def test_ls_hidden(valid_client):
    data = read_json_file("v2/responses/ls_hidden.json")
    resp = valid_client.list_files("cluster", "/home/user",
                                   show_hidden=True)

    assert resp == data["response"]["output"]


def test_ls_recursive(valid_client):
    data = read_json_file("v2/responses/ls_recursive.json")
    resp = valid_client.list_files("cluster", "/home/user",
                                   recursive=True)

    assert resp == data["response"]["output"]


def test_ls_uid(valid_client):
    data = read_json_file("v2/responses/ls_uid.json")
    resp = valid_client.list_files("cluster", "/home/user",
                                   numeric_uid=True)

    assert resp == data["response"]["output"]


def test_ls_invalid_path(valid_client):
    data = read_json_file("v2/responses/ls_invalid_path.json")
    with pytest.raises(UnexpectedStatusException) as excinfo:
        valid_client.list_files("cluster", "/invalid/path")

    byte_content = excinfo.value.responses[-1].content
    decoded_string = byte_content.decode('utf-8')
    response_dict = json.loads(decoded_string)
    message = response_dict["message"]

    assert str(message) == (
        "ls: cannot access '/invalid/path': No such file or directory"
    )


def test_view(valid_client):
    data = read_json_file("v2/responses/view.json")
    resp = valid_client.view("cluster", "/home/user/file")

    assert resp == data["response"]["output"]


def test_stat(valid_client):
    data = read_json_file("v2/responses/stat.json")
    resp = valid_client.stat("cluster", "/home/user/file")

    assert resp == data["response"]["output"]


def test_stat_dereference(valid_client):
    data = read_json_file("v2/responses/stat_dereference.json")
    resp = valid_client.stat("cluster", "/home/user/file",
                             dereference=True)

    assert resp == data["response"]["output"]


def test_file_type(valid_client):
    data = read_json_file("v2/responses/file.json")
    resp = valid_client.file_type("cluster", "/home/user/file")

    assert resp == data["response"]["output"]


def test_checksum(valid_client):
    data = read_json_file("v2/responses/checksum.json")
    resp = valid_client.checksum("cluster", "/home/user/file")

    assert resp == data["response"]["output"]


def test_mkdir(valid_client):
    data = read_json_file("v2/responses/mkdir.json")
    resp = valid_client.mkdir("cluster", "/home/user/file")

    assert resp == data["response"]["output"]


def test_chown(valid_client):
    data = read_json_file("v2/responses/chown.json")
    resp = valid_client.chown("cluster", "/home/user/file",
                              "test1", "users")

    assert resp == data["response"]["output"]


def test_chown_not_permitted(valid_client):
    data = read_json_file("v2/responses/chown_not_permitted.json")
    with pytest.raises(UnexpectedStatusException) as excinfo:
        valid_client.chown("cluster", "/home/test1/xxx",
                           "test1", "users")

    exc = excinfo.value
    assert str(exc) == (
        f"last request: 403 {data['response']}"
        f" [X-Request-ID: {exc.request_id},"
        f" X-Correlation-ID: {exc.correlation_id}]: expected status 200"
    )


def test_chmod(valid_client):
    data = read_json_file("v2/responses/chmod.json")
    resp = valid_client.chmod("cluster", "/home/user/xxx",
                              "777")

    assert resp == data["response"]["output"]


def test_rm(valid_client):
    data = read_json_file("v2/responses/rm.json")
    resp = valid_client.rm("cluster", "/home/user/file")

    assert resp == data["response"]# ["output"]


def test_job_info(valid_client):
    data = read_json_file("v2/responses/job_info.json")
    resp = valid_client.job_info("cluster")

    assert resp == data["response"]["jobs"]


def test_job_info_jobid(valid_client):
    data = read_json_file("v2/responses/job_info.json")
    resp = valid_client.job_info("cluster", "1")

    assert resp == data["response"]["jobs"]


def test_job_allusers(valid_client):
    data = read_json_file("v2/responses/job_info_all_users.json")
    resp = valid_client.job_info("cluster", allusers=True)

    assert resp == data["response"]["jobs"]


def test_job_info_account(valid_client):
    data = read_json_file("v2/responses/job_info_account.json")
    resp = valid_client.job_info("cluster", account="users2")

    assert resp == data["response"]["jobs"]


def test_job_info_name(valid_client):
    data = read_json_file("v2/responses/job_info_name.json")
    resp = valid_client.job_info("cluster", name="allocation")

    assert resp == data["response"]["jobs"]


def test_job_info_name_old_api(valid_client):
    valid_client.set_api_version("2.5.7")
    with pytest.raises(NotImplementedOnAPIversion):
        valid_client.job_info("cluster", name="allocation")


def test_job_info_time_window(valid_client):
    data = read_json_file("v2/responses/job_info_time_window.json")
    resp = valid_client.job_info("cluster", time_window="7d")

    assert resp == data["response"]["jobs"]


def test_job_info_time_window_old_api(valid_client):
    valid_client.set_api_version("2.5.7")
    with pytest.raises(NotImplementedOnAPIversion):
        valid_client.job_info("cluster", time_window="7d")


def test_job_metadata(valid_client):
    data = read_json_file("v2/responses/job_metadata.json")
    resp = valid_client.job_metadata("cluster", "1")

    assert resp == data["response"]["jobs"]


def test_job_submit(valid_client):
    data = read_json_file("v2/responses/job_submit.json")
    resp = valid_client.submit("cluster", "/path/to/dir",
                               script_str="...")

    assert resp == data["response"]


def test_job_submit_partition_reservation(valid_client):
    data = read_json_file("v2/responses/job_submit.json")
    resp = valid_client.submit("cluster", "/path/to/dir",
                               script_str="...",
                               partition="part_a",
                               reservation="res_a")

    assert resp == data["response"]


def test_job_submit_partition_old_api(valid_client):
    valid_client.set_api_version("2.5.3")
    with pytest.raises(NotImplementedOnAPIversion):
        valid_client.submit("cluster", "/path/to/dir",
                            script_str="...", partition="part_a")


def test_job_submit_reservation_old_api(valid_client):
    valid_client.set_api_version("2.5.3")
    with pytest.raises(NotImplementedOnAPIversion):
        valid_client.submit("cluster", "/path/to/dir",
                            script_str="...", reservation="res_a")


def test_job_submit_no_script(valid_client):
    with pytest.raises(ValueError) as excinfo:
        valid_client.submit("cluster", "/path/to/dir")

    assert str(excinfo.value) == (
        "Exactly one of the arguments `script_str`, "
        "`script_local_path` or `script_remote_path` must be set."
    )


def _version_header_client(httpserver, app_version):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    headers = {"f7t-appversion": app_version} if app_version else None
    data = read_json_file("v2/responses/systems.json")
    httpserver.expect_request(
        "/status/systems", method="GET"
    ).respond_with_json(data["response"], headers=headers)
    return Firecrest(
        firecrest_url=httpserver.url_for("/"),
        authorization=ValidAuthorization()
    )


def test_api_version_autodetect(httpserver):
    client = _version_header_client(httpserver, "2.6.1")
    assert str(client._api_version) == "2.6.0"
    client.systems()
    assert str(client._api_version) == "2.6.1"


def test_api_version_autodetect_invalid_version(httpserver):
    client = _version_header_client(httpserver, "2.x.x")
    client.systems()
    assert str(client._api_version) == "2.6.0"


def test_api_version_autodetect_no_header(httpserver):
    client = _version_header_client(httpserver, None)
    client.systems()
    assert str(client._api_version) == "2.6.0"


def test_api_version_explicit_disables_autodetect(httpserver):
    client = _version_header_client(httpserver, "2.6.1")
    client.set_api_version("2.3.0")
    client.systems()
    assert str(client._api_version) == "2.3.0"


def test_tracing_headers(valid_client, fc_server):
    valid_client.systems()
    valid_client.partitions("cluster")

    requests = [req for req, _ in fc_server.log]
    assert len(requests) == 2
    for req in requests:
        assert req.headers["X-Request-ID"]
        assert req.headers["X-Correlation-ID"]

    # Every request gets a fresh request ID and every method call gets its
    # own auto-generated correlation ID
    assert (
        requests[0].headers["X-Request-ID"] !=
        requests[1].headers["X-Request-ID"]
    )
    assert (
        requests[0].headers["X-Correlation-ID"] !=
        requests[1].headers["X-Correlation-ID"]
    )


def test_user_provided_correlation_id(valid_client, fc_server):
    with correlation_id("my-workflow-id"):
        valid_client.systems()
        valid_client.partitions("cluster")

    requests = [req for req, _ in fc_server.log]
    assert len(requests) == 2
    for req in requests:
        assert req.headers["X-Correlation-ID"] == "my-workflow-id"

    # The request IDs are still unique per request
    assert (
        requests[0].headers["X-Request-ID"] !=
        requests[1].headers["X-Request-ID"]
    )


def test_exception_carries_tracing_ids(invalid_client, fc_server):
    with pytest.raises(UnexpectedStatusException) as excinfo:
        invalid_client.systems()

    exc = excinfo.value
    requests = [req for req, _ in fc_server.log]
    assert exc.request_id == requests[-1].headers["X-Request-ID"]
    assert exc.correlation_id == requests[-1].headers["X-Correlation-ID"]
    # The IDs show up in the exception message
    assert exc.request_id in str(exc)
    assert exc.correlation_id in str(exc)


def test_api_key_auth(fc_server):
    data = read_json_file("v2/responses/systems.json")
    client = Firecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=ApiKeyAuth("VALID_KEY"),
    )
    client.set_api_version("2.99.0")
    assert client.systems() == data["response"]["systems"]


def test_api_key_auth_invalid(fc_server):
    client = Firecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=ApiKeyAuth("INVALID_KEY"),
    )
    client.set_api_version("2.99.0")
    with pytest.raises(UnexpectedStatusException):
        client.systems()


def test_custom_auth_headers(fc_server):
    class CustomAuthorization:
        def auth_headers(self):
            return {"Authorization": "Bearer VALID_TOKEN"}

    data = read_json_file("v2/responses/systems.json")
    client = Firecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=CustomAuthorization(),
    )
    client.set_api_version("2.99.0")
    assert client.systems() == data["response"]["systems"]


# Retry policy tests

import httpx  # noqa: E402
from unittest.mock import Mock  # noqa: E402
from context_v2 import RetryPolicy, RetryRule  # noqa: E402


def _no_sleep(policy):
    for key in list(policy.rules):
        for rule in policy.rules_for(key):
            rule.backoff_factor = 0
            rule.max_backoff = 0

    return policy


def _hits(fc_server, path, method=None):
    return [
        req for req, _ in fc_server.log
        if req.path == path and (method is None or req.method == method)
    ]


def _fail_once(fc_server, path, method, status, body=None, headers=None):
    body = body if body is not None else {"message": f"failure {status}"}
    fc_server.expect_oneshot_request(path, method=method).respond_with_json(
        body, status=status, headers=headers
    )


@pytest.fixture
def retry_client(valid_client):
    _no_sleep(valid_client.retry_policy)
    return valid_client


def test_retry_transient_status_then_success(retry_client, fc_server):
    data = read_json_file("v2/responses/systems.json")
    _fail_once(fc_server, "/status/systems", "GET", 503)
    _fail_once(fc_server, "/status/systems", "GET", 424)
    _fail_once(fc_server, "/status/systems", "GET", 502)
    resp = retry_client.systems()
    assert resp == data["response"]["systems"]
    assert len(_hits(fc_server, "/status/systems")) == 4


def test_retry_budget_exhausted(retry_client, fc_server):
    retry_client.retry_policy.rules[503].max_retries = 1
    _fail_once(fc_server, "/status/systems", "GET", 503)
    _fail_once(fc_server, "/status/systems", "GET", 503)
    with pytest.raises(UnexpectedStatusException) as excinfo:
        retry_client.systems()

    assert len(_hits(fc_server, "/status/systems")) == 2
    last = excinfo.value.responses[-1]
    assert last.status_code == 503
    assert last.extensions["firecrest_retries"] == 1


def test_retry_504_only_for_idempotent_requests(retry_client, fc_server):
    # Reading the job information is safe to repeat
    _fail_once(fc_server, "/compute/cluster/jobs/1", "GET", 504)
    resp = retry_client.job_info("cluster", "1")
    assert isinstance(resp, list)
    assert len(_hits(fc_server, "/compute/cluster/jobs/1")) == 2

    # The submission may have gone through before the timeout: not retried
    _fail_once(fc_server, "/compute/cluster/jobs", "POST", 504)
    with pytest.raises(UnexpectedStatusException) as excinfo:
        retry_client.submit("cluster", "/path/to/dir", script_str="...")

    assert excinfo.value.responses[-1].status_code == 504
    assert len(_hits(fc_server, "/compute/cluster/jobs", "POST")) == 1


def test_retry_pre_command_status_on_submit(retry_client, fc_server):
    data = read_json_file("v2/responses/job_submit.json")
    _fail_once(fc_server, "/compute/cluster/jobs", "POST", 503)
    resp = retry_client.submit("cluster", "/path/to/dir", script_str="...")
    assert resp == data["response"]
    assert len(_hits(fc_server, "/compute/cluster/jobs", "POST")) == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404, 500, 501])
def test_no_retry_on_other_statuses(retry_client, fc_server, status):
    _fail_once(fc_server, "/status/systems", "GET", status)
    with pytest.raises(UnexpectedStatusException) as excinfo:
        retry_client.systems()

    assert excinfo.value.responses[-1].status_code == status
    assert len(_hits(fc_server, "/status/systems")) == 1


def test_retry_rule_with_message_pattern(retry_client, fc_server):
    retry_client.retry_policy.rules[500] = RetryRule(
        message="exit status:124", backoff_factor=0
    )
    _fail_once(fc_server, "/status/systems", "GET", 500,
               body={"message": "Remote process failed with exit status:124"})
    retry_client.systems()
    assert len(_hits(fc_server, "/status/systems")) == 2

    _fail_once(fc_server, "/status/systems", "GET", 500,
               body={"message": "Remote process failed with exit status:1"})
    with pytest.raises(UnexpectedStatusException):
        retry_client.systems()

    assert len(_hits(fc_server, "/status/systems")) == 3


def test_retry_rate_limit(retry_client, fc_server):
    _fail_once(fc_server, "/status/systems", "GET", 429,
               headers={"Retry-After": "0"})
    retry_client.systems()
    assert len(_hits(fc_server, "/status/systems")) == 2

    retry_client.num_retries_rate_limit = 1
    _fail_once(fc_server, "/status/systems", "GET", 429,
               headers={"Retry-After": "0"})
    _fail_once(fc_server, "/status/systems", "GET", 429,
               headers={"Retry-After": "0"})
    with pytest.raises(UnexpectedStatusException) as excinfo:
        retry_client.systems()

    assert excinfo.value.responses[-1].status_code == 429
    assert len(_hits(fc_server, "/status/systems")) == 4


def test_retry_budgets_are_per_rule(retry_client, fc_server):
    retry_client.retry_policy.rules[503].max_retries = 1
    retry_client.num_retries_rate_limit = 1
    _fail_once(fc_server, "/status/systems", "GET", 429,
               headers={"Retry-After": "0"})
    _fail_once(fc_server, "/status/systems", "GET", 503)
    retry_client.systems()
    assert len(_hits(fc_server, "/status/systems")) == 3


def test_retry_transport_errors(retry_client, monkeypatch):
    rules = retry_client.retry_policy.rules
    get = Mock(side_effect=httpx.ConnectError("connection refused"))
    monkeypatch.setattr(retry_client._session, "get", get)
    with pytest.raises(httpx.ConnectError):
        retry_client.systems()

    assert get.call_count == 1 + rules[httpx.ConnectError].max_retries

    # A read timeout may have reached the server: a submission is not
    # repeated...
    post = Mock(side_effect=httpx.ReadTimeout("timed out"))
    monkeypatch.setattr(retry_client._session, "post", post)
    with pytest.raises(httpx.ReadTimeout):
        retry_client.submit("cluster", "/path/to/dir", script_str="...")

    assert post.call_count == 1

    # ...but a read is
    get = Mock(side_effect=httpx.ReadTimeout("timed out"))
    monkeypatch.setattr(retry_client._session, "get", get)
    with pytest.raises(httpx.ReadTimeout):
        retry_client.systems()

    assert get.call_count == 1 + rules[httpx.ReadTimeout].max_retries


def test_disabled_retry_policy(fc_server):
    class ValidAuthorization:
        def get_access_token(self):
            return "VALID_TOKEN"

    client = Firecrest(
        firecrest_url=fc_server.url_for("/"),
        authorization=ValidAuthorization(),
        retry_policy=RetryPolicy.disabled(),
    )
    _fail_once(fc_server, "/status/systems", "GET", 503)
    with pytest.raises(UnexpectedStatusException):
        client.systems()

    assert len(_hits(fc_server, "/status/systems")) == 1


def test_cancel_job_already_completed_after_retry(retry_client, fc_server):
    already_done = {
        "message": "scancel: error: Kill job error on job id 1: "
                   "Job/step already completing or completed"
    }
    _fail_once(fc_server, "/compute/cluster/jobs/1", "DELETE", 504)
    _fail_once(fc_server, "/compute/cluster/jobs/1", "DELETE", 500,
               body=already_done)
    assert retry_client.cancel_job("cluster", "1") == {}
    assert len(_hits(fc_server, "/compute/cluster/jobs/1", "DELETE")) == 2

    # Without a previous attempt the error is reported as usual
    _fail_once(fc_server, "/compute/cluster/jobs/1", "DELETE", 500,
               body=already_done)
    with pytest.raises(UnexpectedStatusException):
        retry_client.cancel_job("cluster", "1")

    assert len(_hits(fc_server, "/compute/cluster/jobs/1", "DELETE")) == 3


def test_wait_for_job_propagates_errors(retry_client, fc_server):
    retry_client.retry_policy.rules[503].max_retries = 0
    _fail_once(fc_server, "/compute/cluster/jobs/1", "GET", 503)
    with pytest.raises(UnexpectedStatusException) as excinfo:
        retry_client.wait_for_job("cluster", "1")

    assert excinfo.value.responses[-1].status_code == 503


def test_retry_small_upload_keeps_body(retry_client, fc_server, tmp_path):
    content = b"hello from the retry test"
    local_file = tmp_path / "file.txt"
    local_file.write_bytes(content)

    def upload_handler(request: Request):
        assert request.files["file"].filename == "remote.txt"
        assert request.files["file"].read() == content
        return Response(status=204)

    _fail_once(fc_server, "/filesystem/cluster/ops/upload", "POST", 503)
    fc_server.expect_request(
        "/filesystem/cluster/ops/upload", method="POST"
    ).respond_with_handler(upload_handler)
    retry_client.upload("cluster", local_file, "/home/user", "remote.txt")
    assert len(_hits(fc_server, "/filesystem/cluster/ops/upload", "POST")) == 2
