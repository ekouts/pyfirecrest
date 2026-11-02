#
#  Copyright (c) 2024, ETH Zurich. All rights reserved.
#
#  Please, refer to the LICENSE file in the root directory.
#  SPDX-License-Identifier: BSD-3-Clause
#
from __future__ import annotations

import aiofiles
import asyncio
import functools
import httpx
import inspect
import json
import logging
import os
import pathlib
import ssl
import uuid

from packaging.version import InvalidVersion, Version, parse
from streamer import streamer_client as cli
from typing import Any, Dict, Optional, List

import firecrest.types as t

from firecrest.tracing import current_correlation_id, ensure_correlation_id
from firecrest.v2.retry import RetryPolicy, RetryRule, RuleKey
from firecrest.utilities import (
    part_checksum_xml,
    sched_state_completed,
    sched_state_running,
    time_block,
)
from firecrest.FirecrestException import (
    FirecrestException,
    JobTimeoutException,
    MultipartUploadException,
    NotImplementedOnAPIversion,
    TransferJobFailedException,
    TransferJobTimeoutException,
    UnexpectedStatusException,
)


logger = logging.getLogger(__name__)


# This function is temporarily here
def handle_response(response):
    print("\nResponse status code:")
    print(response.status_code)
    print("\nResponse headers:")
    print(json.dumps(dict(response.headers), indent=4))
    print("\nResponse json:")
    try:
        print(json.dumps(response.json(), indent=4))
    except json.JSONDecodeError:
        print("-")


def _response_message(response: httpx.Response) -> str:
    """The `message` field of a JSON error response, or an empty string."""
    try:
        return str(response.json().get("message", ""))
    except Exception:
        return ""


def sleep_generator():
    yield 0.2
    value = 0.5
    while True:
        yield value
        # Double the value for each iteration, up to 2 minutes
        if value < 60:
            value *= 2


def _with_correlation_id(func):
    """Ensure a correlation ID is set for the duration of the method, so
    that all the requests it makes share the same `X-Correlation-ID`
    header. An ID set by the user with :func:`firecrest.correlation_id`
    takes precedence; otherwise a random UUID4 is generated for each method
    call.
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        with ensure_correlation_id():
            return await func(*args, **kwargs)

    return wrapper


class AsyncExternalTransfer:
    async def wait_for_transfer_job(self, timeout=None):
        with ensure_correlation_id(self._correlation_id):
            await self._client._wait_for_transfer_job(
                self._transfer_info,
                timeout=timeout
            )

    async def wait_for_streamer_job_to_listen(self):
        job_id = self._transfer_info.get("transferJob", {}).get("jobId")
        if job_id is None:
            raise MultipartUploadException(
                self._transfer_info,
                "Could not find transfer job ID in the transfer info"
            )

        system_name = self._transfer_info.get("transferJob", {}).get("system")
        if system_name is None:
            raise MultipartUploadException(
                self._transfer_info,
                "Could not find transfer job system name in the transfer info"
            )

        with ensure_correlation_id(self._correlation_id):
            await self._wait_for_streamer_job_to_listen(system_name, job_id)

    async def _wait_for_streamer_job_to_listen(self, system_name, job_id):
        for i in sleep_generator():
            try:
                job = await self._client.job_info(system_name, job_id)
            except FirecrestException as e:
                if (
                    e.responses[-1].status_code == 404 and
                    "Job not found" in e.responses[-1].json()['message']
                ):
                    self._client.log(
                        logging.DEBUG,
                        f"Job {job_id} information is not yet available, will "
                        f"sleep for {i} seconds."
                    )
                    await asyncio.sleep(i)
                    continue
                else:
                    raise e

            state = job[0]["status"]["state"]
            if isinstance(state, list):
                state = ",".join(state)

            if sched_state_running(state):
                self._client.log(
                    logging.DEBUG,
                    f"Job {job_id} is running with state: {state}."
                )
                break

            if sched_state_completed(state):
                raise MultipartUploadException(
                    self._transfer_info,
                    f"Job {job_id} completed before listening for "
                    f"connections. Current state: {state}."
                )

            self._client.log(
                logging.DEBUG,
                f"Job {job_id} state is {state}. Will sleep for {i} seconds."
            )
            await asyncio.sleep(i)


def _get_wormhole_code(transfer_info: dict) -> str:
    trans_dir = transfer_info.get("transferDirectives", {})
    code = trans_dir.get("wormholeCode")
    if not code:
        raise MultipartUploadException(
            transfer_info,
            "Could not find wormhole code in transfer info"
        )
    return code


class AsyncExternalUpload(AsyncExternalTransfer):
    def __init__(self, client, transfer_info, local_file):
        self._client = client
        self._local_file = local_file
        self._transfer_info = transfer_info
        # Keep the correlation ID of the method that created the transfer,
        # so that follow-up requests can be traced as part of it
        self._correlation_id = current_correlation_id.get()
        self._all_tags = []
        # Chunk size for the multipart upload. Default is 64MB.
        self.chunk_size = 64 * 1024 * 1024  # 64MB
        self._total_file_size = os.path.getsize(local_file)

    @property
    def transfer_data(self):
        return self._transfer_info

    @property
    def wormhole_code(self):
        return self._transfer_info.get(
            "transferDirectives", {}
        ).get("wormholeCode")

    async def send_file_wormhole(self):
        if not isinstance(self._local_file, (str, pathlib.Path)):
            raise ValueError(
                "`local_file` must be a path when using wormhole."
            )
        code = _get_wormhole_code(self._transfer_info)
        self._client.log(
            logging.DEBUG,
            "Starting wormhole send using external CLI."
        )
        process = await asyncio.create_subprocess_exec(
            "wormhole",
            "send",
            str(self._local_file),
            "--code",
            code
        )
        rc = await process.wait()
        if rc != 0:
            raise MultipartUploadException(
                self._transfer_info,
                f"Wormhole send failed with exit code {rc}"
            )

    async def upload_file_to_stage(self):
        urls = self._transfer_info.get("partsUploadUrls")
        if urls is None:
            urls = self._transfer_info.get(
                "transferDirectives", {}
            ).get("parts_upload_urls")

        if urls is None:
            urls = self._transfer_info.get(
                "transferDirectives", {}
            ).get("partsUploadUrls")

        if urls is None:
            raise MultipartUploadException(
                self._transfer_info,
                "Could not find parts upload URLs in the transfer info"
            )

        await asyncio.gather(
            *[
                self._upload_part(
                    upload_url,
                    index,
                ) for index, upload_url in enumerate(urls)
            ]
        )

        # S3 complains when tags are not sorted
        self._all_tags.sort(key=lambda x: x['PartNumber'])
        checksum = part_checksum_xml(self._all_tags)
        await self._complete_upload(
            checksum
        )

    async def _upload_part(self, url, index):
        chunk_size = self._transfer_info.get("maxPartSize")
        if chunk_size is None:
            chunk_size = self._transfer_info.get(
                "transferDirectives", {}
            ).get("max_part_size")

        if chunk_size is None:
            chunk_size = self._transfer_info.get(
                "transferDirectives", {}
            ).get("maxPartSize")

        if chunk_size is None:
            raise MultipartUploadException(
                self._transfer_info,
                "Could not find chunk size in the transfer info"
            )

        async def chunk_reader(f, c):
            i = 0
            while True:
                next_chunk = c if i + c <= chunk_size else chunk_size - i
                i += next_chunk
                data = await f.read(next_chunk)
                if not data:
                    break

                yield data

        start = index * chunk_size
        if start + chunk_size > self._total_file_size:
            content_length = self._total_file_size - start
        else:
            content_length = chunk_size

        async with self._client._upload_semaphore:
            self._client.log(
                logging.DEBUG,
                f"Uploading part {index + 1} to {url}"
            )
            async with aiofiles.open(self._local_file, "rb") as f:
                await f.seek(start)
                resp = await self._client._session.put(
                    url=url,
                    content=chunk_reader(f, self.chunk_size),
                    timeout=None,
                    headers={
                        "Content-Length": str(content_length)
                    }
                )

        if resp.status_code >= 400:
            raise MultipartUploadException(
                self._transfer_info,
                f"Failed to upload part {index + 1}: "
                f"{resp.status_code}: {resp.text}"
            )

        self._client.log(
            logging.DEBUG,
            f"Uploaded part {index + 1} to {url}"
        )
        e_tag = resp.headers['ETag']
        self._all_tags.append({
            'ETag': e_tag,
            'PartNumber': index + 1
        })

    async def _complete_upload(self, checksum):
        url = self._transfer_info.get("completeMultipartUploadUrl")
        if url is None:
            url = self._transfer_info.get(
                "transferDirectives", {}
            ).get("complete_upload_url")

        if url is None:
            url = self._transfer_info.get(
                "transferDirectives", {}
            ).get("completeUploadUrl")

        if url is None:
            raise MultipartUploadException(
                self._transfer_info,
                "Could not find complete upload URL in the transfer info"
            )

        self._client.log(
            logging.DEBUG,
            f"Finishing upload of file {self._local_file} to {url}"
        )
        resp = await self._client._session.post(
            url=url,
            data=checksum
        )
        if resp.status_code >= 400:
            raise MultipartUploadException(
                self._transfer_info,
                f"Failed to finish upload: {resp.status_code}: {resp.text}"
            )

    async def upload_file_streamer(self):
        coordinates = self._transfer_info.get(
            "transferDirectives", {}
        ).get("coordinates")

        if coordinates is None:
            raise MultipartUploadException(
                self._transfer_info,
                "Could not find upload coordinates in the transfer info"
            )

        self._client.log(
            logging.DEBUG,
            f"Uploading file {self._local_file} with `{coordinates}` "
            f"coordinates"
        )

        config = cli.set_coordinates(coordinates)
        config.target = self._local_file
        await cli.client_send(config)

        self._client.log(
            logging.DEBUG,
            f"Uploaded file {self._local_file} to {coordinates} "
            f"using Streamer client"
        )


class AsyncExternalDownload(AsyncExternalTransfer):
    def __init__(self, client, transfer_info, file_path):
        self._client = client
        self._transfer_info = transfer_info
        self._file_path = file_path
        # Keep the correlation ID of the method that created the transfer,
        # so that follow-up requests can be traced as part of it
        self._correlation_id = current_correlation_id.get()
        # Chunk size for the multipart download. Default is 64MB.
        self.chunk_size = 64 * 1024 * 1024  # 64MB

    @property
    def transfer_data(self):
        return self._transfer_info

    @property
    def wormhole_code(self):
        return self._transfer_info.get(
            "transferDirectives", {}
        ).get("wormholeCode")

    async def receive_file_wormhole(self):
        if not isinstance(self._file_path, (str, pathlib.Path)):
            raise ValueError(
                "`target_path` must be a path when using wormhole."
            )
        code = _get_wormhole_code(self._transfer_info)
        self._client.log(
            logging.DEBUG,
            "Starting wormhole receive using external CLI."
        )
        process = await asyncio.create_subprocess_exec(
            "wormhole",
            "receive",
            "--code",
            code,
            "-o",
            str(self._file_path)
        )
        rc = await process.wait()
        if rc != 0:
            raise MultipartUploadException(
                self._transfer_info,
                f"Wormhole receive failed with exit code {rc}"
            )

    async def download_file_from_stage(self, file_path=None):
        file_name = file_path or self._file_path
        download_url = self._transfer_info.get("downloadUrl")
        if download_url is None:
            download_url = self._transfer_info.get(
                "transferDirectives", {}
            ).get("download_url")

        if download_url is None:
            download_url = self._transfer_info.get(
                "transferDirectives", {}
            ).get("downloadUrl")

        if download_url is None:
            raise MultipartUploadException(
                self._transfer_info,
                "Could not find download URL in the transfer info"
            )

        self._client.log(
            logging.DEBUG,
            f"Downloading file from {download_url} "
            f"to {file_name}"
        )

        async with self._client._session.stream(
            "GET",
            download_url
        ) as resp:
            resp.raise_for_status()

            async with aiofiles.open(file_name, "wb") as f:
                async for chunk in resp.aiter_bytes(
                    chunk_size=self.chunk_size
                ):
                    await f.write(chunk)

        self._client.log(
            logging.DEBUG,
            f"Downloaded file from {download_url} to {file_name}"
        )

    async def download_file_streamer(self, file_path=None):
        file_name = file_path or self._file_path
        coordinates = self._transfer_info.get(
            "transferDirectives", {}
        ).get("coordinates")

        if coordinates is None:
            raise MultipartUploadException(
                self._transfer_info,
                "Could not find download coordinates in the transfer info"
            )

        self._client.log(
            logging.DEBUG,
            f"Downloading file {file_name} with `{coordinates}` "
            f"coordinates"
        )

        config = cli.set_coordinates(coordinates)
        config.target = file_name
        await cli.client_receive(config)

        self._client.log(
            logging.DEBUG,
            f"Downloaded file {file_name} from {coordinates} "
            f"using Streamer client"
        )


class AsyncFirecrest:
    """
    This is the basic class you instantiate to access the FirecREST API v2.
    Necessary parameters are the firecrest URL and an authorization object.

    :param firecrest_url: FirecREST's URL
    :param authorization: the authorization object. This object is responsible
                          of handling the credentials. It must provide either
                          a method auth_headers() that returns the HTTP
                          headers that authenticate a request (e.g.
                          `ApiKeyAuth`), or a method get_access_token() that
                          returns a valid bearer token (e.g.
                          `ClientCredentialsAuth`).
    :param verify: either a boolean, in which case it controls whether
                   requests will verify the server’s TLS certificate,
                   or a string, in which case it must be a path to a CA bundle
                   to use
    :param retry_policy: the :class:`firecrest.v2.RetryPolicy` that decides
                         which failed requests are repeated and how. By
                         default a policy with the
                         :data:`firecrest.v2.retry.DEFAULT_RETRY_RULES` is used.
                         It is also available (and editable) as the
                         ``retry_policy`` attribute of the client.
    """

    TOO_MANY_REQUESTS_CODE = 429
    MAX_DIRECT_UPLOAD_SIZE = 1048576
    MAX_S3_CONNECTIONS = 10

    def _retry_requests(idempotent_default):
        """Decorator of the request wrappers. The request is repeated
        according to :attr:`retry_policy`: after every attempt the policy is
        asked whether the response (or the transport error) should be
        retried and how long to sleep before trying again. Each rule of the
        policy has its own budget of retries.

        The wrapper accepts an ``idempotent`` keyword argument (defaults to
        ``idempotent_default``) that is consumed here, not passed to the
        wrapped function, and tells the policy whether the request is safe
        to repeat. The number of retries that were performed is stored in
        ``response.extensions["firecrest_retries"]``.
        """
        def decorator(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                client = args[0]
                idempotent = kwargs.pop("idempotent", idempotent_default)
                endpoint = kwargs.get("endpoint", "")
                policy = client.retry_policy
                retries: Dict[RuleKey, int] = {}
                while True:
                    try:
                        resp = await func(*args, **kwargs)
                    except Exception as exc:
                        match = policy.match(idempotent, exc=exc)
                        if match is None:
                            raise

                        key, rule = match
                        done = retries.get(key, 0)
                        if rule.exhausted(done):
                            client.log(
                                logging.DEBUG,
                                f"Request to {endpoint} failed with "
                                f"{type(exc).__name__} and has been retried "
                                f"already {done} times"
                            )
                            raise

                        delay = rule.sleep_seconds(done, log=client.log)
                        client.log(
                            logging.INFO,
                            f"Request to {endpoint} failed with "
                            f"{type(exc).__name__}: {exc}. Will sleep for "
                            f"{delay:.1f} seconds and try again (retry "
                            f"{done + 1}/{rule.max_retries or 'unlimited'})"
                        )
                        await asyncio.sleep(delay)
                        retries[key] = done + 1
                        continue

                    match = policy.match(idempotent, response=resp)
                    if match is None:
                        break

                    key, rule = match
                    done = retries.get(key, 0)
                    if rule.exhausted(done):
                        client.log(
                            logging.DEBUG,
                            f"Request to {endpoint} got status "
                            f"{resp.status_code} and has been retried "
                            f"already {done} times"
                        )
                        break

                    delay = rule.sleep_seconds(
                        done, response=resp, log=client.log
                    )
                    client.log(
                        logging.INFO,
                        f"Request to {endpoint} got status "
                        f"{resp.status_code}. Will sleep for {delay:.1f} "
                        f"seconds and try again (retry {done + 1}/"
                        f"{rule.max_retries or 'unlimited'})"
                    )
                    await asyncio.sleep(delay)
                    retries[key] = done + 1

                resp.extensions["firecrest_retries"] = sum(retries.values())
                return resp

            return wrapper

        return decorator

    def __init__(
        self,
        firecrest_url: str,
        authorization: Any,
        verify: str | bool | ssl.SSLContext = True,
        retry_policy: Optional[RetryPolicy] = None,
    ) -> None:
        self._firecrest_url = firecrest_url.rstrip('/')
        self._authorization = authorization
        self._verify = verify
        #: This attribute will be passed to all the requests that will be made.
        #: How many seconds to wait for the server to send data before giving
        #: up. After that time a `httpx.TimeoutException` error will be
        #: raised.
        #:
        #: It can be a float or a tuple. More details here:
        #: https://www.python-httpx.org/advanced/#fine-tuning-the-configuration.
        self.timeout: Any = None
        # type is Any because of some incompatibility between httpx and
        # requests library

        #: Disable all logging from the client.
        self.disable_client_logging: bool = False
        #: The :class:`firecrest.v2.RetryPolicy` of the client: a table of
        #: rules, keyed by HTTP status code or `httpx` exception class, that
        #: decides which failed requests are repeated, how many times and
        #: with which backoff. Edit it in place to tune the retries, e.g.
        #: ``client.retry_policy.rules[503].max_retries = 10``.
        self.retry_policy: RetryPolicy = (
            retry_policy if retry_policy is not None else RetryPolicy()
        )
        self._api_version: Version = parse("2.6.0")
        # Set to `True` when the user sets the version explicitly, in which
        # case the version detected from the response headers is ignored
        self._api_version_explicit: bool = False
        self._detected_version_header: Optional[str] = None
        self._session = httpx.AsyncClient(verify=self._verify)
        self._upload_semaphore = asyncio.Semaphore(self.MAX_S3_CONNECTIONS)

    def set_api_version(self, api_version: str) -> None:
        """Set the version of the api of firecrest. By default it will be
        assumed that you are using version 2.5.4 or compatible, until the
        version is detected from the `f7t-appversion` header of the
        responses. Calling this method disables the automatic detection of
        the version from the response headers. The version is parsed by the
        `packaging` library.
        """
        self._api_version = parse(api_version)
        self._api_version_explicit = True

    def _update_api_version_from_response(
        self, response: httpx.Response
    ) -> None:
        if self._api_version_explicit:
            return

        header = response.headers.get("f7t-appversion")
        if not header or header == self._detected_version_header:
            return

        self._detected_version_header = header
        try:
            self._api_version = parse(header)
            self.log(
                logging.DEBUG,
                f"Set API version to {header} from the response header"
            )
        except InvalidVersion:
            # Development deployments report versions like "2.x.x"; keep
            # the current version in that case
            self.log(
                logging.DEBUG,
                f"Could not parse API version from the response header: "
                f"{header}"
            )

    async def close_session(self) -> None:
        """Close the httpx session"""
        await self._session.aclose()

    async def create_new_session(self) -> None:
        """Create a new httpx session"""
        if not self._session.is_closed:
            await self._session.aclose()

        self._session = httpx.AsyncClient(verify=self._verify)

    def set_maximum_s3_connections(self, max_connections: int) -> None:
        """Set the maximum number of simultaneous connections to S3. By
        default it is set to 10.
        """
        # TODO: Check if the semaphore is used?
        self._upload_semaphore = asyncio.Semaphore(max_connections)

    @property
    def is_session_closed(self) -> bool:
        """Check if the httpx session is closed"""
        return self._session.is_closed

    @property
    def num_retries_rate_limit(self) -> Optional[int]:
        """Number of retries in case the rate limit is reached. When it is
        set to `None`, the client will keep trying until it gets a different
        status code than 429. This is an alias of the ``max_retries`` of the
        rule for status 429 in :attr:`retry_policy`.
        """
        rules = self.retry_policy.rules_for(429)
        return rules[0].max_retries if rules else 0

    @num_retries_rate_limit.setter
    def num_retries_rate_limit(self, value: Optional[int]) -> None:
        rules = self.retry_policy.rules_for(429)
        if rules:
            rules[0].max_retries = value
        else:
            self.retry_policy.rules[429] = RetryRule(max_retries=value)

    def log(self, level: int, msg: Any) -> None:
        """Log a message with the given level on the client logger.
        """
        if not self.disable_client_logging:
            logger.log(level, msg)

    async def _auth_headers(self) -> dict:
        """Return the headers that authenticate a request, as provided by the
        authorization object. Objects exposing `auth_headers()` are used
        directly (the result may be awaitable); otherwise the legacy
        `get_access_token()` contract is used to build a Bearer header.
        """
        auth_headers = getattr(self._authorization, "auth_headers", None)
        if auth_headers is None:
            token = self._authorization.get_access_token()
            return {
                "Authorization": f"Bearer {token}"
            }

        headers = auth_headers()
        if inspect.isawaitable(headers):
            headers = await headers

        return dict(headers)

    def _tracing_headers(self) -> dict:
        """Return the tracing headers of a request: a fresh `X-Request-ID`
        for this request and, when one is set in the current context, the
        `X-Correlation-ID`.
        """
        headers = {"X-Request-ID": str(uuid.uuid4())}
        correlation = current_correlation_id.get()
        if correlation is not None:
            headers["X-Correlation-ID"] = correlation

        return headers

    @_retry_requests(idempotent_default=True)  # type: ignore
    async def _get_request(
        self,
        endpoint,
        additional_headers=None,
        params=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = self._tracing_headers()
        headers.update(await self._auth_headers())
        if additional_headers:
            headers.update(additional_headers)

        # httpx serializes `None` values as empty strings, which the API may
        # reject (e.g. enum-typed query parameters), so drop them entirely
        if params:
            params = {k: v for k, v in params.items() if v is not None}

        self.log(
            logging.DEBUG,
            f"Making GET request to {endpoint} "
            f"(X-Request-ID: {headers['X-Request-ID']}, "
            f"X-Correlation-ID: {headers.get('X-Correlation-ID')})"
        )
        with time_block(f"GET request to {endpoint}", logger):
            resp = await self._session.get(
                url=url, headers=headers, params=params, timeout=self.timeout
            )

        self._update_api_version_from_response(resp)
        return resp

    @_retry_requests(idempotent_default=False)  # type: ignore
    async def _post_request(
        self, endpoint, additional_headers=None, params=None, data=None, files=None, json_data=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = self._tracing_headers()
        headers.update(await self._auth_headers())
        if additional_headers:
            headers.update(additional_headers)

        self.log(
            logging.DEBUG,
            f"Making POST request to {endpoint} "
            f"(X-Request-ID: {headers['X-Request-ID']}, "
            f"X-Correlation-ID: {headers.get('X-Correlation-ID')})"
        )
        with time_block(f"POST request to {endpoint}", logger):
            resp = await self._session.post(
                url=url,
                headers=headers,
                params=params,
                data=data,
                files=files,
                timeout=self.timeout,
                json=json_data
            )

        self._update_api_version_from_response(resp)
        return resp

    @_retry_requests(idempotent_default=False)  # type: ignore
    async def _put_request(
        self, endpoint, additional_headers=None, data=None, json_data=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = self._tracing_headers()
        headers.update(await self._auth_headers())
        if additional_headers:
            headers.update(additional_headers)

        self.log(
            logging.DEBUG,
            f"Making PUT request to {endpoint} "
            f"(X-Request-ID: {headers['X-Request-ID']}, "
            f"X-Correlation-ID: {headers.get('X-Correlation-ID')})"
        )
        with time_block(f"PUT request to {endpoint}", logger):
            resp = await self._session.put(
                url=url, headers=headers, data=data, timeout=self.timeout, json=json_data
            )

        self._update_api_version_from_response(resp)
        return resp

    @_retry_requests(idempotent_default=False)  # type: ignore
    async def _delete_request(
        self, endpoint, additional_headers=None, params=None, data=None
    ) -> httpx.Response:
        url = f"{self._firecrest_url}{endpoint}"
        headers = self._tracing_headers()
        headers.update(await self._auth_headers())
        if additional_headers:
            headers.update(additional_headers)

        self.log(
            logging.INFO,
            f"Making DELETE request to {endpoint} "
            f"(X-Request-ID: {headers['X-Request-ID']}, "
            f"X-Correlation-ID: {headers.get('X-Correlation-ID')})"
        )
        with time_block(f"DELETE request to {endpoint}", logger):
            # httpx doesn't support data in the `delete` method so we will
            # have to use the generic `request` method
            # https://www.python-httpx.org/compatibility/#request-body-on-http-methods
            resp = await self._session.request(
                method="DELETE",
                url=url,
                headers=headers,
                params=params,
                data=data,
                timeout=self.timeout,
            )

        self._update_api_version_from_response(resp)
        return resp

    def _check_response(
        self,
        response: httpx.Response,
        expected_status_code: int,
        return_json: bool = True
    ) -> dict:
        status_code = response.status_code
        # handle_response(response)
        if status_code != expected_status_code:
            self.log(
                logging.DEBUG,
                f"Unexpected status of last request {status_code}, it "
                f"should have been {expected_status_code}"
            )
            raise UnexpectedStatusException(
                [response], expected_status_code
            )

        return response.json() if return_json and status_code != 204 else {}

    @_with_correlation_id  # type: ignore
    async def server_version(self) -> str | None:
        """Returns the exact API version of the FirecREST server.

        :calls: GET `/status/systems`
        """
        resp = await self._get_request(endpoint="/status/systems")
        if resp.headers.get("f7t-appversion") == "2.x.x":
            return "2"
        elif (
            resp.headers.get("f7t-appversion")
        ):
            return resp.headers["f7t-appversion"]
        else:
            return None

    @_with_correlation_id  # type: ignore
    async def systems(self) -> List[dict]:
        """Returns available systems.

        :calls: GET `/status/systems`
        """
        resp = await self._get_request(endpoint="/status/systems")
        return self._check_response(resp, 200)['systems']

    @_with_correlation_id  # type: ignore
    async def nodes(
        self,
        system_name: str
    ) -> List[dict]:
        """Returns nodes of the system.

        :param system_name: the system name where the nodes belong to
        :calls: GET `/status/{system_name}/nodes`
        """
        resp = await self._get_request(
            endpoint=f"/status/{system_name}/nodes"
        )
        return self._check_response(resp, 200)['nodes']

    @_with_correlation_id  # type: ignore
    async def reservations(
        self,
        system_name: str
    ) -> List[dict]:
        """Returns reservations defined in the system.

        :param system_name: the system name where the reservations belong to
        :calls: GET `/status/{system_name}/reservations`
        """
        resp = await self._get_request(
            endpoint=f"/status/{system_name}/reservations"
        )
        return self._check_response(resp, 200)['reservations']

    @_with_correlation_id  # type: ignore
    async def partitions(
        self,
        system_name: str
    ) -> List[dict]:
        """Returns partitions defined in the scheduler of the system.

        :param system_name: the system name where the partitions belong to
        :calls: GET `/status/{system_name}/partitions`
        """
        resp = await self._get_request(
            endpoint=f"/status/{system_name}/partitions"
        )
        return self._check_response(resp, 200)["partitions"]

    @_with_correlation_id  # type: ignore
    async def userinfo(
        self,
        system_name: str
    ) -> t.UserInfo:
        """Returns user, groups and accounts information.

        The ``accounts`` field and the ``default`` flag of the groups
        require version 2.6.0 of the API. Older versions return the
        primary group in a top-level ``group`` field instead.

        :param system_name: the system name
        :calls: GET `/status/{system_name}/userinfo`
        """
        resp = await self._get_request(
             endpoint=f"/status/{system_name}/userinfo"
        )
        return self._check_response(resp, 200)  # type: ignore

    @_with_correlation_id  # type: ignore
    async def list_files(
        self,
        system_name: str,
        path: str,
        show_hidden: bool = False,
        recursive: bool = False,
        numeric_uid: bool = False,
        dereference: bool = False
    ) -> List[dict]:
        """Returns a list of files in a directory.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :param show_hidden: Show hidden files
        :param recursive: recursively list directories encountered
        :numeric_uid: list numeric user and group IDs
        :param dereference: when showing file information for a symbolic link,
                            show information for the file the link references
                            rather than for the link itself
        :calls: GET `/filesystem/{system_name}/ops/ls`
        """
        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/ls",
            params={
                "path": path,
                "showHidden": show_hidden,
                "recursive": recursive,
                "numericUid": numeric_uid,
                "dereference": dereference
            }
        )
        return self._check_response(resp, 200)["output"]

    @_with_correlation_id  # type: ignore
    async def head(
        self,
        system_name: str,
        path: str,
        num_bytes: Optional[int] = None,
        num_lines: Optional[int] = None,
        exclude_trailing: bool = False,
    ) -> dict:
        """Display the beginning of a specified file.
        By default 10 lines will be returned.
        `num_bytes` and `num_lines` cannot be specified simultaneously.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :param num_bytes: the output will be the first NUM bytes of each file
        :param num_lines: the output will be the first NUM lines of each file
        :param exclude_trailing: the output will be the whole file, without
                                 the last NUM bytes/lines of each file. NUM
                                 should be specified in the respective
                                 argument through ``bytes`` or ``lines``.
        :calls: GET `/filesystem/{system_name}/ops/head`
        """
        # Validate that num_bytes and num_lines are not passed together
        if num_bytes is not None and num_lines is not None:
            raise ValueError(
                "You cannot specify both `num_bytes` and `num_lines`."
            )

        # If `exclude_trailing` is passed, either `num_bytes` or `num_lines`
        # must be passed
        if exclude_trailing and num_bytes is None and num_lines is None:
            raise ValueError(
                "`exclude_trailing` requires either `num_bytes` or "
                "`num_lines` to be specified.")

        params = {
            "path": path,
            "skipTrailing": exclude_trailing
        }
        if num_bytes is not None:
            params["bytes"] = num_bytes

        if num_lines is not None:
            params["lines"] = num_lines

        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/head",
            params=params
        )
        return self._check_response(resp, 200)['output']

    @_with_correlation_id  # type: ignore
    async def tail(
        self,
        system_name: str,
        path: str,
        num_bytes: Optional[int] = None,
        num_lines: Optional[int] = None,
        exclude_beginning: bool = False,
    ) -> dict:
        """Display the ending of a specified file.
        By default, 10 lines will be returned.
        `num_bytes` and `num_lines` cannot be specified simultaneously.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :param num_bytes: The output will be the last NUM bytes of each file
        :param num_lines: The output will be the last NUM lines of each file
        :param exclude_beginning: The output will be the whole file, without
                                  the first NUM bytes/lines of each file. NUM
                                  should be specified in the respective
                                  argument through ``num_bytes`` or
                                  ``num_lines``.
        :calls: GET `/filesystem/{system_name}/ops/tail`
        """
        # Ensure `num_bytes` and `num_lines` are not passed together
        if num_bytes is not None and num_lines is not None:
            raise ValueError(
                "You cannot specify both `num_bytes` and `num_lines`."
            )

        # If `exclude_beginning` is passed, either `num_bytes` or `num_lines`
        # must be passed
        if exclude_beginning and num_bytes is None and num_lines is None:
            raise ValueError(
                "`exclude_beginning` requires either `num_bytes` or "
                "`num_lines` to be specified."
            )

        params = {
            "path": path,
            "skipHeading": exclude_beginning
        }
        if num_bytes is not None:
            params["bytes"] = num_bytes

        if num_lines is not None:
            params["lines"] = num_lines

        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/tail",
            params=params
        )
        return self._check_response(resp, 200)['output']

    @_with_correlation_id  # type: ignore
    async def view(
        self,
        system_name: str,
        path: str,
    ) -> str:
        """
        View full file content (up to 5MB files)

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :calls: GET `/filesystem/{system_name}/ops/view`
        """
        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/view",
            params={"path": path}
        )
        return self._check_response(resp, 200)["output"]

    @_with_correlation_id  # type: ignore
    async def checksum(
        self,
        system_name: str,
        path: str,
    ) -> dict:
        """
        Calculate the SHA256 (256-bit) checksum of a specified file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :calls: GET `/filesystem/{system_name}/ops/checksum`
        """
        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/checksum",
            params={"path": path}
        )
        return self._check_response(resp, 200)["output"]

    @_with_correlation_id  # type: ignore
    async def file_type(
        self,
        system_name: str,
        path: str,
    ) -> str:
        """
        Uses the `file` linux application to determine the type of a file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :calls: GET `/filesystem/{system_name}/ops/file`
        """
        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/file",
            params={"path": path}
        )
        return self._check_response(resp, 200)["output"]

    @_with_correlation_id  # type: ignore
    async def chmod(
        self,
        system_name: str,
        path: str,
        mode: str
    ) -> dict:
        """Changes the file mod bits of a given file according to the
        specified mode.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :param mode: same as numeric mode of linux chmod tool
        :calls: PUT `/filesystem/{system_name}/ops/chmod`
        """
        data: dict[str, str] = {
            "path": path,
            "mode": mode
        }
        resp = await self._put_request(
            endpoint=f"/filesystem/{system_name}/ops/chmod",
            idempotent=True,
            json_data=data
        )
        return self._check_response(resp, 200)["output"]

    @_with_correlation_id  # type: ignore
    async def chown(
        self,
        system_name: str,
        path: str,
        owner: str,
        group: str
    ) -> dict:
        """Changes the user and/or group ownership of a given file.
        If only owner or group information is passed, only that information
        will be updated.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path of the file
        :param owner: owner ID for target
        :param group: group ID for target
        :calls: PUT `/filesystem/{system_name}/ops/chown`
        """
        data: dict[str, str] = {
            "path": path,
            "owner": owner,
            "group": group
        }
        resp = await self._put_request(
            endpoint=f"/filesystem/{system_name}/ops/chown",
            idempotent=True,
            json_data=data
        )
        return self._check_response(resp, 200)["output"]

    @_with_correlation_id  # type: ignore
    async def stat(
        self,
        system_name: str,
        path: str,
        dereference: bool = False,
    ) -> dict:
        """
        Uses the stat linux application to determine the status of a file on
        the system's filesystem. The result follows:
        https://docs.python.org/3/library/os.html#os.stat_result.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :param dereference: follow symbolic links
        :calls: GET `/filesystem/{system_name}/ops/stat`
        """
        resp = await self._get_request(
            endpoint=f"/filesystem/{system_name}/ops/stat",
            params={
                "path": path,
                "dereference": dereference
            }
        )
        return self._check_response(resp, 200)["output"]

    @_with_correlation_id  # type: ignore
    async def symlink(
        self,
        system_name: str,
        source_path: str,
        link_path: str,
    ) -> dict:
        """Create a symbolic link.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute path to the file the link points to
        :param link_path: the absolute path to the symlink

        :calls: POST `/filesystem/{system_name}/ops/symlink`
        """
        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/ops/symlink",
            json_data={
                "sourcePath": source_path,
                "linkPath": link_path
            }
        )
        return self._check_response(resp, 201)

    @_with_correlation_id  # type: ignore
    async def mkdir(
        self,
        system_name: str,
        path: str,
        create_parents: bool = False
    ) -> dict:
        """Create a directory.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute path to the new directory
        :param create_parents: create intermediate parent directories

        :calls: POST `/filesystem/{system_name}/ops/mkdir`
        """
        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/ops/mkdir",
            json_data={
                "sourcePath": path,
                "parent": create_parents
            }
        )
        return self._check_response(resp, 201)["output"]

    @_with_correlation_id  # type: ignore
    async def mv(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        account: Optional[str] = None,
        blocking: bool = True,
        timeout: Optional[float] = None
    ) -> dict:
        """Rename/move a file, directory, or symlink at the `source_path` to
        the `target_path` on `system_name`'s filesystem.
        This operation runs in a job.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :param account: the account to be used for the transfer job
        :param blocking: whether to wait for the job to complete
        :param timeout: the maximum time to wait for the job to complete

        :calls: POST `/filesystem/{system_name}/transfer/mv`
        """
        data: dict[str, str] = {
            "sourcePath": source_path,
            "targetPath": target_path,
        }
        if account is not None:
            data["account"] = account

        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/mv",
            json_data=data
        )
        job_info = self._check_response(resp, 201)

        if blocking:
            await self._wait_for_transfer_job(
                job_info,
                timeout=timeout
            )

        return job_info

    @_with_correlation_id  # type: ignore
    async def compress(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        dereference: bool = False,
        compression: str = "gzip",
        match_pattern: Optional[str] = None,
        account: Optional[str] = None,
        blocking: bool = True
    ) -> None:
        """Compress a directory or file.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute path to source directory
        :param target_path: the absolute path to the newly created
                            compressed file
        :param dereference: dereference links when compressing
        :param compression: compression algorithm to use (one of "none",
                            "gzip", "bzip2", "xz"). Default is "gzip".
        :param match_pattern: regex pattern to filter files to compress
        :param account: the account to be used for the transfer job (only
                        relevant when the compression operation is not done
                        directly)
        :param blocking: whether to wait for the job to complete (only
                         relevant when the compression operation is not done
                         directly)
        :calls: POST `/filesystem/{system_name}/ops/compress`

                POST `/filesystem/{system_name}/transfer/compress`

                GET `/jobs/{system_name}/{job_id}`
        """
        data = {
            "source_path": source_path,
            "target_path": target_path,
            "dereference": dereference,
            "compression": compression,
            "match_pattern": match_pattern
        }
        try:
            resp = await self._post_request(
                endpoint=f"/filesystem/{system_name}/ops/compress",
                json_data=data
            )
            self._check_response(resp, 204)
            return None
        except FirecrestException as e:
            if e.responses[-1].status_code == 408:
                # Operation took too long, will run it as a transfer job
                self.log(
                    logging.INFO,
                    "Compression operation is taking too long, "
                    "will run it as a transfer job"
                )
            else:
                raise e

        if account is not None:
            data["account"] = account

        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/compress",
            json_data=data
        )
        job_info = self._check_response(resp, 201)

        if blocking:
            await self._wait_for_transfer_job(job_info)

        return None

    @_with_correlation_id  # type: ignore
    async def extract(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        compression: str = "gzip",
        account: Optional[str] = None,
        blocking: bool = True
    ) -> None:
        """Extract compressed archives.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute path to the archive
        :param target_path: the absolute path to target directory
        :param compression: compression algorithm to use (one of "none",
                            "gzip", "bzip2", "xz"). Default is "gzip".
        :param account: the account to be used for the transfer job (only
                        relevant when the extract operation is not done
                        directly)
        :param blocking: whether to wait for the job to complete (only
                         relevant when the extract operation is not done
                         directly)
        :calls: POST `/filesystem/{system_name}/ops/extract`

                POST `/filesystem/{system_name}/transfer/extract`

                GET `/jobs/{system_name}/{job_id}`
        """
        data = {
            "source_path": source_path,
            "target_path": target_path,
            "compression": compression
        }
        try:
            resp = await self._post_request(
                endpoint=f"/filesystem/{system_name}/ops/extract",
                json_data=data
            )
            self._check_response(resp, 204)
            return None
        except FirecrestException as e:
            if e.responses[-1].status_code == 408:
                # Operation took too long, will run it as a transfer job
                self.log(
                    logging.INFO,
                    "Extract operation is taking too long, "
                    "will run it as a transfer job"
                )
            else:
                raise e

        if account is not None:
            data["account"] = account

        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/extract",
            json_data=data
        )
        job_info = self._check_response(resp, 201)

        if blocking:
            await self._wait_for_transfer_job(job_info)

        return None

    async def _wait_for_transfer_job(
        self,
        job_info: dict,
        timeout: Optional[float] = None
    ) -> None:
        job_id = job_info["transferJob"]["jobId"]
        system_name = job_info["transferJob"]["system"]
        try:
            await self.wait_for_job(
                system_name,
                job_id,
                timeout=timeout
            )
        except JobTimeoutException:
            self.log(
                logging.DEBUG,
                f"Transfer job {job_id} timed out"
            )
            raise TransferJobTimeoutException(job_info)

        try:
            # If job is cancelled before it starts running, the log file will
            # not be available
            stdout_file = await self.view(
                system_name,
                job_info["transferJob"]["logs"]["outputLog"]
            )
        except FirecrestException as e:
            if (
                e.responses[-1].status_code == 404 and
                "No such file or directory" in e.responses[-1].json()['message']
            ):
                raise TransferJobFailedException(
                    job_info, file_not_found=True
                )
            else:
                raise e

        if (
            "Files were successfully" not in stdout_file and
            "File was successfully" not in stdout_file and
            "Multipart file upload successfully completed" not in stdout_file
        ):
            raise TransferJobFailedException(job_info)

    @_with_correlation_id  # type: ignore
    async def wait_for_job(
        self,
        system_name: str,
        job_id: str,
        timeout: Optional[float] = None,
        not_found_timeout: float = 20.0
    ) -> List[Any]:
        """Wait for a job to complete. When the job is completed, it will
        return the job information.
        :param system_name: the system name where the filesystem belongs to
        :param job_id: the ID of the job to wait for
        :param timeout: the maximum time to wait for the job to complete
                        in seconds. If the timeout is set and the job is not
                        completed within this time, it will be cancelled.
        :param not_found_timeout: the maximum time to wait for the job to be
                                  available in the scheduler, in seconds.
                                  Slurm or other scheduler will need a few
                                  seconds before updating the database. If
                                  the job is not found within this time, the
                                  function will raise an exception.
        :calls: GET `/jobs/{system_name}/{job_id}`
        """
        if timeout:
            timeout_time = asyncio.get_event_loop().time() + timeout

        not_found_timeout_time = (
            asyncio.get_event_loop().time() + not_found_timeout
        )

        self.log(
            logging.DEBUG,
            f"Waiting for job {job_id}."
        )

        async def check_timeout(sleep_time):
            if not timeout:
                return

            if asyncio.get_event_loop().time() + sleep_time > timeout_time:
                self.log(
                    logging.DEBUG,
                    f"Timeout is about to be reached, while waiting for "
                    f"job {job_id}. Will cancel the job to stop the transfer."
                )

                try:
                    await self.cancel_job(system_name, job_id)
                except FirecrestException as e:
                    self.log(
                        logging.DEBUG,
                        f"Failed to cancel job {job_id}: {e}"
                    )

                raise JobTimeoutException(job_id)

        for i in sleep_generator():
            try:
                job = await self.job_info(system_name, job_id)
            except FirecrestException as e:
                if (
                    e.responses[-1].status_code == 404 and
                    "Job not found" in e.responses[-1].json()['message']
                ):
                    if asyncio.get_event_loop().time() > not_found_timeout_time:
                        raise e

                    self.log(
                        logging.DEBUG,
                        f"Job {job_id} information is not yet available, will "
                        f"sleep for {i} seconds."
                    )
                    await check_timeout(i)
                    await asyncio.sleep(i)
                    continue

                raise e

            state = job[0]["status"]["state"]
            if isinstance(state, list):
                state = ",".join(state)

            if sched_state_completed(state):
                self.log(
                    logging.DEBUG,
                    f"Job {job_id} is completed with state: {state}."
                )
                break

            self.log(
                logging.DEBUG,
                f"Job {job_id} state is {state}. Will sleep for {i} seconds."
            )
            await check_timeout(i)
            await asyncio.sleep(i)

        return job

    @_with_correlation_id  # type: ignore
    async def cp(
        self,
        system_name: str,
        source_path: str,
        target_path: str,
        dereference: bool = False,
        account: Optional[str] = None,
        blocking: bool = True,
        timeout: Optional[float] = None
    ) -> dict:
        """Copies file from `source_path` to `target_path`.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path
        :param target_path: the absolute target path
        :param dereference: dereference links when copying
        :param blocking: whether to wait for the job to complete
        :param account: the account to be used for the transfer job
        :param timeout: the maximum time to wait for the job to complete
        :calls: POST `/filesystem/{system_name}/transfer/cp`
        """
        if self._api_version < parse("2.2.8") and dereference:
            raise NotImplementedOnAPIversion(
                "cp dereferencing is not available for "
                "version <2.2.8 of the API."
            )
        data: dict[str, Any] = {
            "sourcePath": source_path,
            "targetPath": target_path,
            "dereference": dereference,
        }
        if account is not None:
            data["account"] = account

        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/cp",
            json_data=data
        )
        job_info = self._check_response(resp, 201)

        if blocking:
            await self._wait_for_transfer_job(
                job_info,
                timeout=timeout
            )

        return job_info

    @_with_correlation_id  # type: ignore
    async def rm(
        self,
        system_name: str,
        path: str,
        account: Optional[str] = None,
        blocking: bool = True,
        timeout: Optional[float] = None
    ) -> Optional[dict]:
        """Delete a file.
        First the client will try to delete the file directly, and if it
        fails with a timeout, it will launch a job to delete the file.

        :param system_name: the system name where the filesystem belongs to
        :param path: the absolute target path
        :param account: the account to be used for the transfer job  (only
                        relevant when the file is not deleted directly)
        :param blocking: whether to wait for the job to complete (only
                         relevant when the file is not deleted directly)
        :param timeout: the maximum time to wait for the job to complete
        :calls: DELETE `/filesystem/{system_name}/ops/rm`

                DELETE `/filesystem/{system_name}/transfer/rm`

                GET `/jobs/{system_name}/{job_id}`
        """
        params = {
            "path": path,
        }
        try:
            resp = await self._delete_request(
                endpoint=f"/filesystem/{system_name}/ops/rm",
                params=params
            )
            self._check_response(resp, 204)
            return None
        except FirecrestException as e:
            if e.responses[-1].status_code == 408:
                self.log(
                    logging.DEBUG,
                    f"The command for the deletion of file {path} "
                    f"got a timeout, a job will be launched to "
                    f"delete the file."
                )
            elif e.responses[-1].status_code == 500:
                try:
                    json_resp = e.responses[-1].json()
                    if (
                        "message" in json_resp and
                        "exit status:124" in json_resp["message"]
                    ):
                        self.log(
                            logging.DEBUG,
                            f"The command for the deletion of file {path} "
                            f"got a timeout, a job will be launched to "
                            f"delete the file."
                        )
                    else:
                        raise e

                except json.JSONDecodeError:
                    raise e
            else:
                raise e

        if account is not None:
            params["account"] = account

        resp = await self._delete_request(
            endpoint=f"/filesystem/{system_name}/transfer/rm",
            params=params
        )

        job_info = self._check_response(resp, 200)
        if blocking:
            await self._wait_for_transfer_job(
                job_info,
                timeout=timeout
            )

        return job_info

    @_with_correlation_id  # type: ignore
    async def upload(
        self,
        system_name: str,
        local_file: str | pathlib.Path,
        directory: str,
        filename: str,
        account: Optional[str] = None,
        blocking: bool = True,
        transfer_method: str = "s3"
    ) -> Optional[AsyncExternalUpload]:
        """Upload a file to the system. Small files will be
        uploaded directly to FirecREST and will be immediately available.
        The function will return `None` in this case.
        Large files will be uploaded in parts to the
        staging area of FirecREST and then moved to the target directory in a
        job. The function will return the transfer job information in this
        case.

        :param system_name: the system name where the filesystem belongs to
        :param local_file: the local file's path to be uploaded (can be
                           relative)
        :param directory: the absolut target path of the directory where the
                          file will be uploaded
        :param filename: the name of the file in the target directory
        :param account: the account to be used for the transfer job (only
                        relevant when the file is larger than
                        `MAX_DIRECT_UPLOAD_SIZE`)
        :param blocking: whether to wait for the job to complete (only
                         relevant when the file is larger than
                         `MAX_DIRECT_UPLOAD_SIZE`)
        :param transfer_method: the method to be used for the upload of large
                                files. Supported methods: "s3", "streamer",
                                "wormhole".
        :calls: POST `/filesystem/{system_name}/transfer/upload`
        """
        data: dict[str, Any]

        if transfer_method not in ["s3", "streamer", "wormhole"]:
            raise ValueError(
                f"Unsupported transfer_method '{transfer_method}'. Only 's3', "
                f"'streamer' and 'wormhole' are currently supported."
            )

        if not isinstance(local_file, (str, pathlib.Path)):
            raise TypeError(
                f"`local_file` must be a string or pathlib.Path, got "
                f"{type(local_file)}. For more options, consider using the "
                "serial Client."
            )

        if not os.path.isfile(local_file):
            raise FileNotFoundError(f"File not found: {local_file}")

        local_file_size = os.path.getsize(local_file)
        if local_file_size < self.MAX_DIRECT_UPLOAD_SIZE:
            self.log(
                logging.DEBUG,
                f"File ({local_file}) will be directly uploaded to the "
                f"target directory, since it's {local_file_size} bytes."
            )
            async with aiofiles.open(local_file, "rb") as f:
                file_content = await f.read()
                resp = await self._post_request(
                    endpoint=f"/filesystem/{system_name}/ops/upload",
                    params={
                        "path": directory,
                    },
                    files={"file": (filename, file_content)}
                )
                self._check_response(resp, 204)
                return None

        self.log(
            logging.DEBUG,
            f"File ({local_file}) needs to be uploaded in parts to the "
            f"stage area of FirecREST and then moved to the "
            f"target directory, since it's {local_file_size} bytes."
        )

        if transfer_method == "wormhole":
            if not blocking:
                self.log(
                    logging.WARNING,
                    "For the wormhole upload `blocking=True` is mandatory."
                )

            # Generate wormhole code using external CLI

            self.log(
                logging.DEBUG,
                "Will upload through wormhole."
            )

            wormhole_cmd = [
                "wormhole",
                "send",
                str(local_file),
                "--code-length",
                "4"
            ]
            # Start wormhole process and capture code from stderr
            wormhole_proc: Any = await asyncio.create_subprocess_exec(
                *wormhole_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            wormhole_code = None
            while True:
                line = await wormhole_proc.stderr.readline()
                if not line:
                    break
                decoded_line = line.decode().strip()
                if "Wormhole code is:" in decoded_line:
                    wormhole_code = decoded_line.split(":")[-1].strip()
                    self.log(
                        logging.DEBUG,
                        f"Upload through wormhole: {wormhole_code}."
                    )
                    break

            if not wormhole_code:
                await wormhole_proc.wait()
                stderr = await wormhole_proc.stderr.read()
                raise MultipartUploadException(
                    {},
                    f"Failed to get wormhole code: {stderr.decode()}"
                )

            data = {
                "path": os.path.join(directory, filename),
                "transfer_directives": {
                    "wormholeCode": wormhole_code,
                    "transfer_method": transfer_method
                }
            }

            if account is not None:
                data["account"] = account

            resp = await self._post_request(
                endpoint=f"/filesystem/{system_name}/transfer/upload",
                json_data=data
            )

            transfer_info = self._check_response(resp, 201)
            ext_upload = AsyncExternalUpload(
                client=self,
                transfer_info=transfer_info,
                local_file=local_file,
            )

            await ext_upload.wait_for_transfer_job()

            # Wait for the wormhole command of upload to finish
            rc = await wormhole_proc.wait()
            if rc != 0:
                raise MultipartUploadException(
                    transfer_info,
                    f"Wormhole send failed with exit code {rc}"
                )

            return ext_upload

        if self._api_version < parse("2.4.0"):
            data = {
                "source_path": directory,
                "fileName": filename,
                "fileSize": local_file_size,
            }
        else:
            data = {
                "path": os.path.join(directory, filename),
                "transfer_directives": {
                    "file_size": local_file_size,
                    "transfer_method": transfer_method
                }
            }

        if account is not None:
            data["account"] = account

        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/upload",
            json_data=data
        )

        transfer_info = self._check_response(resp, 201)
        ext_upload = AsyncExternalUpload(
            client=self,
            transfer_info=transfer_info,
            local_file=local_file,
        )

        trans_dir = transfer_info.get("transferDirectives", {})
        if trans_dir:
            actual_transfer_method = trans_dir.get(
                "transferMethod",
                trans_dir.get("transfer_method", "s3")
            )
        else:
            actual_transfer_method = "s3"

        if blocking:
            self.log(
                logging.DEBUG,
                f"Blocking until ({local_file}) is transfered to the "
                f"filesystem."
            )
            if actual_transfer_method == "s3":
                # Upload the file in parts
                await ext_upload.upload_file_to_stage()

                # Wait for the file to be available in the target directory
                await ext_upload.wait_for_transfer_job()
            elif actual_transfer_method == "streamer":
                # Wait for job to start listening
                await ext_upload.wait_for_streamer_job_to_listen()

                # Directly upload the file via the streamer
                await ext_upload.upload_file_streamer()

        return ext_upload

    @_with_correlation_id  # type: ignore
    async def download(
        self,
        system_name: str,
        source_path: str,
        target_path: str | pathlib.Path,
        account: Optional[str] = None,
        blocking: bool = True,
        transfer_method: str = "s3"
    ) -> Optional[AsyncExternalDownload]:
        """Download a file from the remote system.

        :param system_name: the system name where the filesystem belongs to
        :param source_path: the absolute source path of the file
        :param target_path: the target path in the local filesystem (can
                            be relative path)
        :param account: the account to be used for the transfer job (only
                        relevant when the file is larger than
                        `MAX_DIRECT_UPLOAD_SIZE`)
        :param blocking: whether to wait for the job to complete
        :param transfer_method: the method to be used for the download of large
                                files. Supported methods: "s3", "streamer",
                                "wormhole".
        :calls: POST `/filesystem/{system_name}/transfer/download`
        """
        if transfer_method not in ["s3", "streamer", "wormhole"]:
            raise ValueError(
                f"Unsupported transfer_method '{transfer_method}'. Only 's3', "
                f"'streamer' and 'wormhole' are currently supported."
            )

        if not isinstance(target_path, (str, pathlib.Path)):
            raise TypeError(
                f"`target_path` must be a string or pathlib.Path, got "
                f"{type(target_path)}. For more options, consider using the "
                "serial Client."
            )

        # Check if the file is small enough to be downloaded directly
        try:
            file_info = await self.stat(system_name, source_path)
            file_size = file_info["size"]
        except FirecrestException as e:
            if (
                e.responses[-1].status_code == 404 and
                "No such file or directory" in e.responses[-1].json()['message']
            ):
                raise FileNotFoundError(
                    f"File not found: {source_path} on system {system_name}"
                )
            else:
                raise e

        if file_size < self.MAX_DIRECT_UPLOAD_SIZE:
            self.log(
                logging.DEBUG,
                f"File ({source_path}) will be directly downloaded to the "
                f"target directory, since it's {file_size} bytes."
            )
            self.log(
                logging.DEBUG,
                "Arguments `account` and `blocking` will be ignored."
            )
            resp = await self._get_request(
                endpoint=f"/filesystem/{system_name}/ops/download",
                params={
                    "path": source_path,
                }
            )
            self._check_response(resp, 200, return_json=False)

            async with aiofiles.open(target_path, "wb") as f:
                await f.write(resp.content)

            return None

        data: dict[str, Any] = {}
        if self._api_version < parse("2.4.0"):
            data = {
                "source_path": source_path,
            }
        else:
            data = {
                "source_path": source_path,
                "transfer_directives": {
                    "transferMethod": transfer_method
                }
            }

        if account is not None:
            data["account"] = account

        resp = await self._post_request(
            endpoint=f"/filesystem/{system_name}/transfer/download",
            json_data=data
        )

        transfer_info = self._check_response(resp, 201)
        download_obj = AsyncExternalDownload(
            client=self,
            transfer_info=transfer_info,
            file_path=target_path
        )

        trans_dir = transfer_info.get("transferDirectives", {})
        if trans_dir:
            actual_transfer_method = trans_dir.get(
                "transferMethod",
                trans_dir.get("transfer_method", "s3")
            )
        else:
            actual_transfer_method = "s3"

        if blocking:
            self.log(
                logging.DEBUG,
                f"Blocking until ({source_path}) is transfered to the "
                f"filesystem."
            )
            if actual_transfer_method == "s3":
                await download_obj.wait_for_transfer_job()
                await download_obj.download_file_from_stage()
            elif actual_transfer_method == "streamer":
                await download_obj.wait_for_streamer_job_to_listen()
                await download_obj.download_file_streamer()
            elif actual_transfer_method == "wormhole":
                await download_obj.receive_file_wormhole()

        return download_obj

    @_with_correlation_id  # type: ignore
    async def submit(
        self,
        system_name: str,
        working_dir: str,
        script_str: Optional[str] = None,
        script_local_path: Optional[str] = None,
        script_remote_path: Optional[str] = None,
        env_vars: Optional[dict[str, str]] = None,
        account: Optional[str] = None,
        partition: Optional[str] = None,
        reservation: Optional[str] = None
    ) -> dict:
        """Submit a job.

        :param system_name: the system name where the filesystem belongs to
        :param working_dir: the working directory of the job
        :param script_str: the job script
        :param script_local_path: path to the job script
        :param script_remote_path: path to the job script on the remote
                                   filesystem
        :param env_vars: environment variables to be set before running the
                         job
        :param account: the account to be used for the job
        :param partition: the partition to be used for the job
        :param reservation: the reservation to be used for the job
        :calls: POST `/compute/{system_name}/jobs`
        """

        if sum(
            arg is not None for arg in [
                script_str,
                script_local_path,
                script_remote_path
            ]
        ) != 1:
            raise ValueError(
                "Exactly one of the arguments `script_str`, "
                "`script_local_path` or `script_remote_path` must "
                "be set."
            )

        data: dict = {
            "job": {
                "working_directory": working_dir
            }
        }
        if script_remote_path:
            if self._api_version < parse("2.2.6"):
                raise NotImplementedOnAPIversion(
                    "submitting a job from a remote script path is not "
                    "supported in API version <2.2.6."
                )

            if not script_remote_path.startswith("/"):
                raise ValueError(
                    "The `script_remote_path` must be an absolute path."
                )

            data["job"]["scriptPath"] = script_remote_path

        else:
            if script_local_path:
                if not os.path.isfile(script_local_path):
                    raise FileNotFoundError(
                        f"Script file not found: {script_local_path}"
                    )
                with open(script_local_path) as file:
                    script_str = file.read()

            data["job"]["script"] = script_str

        if env_vars:
            data["job"]["env"] = env_vars

        if account:
            if self._api_version < parse("2.2.6"):
                raise NotImplementedOnAPIversion(
                    "account information is not available for "
                    "version <2.2.6 of the API."
                )

            data["job"]["account"] = account

        if partition is not None:
            if self._api_version < parse("2.5.4"):
                raise NotImplementedOnAPIversion(
                    "partition override is not available for "
                    "version <2.5.4 of the API."
                )

            data["job"]["partition"] = partition

        if reservation is not None:
            if self._api_version < parse("2.5.4"):
                raise NotImplementedOnAPIversion(
                    "reservation override is not available for "
                    "version <2.5.4 of the API."
                )

            data["job"]["reservation"] = reservation

        resp = await self._post_request(
            endpoint=f"/compute/{system_name}/jobs",
            json_data=data
        )
        return self._check_response(resp, 201)

    @_with_correlation_id  # type: ignore
    async def job_info(
        self,
        system_name: str,
        jobid: Optional[str] = None,
        allusers: bool = False,
        account: Optional[str] = None,
        name: Optional[str] = None,
        time_window: Optional[str] = None
    ) -> list:
        """Get job information. When the job is not specified, it will return
        all the jobs.

        :param system_name: the system name where the filesystem belongs to
        :param jobid: the ID of the job
        :param allusers: whether to return jobs of all users or only the
                         current user
        :param account: an account to filter the jobs by. It will only be taken
                        into account when you are not specifying a jobid.
        :param name: a job name to filter the jobs by. It will only be taken
                     into account when you are not specifying a jobid.
        :param time_window: how far back to look for historical (completed,
                            failed, cancelled...) jobs. Pending and running
                            jobs are always returned. Accepted values are
                            `1h`, `8h`, `24h`, `3d` and `7d`. It will
                            only be taken into account when you are not
                            specifying a jobid. Has no effect on PBS
                            clusters.
        :calls: GET `/compute/{system_name}/jobs` or
                GET `/compute/{system_name}/jobs/{job}`
        """
        url = f"/compute/{system_name}/jobs"
        url = f"{url}/{jobid}" if jobid else url

        if self._api_version < parse("2.2.7") and allusers:
            raise NotImplementedOnAPIversion(
                "The `allusers` parameter is not available for "
                "version <2.2.7 of the API."
            )

        if self._api_version < parse("2.4.2") and account:
            raise NotImplementedOnAPIversion(
                "The `account` parameter is not available for "
                "version <2.4.2 of the API."
            )

        if self._api_version < parse("2.6.0") and name:
            raise NotImplementedOnAPIversion(
                "The `name` parameter is not available for "
                "version <2.6.0 of the API."
            )

        if self._api_version < parse("2.6.0") and time_window:
            raise NotImplementedOnAPIversion(
                "The `time_window` parameter is not available for "
                "version <2.6.0 of the API."
            )

        resp = await self._get_request(
            endpoint=url,
            params={
                "allusers": allusers,
                "account": account,
                "name": name,
                "time_window": time_window,
            }
        )
        result_jobs = self._check_response(resp, 200)["jobs"]
        return result_jobs if result_jobs is not None else []

    @_with_correlation_id  # type: ignore
    async def job_metadata(
        self,
        system_name: str,
        jobid: str,
    ) -> dict:
        """Get job metadata.

        :param system_name: the system name where the filesystem belongs to
        :param jobid: the ID of the job
        :calls: GET `/compute/{system_name}/jobs/{jobid}/metadata`
        """
        resp = await self._get_request(
            endpoint=f"/compute/{system_name}/jobs/{jobid}/metadata",
        )
        return self._check_response(resp, 200)['jobs']

    @_with_correlation_id  # type: ignore
    async def cancel_job(
        self,
        system_name: str,
        jobid: str,
    ) -> dict:
        """Cancel a job.

        :param system_name: the system name where the filesystem belongs to
        :param jobid: the ID of the job to be cancelled
        :calls: DELETE `/compute/{system_name}/jobs/{jobid}`
        """
        resp = await self._delete_request(
            endpoint=f"/compute/{system_name}/jobs/{jobid}",
            idempotent=True,
        )
        if (
            resp.status_code == 500 and
            resp.extensions.get("firecrest_retries", 0) > 0 and
            "already completing or completed" in _response_message(resp)
        ):
            # An earlier attempt (or the job itself) already ended the job,
            # so the goal of the call is reached
            self.log(
                logging.INFO,
                f"Job {jobid} is already completing or completed, "
                f"probably cancelled by a previous attempt of the request"
            )
            return {}

        return self._check_response(resp, 204)

    @_with_correlation_id  # type: ignore
    async def attach_to_job(
        self,
        system_name: str,
        jobid: str,
        command: str,
    ) -> dict:
        """Attach a process to a job.

        :param system_name: the system name where the filesystem belongs to
        :param jobid: the ID of the job
        :param command: the command to be executed
        :calls: PUT `/compute/{system_name}/jobs/{jobid}/attach`
        """
        resp = await self._put_request(
            endpoint=f"/compute/{system_name}/jobs/{jobid}/attach",
            json_data={"command": command}
        )
        return self._check_response(resp, 204)
