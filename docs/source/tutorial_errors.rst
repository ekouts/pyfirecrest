How to catch and debug errors
=============================

The methods of the ``Firecrest``, ``ExternalUpload`` and ``ExternalDownload`` objects will raise exceptions in case something goes wrong.
The same happens for their asynchronous counterparts.
When the error comes from the response of some request pyFirecREST will raise ``FirecrestException``.
In these cases you can manually examine all the responses from the requests in order to get more information, when the message is not informative enough.
These responses are from the requests package of python and you can get all types of useful information from it, like the status code, the json response, the headers and more.
Here is an example of the code that will handle those failures.

.. code-block:: Python

    import firecrest as fc


    try:
        files = client.list_files("cluster", "/home/test_user")
        print(f"List of files: {files}")
    except fc.FirecrestException as e:
        # You can just print the exception to get more information about the type of error,
        # for example an invalid or expired token.
        print(e)
        # Or you can manually examine the responses.
        print(e.responses[-1])
        print(e.responses[-1].status_code)
        print(e.responses[-1].body)
    except Exception as e:
        # You might also get regular exceptions in some cases. For example when you are
        # trying to upload a file that doesn't exist in your local filesystem.
        print(f"A different exception was encountered: {e}")

Retrying failed requests
------------------------

Transient failures, like an unhealthy scheduler or a dropped connection to the cluster, are retried automatically by the v2 clients according to their ``retry_policy`` attribute.
The policy is a table of rules, one per HTTP status code or ``httpx`` exception class, and every rule has its own number of retries and backoff.
The default table (``firecrest.v2.DEFAULT_RETRY_RULES``) is:

.. list-table::
   :header-rows: 1

   * - Key
     - Retried for
     - Why
   * - 429 (rate limit)
     - every request, until a different status is received
     - the client sleeps as instructed by the ``Retry-After`` header
   * - 503, 424, 502
     - every request, 3 retries
     - the server returns them before running any command on the cluster
   * - 504, ``httpx.ReadTimeout``, ``httpx.RemoteProtocolError``
     - idempotent requests only, 3 retries
     - the command may have run on the cluster; a job submission or a transfer is never repeated
   * - ``httpx.ConnectError``, ``httpx.ConnectTimeout``
     - every request, 3 retries
     - the request never reached the server

Between retries the client sleeps ``backoff_factor * 2**n`` seconds (1, 2 and 4 seconds by default) with some random jitter, capped at ``max_backoff``.
Status codes without a rule, like permission or validation errors, are reported immediately with ``UnexpectedStatusException``.
When the retries of a rule are exhausted the last response is reported as usual, and the number of retries that were made is available in ``e.responses[-1].extensions["firecrest_retries"]``.
The number of retries and the backoff of the default rules are provisional and may change in future versions.

Idempotent requests are the ones that only read information (``systems``, ``list_files``, ``job_info``...) and the writes that can be safely repeated (``chmod``, ``chown``, ``cancel_job``).
For ``cancel_job``, an error reporting that the job is already completing or completed is treated as a success when it follows a retry, since the previous attempt may have cancelled the job.

The table can be edited in place, for example:

.. code-block:: Python

    import firecrest as fc
    from firecrest.v2 import RetryPolicy, RetryRule

    client = fc.v2.Firecrest(firecrest_url=..., authorization=...)

    # More patience when the scheduler is unhealthy
    client.retry_policy.rules[503].max_retries = 10
    client.retry_policy.rules[503].max_backoff = 60

    # Retry a timed out command (exit status 124) when it is safe to do so
    client.retry_policy.rules[500] = RetryRule(
        message="exit status:124", idempotent_only=True
    )

    # Never retry a failed SSH connection
    del client.retry_policy.rules[424]

    # Or disable the retries altogether, except for the rate limit
    client = fc.v2.Firecrest(
        firecrest_url=..., authorization=..., retry_policy=RetryPolicy.disabled()
    )

The rate limit rule can also be tuned through the ``num_retries_rate_limit`` attribute of the client, which is an alias of ``client.retry_policy.rules[429].max_retries``.
Every retry is logged at the ``INFO`` level by the ``firecrest`` logger, see :doc:`tutorial_logging`.
