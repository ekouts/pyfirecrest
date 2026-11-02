import email.utils as eut
import logging
import time

from contextlib import contextmanager
from packaging.version import parse
from xml.etree import ElementTree

import firecrest.FirecrestException as fe


@contextmanager
def time_block(label, logger):
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        logger.debug(f"{label} took {end_time - start_time:.6f} seconds")


def sched_state_completed(state):
    completion_states = {
        'BOOT_FAIL',
        'CANCELLED',
        'COMPLETED',
        'DEADLINE',
        'FAILED',
        'NODE_FAIL',
        'OUT_OF_MEMORY',
        'PREEMPTED',
        'TIMEOUT',
        'F', # PBS state 'F': job Finished
    }
    if state:
        # Make sure all the steps include one of the completion states
        return all(
            any(cs in s for cs in completion_states) for s in state.split(',')
        )

    return False


def sched_state_running(state):
    running_states = {
        'RUNNING',
        'R', # PBS state 'R': job Running
    }
    return any(
        any(rs in s for rs in running_states)
        for s in state.split(',')
    )


def parse_retry_after(retry_after_header, log_func, fallback=10):
    """
    Parse the Retry-After header.

    :param retry_after_header: The value of the Retry-After header.
    :param log_func: logging callback ``(level, message)``.
    :param fallback: the value returned when the header cannot be parsed.
    :return: A non-negative floating point number representing when the retry
    should occur.
    """
    try:
        # Try to parse it as a delta-seconds
        delta_seconds = int(retry_after_header)
        return max(delta_seconds, 0)
    except ValueError:
        pass

    try:
        # Try to parse it as an HTTP-date
        retry_after_date = eut.parsedate_to_datetime(retry_after_header)
        delta_seconds = retry_after_date.timestamp() - time.time()
        return max(delta_seconds, 0)
    except Exception:
        log_func(
            logging.WARNING,
            f"Could not parse Retry-After header: {retry_after_header}"
        )
        return fallback


def validate_api_version_compatibility():
    def decorator(func):
        def wrapper(*args, **kwargs):
            client = args[0]
            if client._query_api_version:
                # This will set the version in the client as a side
                # effect
                client.parameters()

            function_name = func.__name__
            min_version = missing_api_features.get(
                function_name, {}
            ).get('min_version', None)

            if min_version and client._api_version < min_version:
                raise fe.NotImplementedOnAPIversion(
                    f"function `{function_name}` is not available for "
                    f"version <{min_version} in the client."
                )

            return func(*args, **kwargs)
        return wrapper
    return decorator


def async_validate_api_version_compatibility():
    def decorator(func):
        async def wrapper(*args, **kwargs):

            client = args[0]
            if client._query_api_version:
                # This will set the version in the client as a side
                # effect
                await client.parameters()

            function_name = func.__name__
            min_version = missing_api_features.get(
                function_name, {}
            ).get('min_version', None)

            if min_version and client._api_version < min_version:
                raise fe.NotImplementedOnAPIversion(
                    f"function `{function_name}` is not available for "
                    f"version <{min_version} in the client."
                )

            return await func(*args, **kwargs)
        return wrapper
    return decorator


missing_api_features = {
    'compress': {
        # Using dictionaries in case we have max_version at
        # some point
        'min_version': parse("1.16.0"),
    },
    'extract': {
        'min_version': parse("1.16.0"),
    },
    'filesystems': {
        'min_version': parse("1.15.0"),
    },
    'groups': {
        'min_version': parse("1.15.0"),
    },
    'nodes': {
        'min_version': parse("1.16.0"),
    },
    'partitions': {
        'min_version': parse("1.16.0"),
    },
    'reservations': {
        'min_version': parse("1.16.0"),
    },
    'submit_compress_job': {
        'min_version': parse("1.16.0"),
    },
    'submit_extract_job': {
        'min_version': parse("1.16.0"),
    },
}


def part_checksum_xml(all_tags):
    root = ElementTree.Element(
        'CompleteMultipartUpload', {'xmlns': "http://s3.amazonaws.com/doc/2006-03-01/"}
    )
    for p in all_tags:
        part_element = ElementTree.SubElement(root, 'Part')
        part_etag = ElementTree.SubElement(part_element, 'ETag')
        part_etag.text = p['ETag']
        part_number = ElementTree.SubElement(part_element, 'PartNumber')
        part_number.text = str(p['PartNumber'])
    xml_body = ElementTree.tostring(root, encoding='unicode', method='xml')
    return '<?xml version=\'1.0\' encoding=\'utf-8\'?>\n' + xml_body
