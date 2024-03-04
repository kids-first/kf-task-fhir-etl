import os
import logging
from pprint import pformat

from dotenv import find_dotenv, load_dotenv
import requests
from requests import RequestException

from d3b_utils.requests_retry import Session

DOTENV_PATH = find_dotenv()
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)

FHIR_USERNAME = os.getenv("FHIR_USERNAME")
FHIR_PASSWORD = os.getenv("FHIR_PASSWORD")

logger = logging.getLogger(__name__)


def not_none(val):
    if val is None:
        raise ValueError("Missing required value")
    return val


def drop_none(body):
    return {k: v for k, v in body.items() if v is not None}


def yield_resources(host, endpoint, filters, show_progress=False):
    """Scrapes the FHIR service for paginated entities matching the filter params.
    Note: It's almost always going to be safer to use this than requests.get
    with search parameters, because you never know when you'll get back more
    than one page of results for a query.

    :param host: A FHIR service base URL (e.g. "http://localhost:8000")
    :type host: str
    :param endpoint: A FHIR service endpoint (e.g. "Patient")
    :type endpoint: str
    :param filters: dict of filters to winnow results from the FHIR service
        (e.g. {"name": "Children\'s Hospital of Philadelphia"})
    :type filters: dict
    :raises Exception: If the FHIR service doesn't return status 200
    :yields: resources matching the filters
    """
    url = f"{host.rstrip('/')}/{endpoint.lstrip('/')}"

    expected = 0
    link_next = url
    found_resource_ids = set()

    headers = {"Content-Type": "application/fhir+json;charset=utf-8"}
    auth = None

    if FHIR_USERNAME and FHIR_PASSWORD:
        auth = (FHIR_USERNAME, FHIR_PASSWORD)

    session = Session()
    while link_next is not None:
        resp = session.get(link_next, params=filters,
                           headers=headers, auth=auth)

        if resp.status_code != 200:
            raise RequestException(resp.text)

        bundle = resp.json()
        expected = bundle["total"]
        link_next = None

        for link in bundle.get("link", []):
            if link["relation"] == "next":
                link_next = link["url"]
                link_next = link_next.replace("http://localhost:8000", host)

        if show_progress and not expected:
            print("o", end="", flush=True)

        for entry in bundle.get("entry", []):
            resource_id = entry["resource"]["id"]
            if resource_id not in found_resource_ids:
                found_resource_ids.add(resource_id)
                if show_progress:
                    print(".", end="", flush=True)
                yield entry

    found = len(found_resource_ids)
    assert expected == found, f"Found {found} resources but expected {expected}"


def get_dataservice_entity(host, endpoint, params=None):
    """Get a dataservice entity."""
    resp = Session().get(
        f"{host.rstrip('/')}/{endpoint.lstrip('/')}",
        params=params,
        headers={"Content-Type": "application/json"},
    )

    try:
        resp.raise_for_status()
    except:
        raise RequestException(f"{resp.text}")

    return resp.json()


def yield_resource_ids(host, endpoint, filters, show_progress=False):
    """Simple wrapper around yield_resources that yields just the FHIR resource IDs"""
    for entry in yield_resources(host, endpoint, filters, show_progress):
        yield entry["resource"]["id"]


def send_request(method, *args, **kwargs):
    """Send http request. Raise exception on status_code >= 300

    :param method: name of the requests method to call
    :type method: str
    :raises: requests.Exception.HTTPError
    :returns: requests Response object
    :rtype: requests.Response
    """
    # NOTE: Set timeout so requests don't hang
    # See https://requests.readthedocs.io/en/latest/user/advanced/#timeouts
    if not kwargs.get("timeout"):
        # connect timeout, read timeout
        kwargs["timeout"] = (3, 60)
    logger.debug(
        f"⌚️ Applying timeout: {kwargs['timeout']} (connect, read)"
        " seconds to request"
    )

    requests_op = getattr(requests, method.lower())
    try:
        resp = requests_op(*args, **kwargs)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        body = ""
        try:
            body = pformat(resp.json())
        except:
            body = resp.text

        msg = (
            "❌ Problem sending request to server\n"
            f"{str(e)}\n"
            f"args: {args}\n"
            f"kwargs: {pformat(kwargs)}\n"
            f"{body}\n"
        )
        logger.error(msg)
        raise e

    return resp
