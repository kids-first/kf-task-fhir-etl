
import logging
from urllib.parse import urlparse
from kf_task_fhir_etl.config import (
    DCF_BASE_URL, INDEXD_BASE_URL, INDEXD_ENDPOINT, KF_API_DATASERVICE_URL
)
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.common.utils import send_request


DCF_HOST = urlparse(DCF_BASE_URL).netloc
DRS_URI_KEY = "drs_uri"

logger = logging.getLogger(__name__)


def _set_authorization(genomic_file):
    """
    Create the auth codes for the security label from genomic_file authz or acl field

    - authz takes precedence over acl
    - Ensure all values are in "old" acl format
    """
    authz = genomic_file.get("authz")
    if authz:
        new_codes = []
        for code in authz:
            new_code = code.split("/")[-1].strip()
            if new_code == "open":
                new_code = "*"
            new_codes.append(new_code)
    else:
        new_codes = genomic_file.get("acl") or []

    return new_codes


def _get_dcf_id(urls):
    """
    Look for DCF URLs and extract the Gen3 id from the url if it is
    a DCF URL
    """
    dcf_id = None
    for url in urls:
        parts = urlparse(url)
        is_dcf_host = DCF_HOST == parts.netloc
        is_gen3_url = parts.path.startswith("/" + INDEXD_ENDPOINT.lstrip("/"))
        is_drs_url = parts.path.startswith("/ga4gh/drs/v1/objects")
        if is_dcf_host and (is_gen3_url or is_drs_url):
            dcf_id = parts.path.split("/")[-1]
            break

    return dcf_id


def _drs_uri(gen3_url):
    """
    DRS URI from a Gen3 url 
    """
    parts = urlparse(gen3_url)
    did = parts.path.split("/")[-1]
    return f"drs://{parts.netloc}/{did}"


def get_dcf_file(dcf_id):
    """
    Fetch metadata from DCF
    """
    base_url = DCF_BASE_URL
    endpoint = INDEXD_ENDPOINT
    url = "/".join(part.strip("/")
                   for part in [base_url, endpoint]) + f"/{dcf_id}"
    logger.debug(f"Fetching DCF file {url}")

    headers = {"Content-Type": "application/json"}
    return send_request("get", url, headers=headers)


def update_gf_metadata(input_gf):
    """
    Fetch file metadata from appropriate Gen3 service and return updated gf 

    For NCI Gen3 (DCF) genomic files, fetch the metadata from NCI Gen3

    For KF Gen3 genomic files, do nothing since input_gf already has the
    metadata it needs from KF Gen3

    For both types of genomic files, add a DRS_URI field and populate it with 
    the drs uri
    """
    # Check if its a dcf file
    dcf_id = _get_dcf_id(input_gf["urls"])

    # Fetch metadata from dcf
    if dcf_id:
        resp = get_dcf_file(dcf_id)
        dcf_file = resp.json()
        for key in ["size", "urls", "hashes", "file_name", "acl", "authz"]:
            input_gf[key] = dcf_file.get(key)
        gen3_url = "/".join(
            part.strip("/") for part in [DCF_BASE_URL, dcf_id]
        )
    else:
        gen3_url = "/".join(
            part.strip("/")
            for part in [INDEXD_BASE_URL, input_gf["latest_did"]]
        )

    input_gf[DRS_URI_KEY] = _drs_uri(gen3_url)

    return input_gf
