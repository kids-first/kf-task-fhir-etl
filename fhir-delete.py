import sys
import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pprint import pformat

from dotenv import find_dotenv, load_dotenv
from requests import RequestException

from d3b_utils.requests_retry import Session

DOTENV_PATH = find_dotenv()
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)

FHIR_API = os.getenv("KF_API_FHIR_SERVICE_URL")
FHIR_USERNAME = os.getenv("FHIR_USERNAME")
FHIR_PASSWORD = os.getenv("FHIR_PASSWORD")


class CustomParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write(f"\nerror: {message}\n\n")
        if not isinstance(sys.exc_info()[1], argparse.ArgumentError):
            self.print_help()
        sys.exit(2)


def delete_resource(session, url, headers, auth):
    resp = session.delete(url, headers=headers, auth=auth)
    return resp, url


def consume_futures(futures):
    for future in as_completed(futures):
        resp, url = future.result()
        try:
            resp.raise_for_status()
            print(
                f"🔥 DELETE {url} - {resp.status_code}\n:{pformat(resp.json())}"
            )
            print()
        except RequestException as e:
            print(e)
            # raise e


# Instantiate a parser
parser = CustomParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

# Required arguments
parser.add_argument("endpoint", help="Study to be deleted")
parser.add_argument("study", help="Study to be deleted")

# Optional arguments
parser.add_argument(
    "--params",
    required=False,
    default=None,
    help="A query string (e.g., 'code=FAMMEMB&value-concept=MTH')",
)

# Parse arguments
args = parser.parse_args()
endpoint, study, params = args.endpoint, args.study, args.params

print(f"🚀 Start deleting {endpoint} ({study}) from {FHIR_API}!")

session = Session()
link_next = os.path.join(FHIR_API.rstrip("/"), endpoint.lstrip("/"))
params = {
    **{"_tag": study},
    **(
        {param.split("=")[0]: param.split("=")[1] for param in params.split("&")}
        if params is not None
        else {}
    ),
}
headers = {"Content-Type": "application/json+fhir"}
auth = (FHIR_USERNAME, FHIR_PASSWORD)
resource_id_list = []

while link_next is not None:
    resp = session.get(
        link_next,
        params=params,
        headers=headers,
        auth=auth,
    )

    try:
        resp.raise_for_status()
    except RequestException as e:
        raise e

    bundle = resp.json()
    link_next = None

    for link in bundle.get("link", []):
        if link["relation"] == "next":
            link_next = link["url"]
            link_next = link_next.replace(
                "http://localhost:8000",
                FHIR_API,
            )
            params = None

    for entry in bundle.get("entry", []):
        resource_id = entry["resource"]["id"]
        resource_id_list.append(resource_id)

with ThreadPoolExecutor() as tpex:
    futures = []
    for resource_id in resource_id_list:
        url = os.path.join(
            FHIR_API.rstrip("/"),
            endpoint.lstrip("/"),
            resource_id,
        )
        futures.append(
            tpex.submit(
                delete_resource,
                session,
                url,
                headers,
                auth,
            )
        )
    consume_futures(futures)

print(f"🎉 Close deleting {endpoint} ({study}) from {FHIR_API}.")
