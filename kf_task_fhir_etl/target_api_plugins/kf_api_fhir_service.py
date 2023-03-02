import os

from dotenv import find_dotenv, load_dotenv
from requests import RequestException
from d3b_utils.requests_retry import Session

from kf_task_fhir_etl.target_api_plugins.entity_builders import (
    Practitioner,
    Organization,
    PractitionerRole,
    Patient,
    ProbandStatus,
    FamilyRelationship,
    Family,
    ResearchStudy,
    ResearchSubject,
    Disease,
    Phenotype,
    VitalStatus,
    SequencingCenter,
    Specimen,
    Histopathology,
    DRSDocumentReference,
)

LOADER_VERSION = 2

DOTENV_PATH = find_dotenv()
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)

FHIR_COOKIE = os.getenv("FHIR_COOKIE")
FHIR_USERNAME = os.getenv("FHIR_USERNAME")
FHIR_PASSWORD = os.getenv("FHIR_PASSWORD")


def _PUT(host, api_path, resource_id, body, headers, auth=None):
    return Session().put(
        "/".join([v.strip("/") for v in [host, api_path, resource_id]]),
        json=body,
        headers=headers,
        auth=auth,
    )


def _POST(host, api_path, body, headers, auth=None):
    return Session().post(
        "/".join([v.strip("/") for v in [host, api_path]]),
        json=body,
        headers=headers,
        auth=auth,
    )


def submit(entity_class, host, body):
    """Negotiates submitting the data for an entity to the target service.

    :param entity_class: Which entity class is being sent
    :type entity_class: class
    :param host: A host url
    :type host: str
    :param body: Map between entity keys and values
    :type body: dict
    :raise: RequestException on error
    :return: The target entity ID that the service says was created or updated
    :rtype: str
    """
    headers = {
        "Content-Type": "application/fhir+json;charset=utf-8",
        "Cookie": FHIR_COOKIE,
    }
    auth = (FHIR_USERNAME, FHIR_PASSWORD)

    resp = None
    api_path = entity_class.api_path
    resource_id = body.get("id")

    if resource_id:
        resp = _PUT(host, api_path, resource_id, body, headers=headers, auth=auth)
        if (resp.status_code not in {200, 201}) and (
            "no resource with this ID exists"
            in resp.get("issue", [{}])[0].get("diagnostics", "")
        ):
            resp = None
    else:
        body.pop("id", None)

    if not resp:
        resp = _POST(host, api_path, body, headers=headers, auth=auth)

    if resp.status_code in {200, 201}:
        return resp.json()["id"]
    else:
        raise RequestException(f"Sent to /{api_path}:\n{body}\nGot:\n{resp.text}")


# Override submitter
Practitioner.submit = classmethod(submit)
Organization.submit = classmethod(submit)
PractitionerRole.submit = classmethod(submit)
Patient.submit = classmethod(submit)
ProbandStatus.submit = classmethod(submit)
FamilyRelationship.submit = classmethod(submit)
Family.submit = classmethod(submit)
ResearchStudy.submit = classmethod(submit)
ResearchSubject.submit = classmethod(submit)
Disease.submit = classmethod(submit)
Phenotype.submit = classmethod(submit)
VitalStatus.submit = classmethod(submit)
SequencingCenter.submit = classmethod(submit)
Specimen.submit = classmethod(submit)
Histopathology.submit = classmethod(submit)
DRSDocumentReference.submit = classmethod(submit)

all_targets = [
    Practitioner,
    Organization,
    PractitionerRole,
    Patient,
    ProbandStatus,
    FamilyRelationship,
    Family,
    ResearchStudy,
    ResearchSubject,
    Disease,
    Phenotype,
    VitalStatus,
    SequencingCenter,
    Specimen,
    Histopathology,
    DRSDocumentReference,
]
