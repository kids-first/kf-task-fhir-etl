"""
Builds FHIR ResearchStudy resources (https://www.hl7.org/fhir/researchstudy.html)
from rows of tabular study metadata.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import PractitionerRole
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/research-study-status
status_code = "completed"

category_coding = {
    "BIRTHDEFECT": {
        "system": "http://snomed.info/sct",
        "code": "276720006",
        "display": "Dysmorphism (disorder)",
    },
    "CANCER": {
        "system": "http://snomed.info/sct",
        "code": "86049000",
        "display": "Malignant neoplasm, primary (morphologic abnormality)",
    },
    "COVID19": {
        "system": "http://snomed.info/sct",
        "code": "840539006",
        "display": "Disease caused by Severe acute respiratory syndrome coronavirus 2",
    },
}


class ResearchStudy:
    class_name = "research_study"
    api_path = "ResearchStudy"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.STUDY.TARGET_SERVICE_ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        external_id = record[CONCEPT.STUDY.ID]
        version = record.get(CONCEPT.STUDY.VERSION)
        study_name = record.get(CONCEPT.STUDY.NAME)
        domain = record.get("STUDY|DOMAIN")
        program = record.get("STUDY|PROGRAM")
        short_code = record.get("STUDY|SHORT_CODE")
        investigator_id = record.get(CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
                "tag": [{"code": study_id}],
            },
            "identifier": [
                {
                    "use": "official",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/studies/",
                    "value": study_id,
                },
            ],
            "status": status_code,
        }

        # identifier
        if external_id and external_id.startswith("phs"):
            accession = external_id.split(".")[0].strip()
            if version and version.startswith("v"):
                accession = ".".join([accession, version.strip()])
            entity["identifier"].append(
                {
                    "use": "secondary",
                    "system": "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=",
                    "value": accession,
                }
            )

        # title
        if study_name:
            entity["title"] = study_name

        # cateogry
        category = {}
        if domain:
            category["text"] = domain
            if category_coding.get(domain):
                category.setdefault("coding", []).append(category_coding[domain])
            elif domain == "CANCERANDBIRTHDEFECT":
                category["coding"] = [
                    category_coding["CANCER"],
                    category_coding["BIRTHDEFECT"],
                ]
        if category:
            entity.setdefault("category", []).append(category)

        # keyword
        if program:
            entity.setdefault("keyword", []).append({"coding": [{"code": program}]})
        if short_code:
            entity.setdefault("keyword", []).append({"coding": [{"code": short_code}]})

        # principalInvestigator
        if investigator_id:
            practitioner_role_id = not_none(
                get_target_id_from_record(PractitionerRole, record)
            )
            entity["principalInvestigator"] = {
                "reference": f"{PractitionerRole.api_path}/{practitioner_role_id}",
            }

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
