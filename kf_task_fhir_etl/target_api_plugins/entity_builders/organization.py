"""
Builds FHIR Organization resources (https://www.hl7.org/fhir/organization.html)
from rows of tabular investigator metadata.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids


class Organization:
    class_name = "organization"
    api_path = "Organization"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        kf_id = record[CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID]
        institution = record.get(CONCEPT.INVESTIGATOR.INSTITUTION)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"]
            },
            "identifier": [
                {
                    "use": "official",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/investigators/",
                    "value": kf_id,
                }
            ],
            "active": True,
        }

        if institution:
            entity["name"] = institution

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
