"""
Builds FHIR PractitionerRole resources (https://www.hl7.org/fhir/practitionerrole.html)
from rows of tabular investigator metadata.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import (
    Practitioner,
    Organization,
)
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids


class PractitionerRole:
    class_name = "practitioner_role"
    api_path = "PractitionerRole"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        practitioner_id = not_none(get_target_id_from_record(Practitioner, record))
        organization_id = not_none(get_target_id_from_record(Organization, record))
        return {
            "practitioner": f"{Practitioner.api_path}/{practitioner_id}",
            "organization": f"{Organization.api_path}/{organization_id}",
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        kf_id = record[CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID]
        key_components = cls.get_key_components(record, get_target_id_from_record)
        external_id = record.get(CONCEPT.INVESTIGATOR.ID)

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
            "practitioner": {"reference": key_components["practitioner"]},
            "organization": {"reference": key_components["organization"]},
            "code": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/practitioner-role",
                            "code": "researcher",
                            "display": "Researcher",
                        }
                    ]
                }
            ],
        }

        # identifier
        if external_id:
            entity["identifier"].append(
                {
                    "use": "secondary",
                    "value": external_id,
                }
            )

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
