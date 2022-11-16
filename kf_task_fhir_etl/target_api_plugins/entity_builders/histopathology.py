"""
Builds FHIR Observation resources (https://www.hl7.org/fhir/observation.html)
from rows of tabular disease biospecimen association data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import (
    Patient,
    Disease,
    Specimen,
)
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/observation-status
status_code = "final"


class Histopathology:
    class_name = "histopathology"
    api_path = "Observation"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "identifier": not_none(
                record[CONCEPT.BIOSPECIMEN_DIAGNOSIS.TARGET_SERVICE_ID]
            )
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        biospecimen_diagnosis_id = record[
            CONCEPT.BIOSPECIMEN_DIAGNOSIS.TARGET_SERVICE_ID
        ]

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
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/biospecimen-diagnoses/",
                    "value": biospecimen_diagnosis_id,
                }
            ],
            "status": status_code,
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "laboratory",
                            "display": "Laboratory",
                        }
                    ],
                    "text": "Histopathology",
                }
            ],
            "code": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "250537006",
                        "display": "Histopathology finding (finding)",
                    }
                ],
                "text": "Histopathology",
            },
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
            "focus": [
                {
                    "reference": "/".join(
                        [
                            Disease.api_path,
                            not_none(get_target_id_from_record(Disease, record)),
                        ]
                    )
                }
            ],
            "specimen": {
                "reference": "/".join(
                    [
                        Specimen.api_path,
                        not_none(get_target_id_from_record(Specimen, record)),
                    ]
                )
            },
        }

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
