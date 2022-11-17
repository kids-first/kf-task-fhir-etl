"""
Builds FHIR Observation resources (https://www.hl7.org/fhir/observation.html)
from rows of tabular participant vital status data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import Patient
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/observation-status
status_code = "final"

code_coding = {
    constants.OUTCOME.VITAL_STATUS.ALIVE: {
        "system": "http://snomed.info/sct",
        "code": "438949009",
        "display": "Alive (finding)",
    },
    constants.OUTCOME.VITAL_STATUS.DEAD: {
        "system": "http://snomed.info/sct",
        "code": "419099009",
        "display": "Dead (finding)",
    },
}


class VitalStatus:
    class_name = "vital_status"
    api_path = "Observation"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.OUTCOME.TARGET_SERVICE_ID])}

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        outcome_id = record[CONCEPT.OUTCOME.TARGET_SERVICE_ID]
        vital_status = record.get(CONCEPT.OUTCOME.VITAL_STATUS)
        event_age_days = record.get(CONCEPT.OUTCOME.EVENT_AGE_DAYS)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [
                    "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/vital-status"
                ],
                "tag": [{"code": study_id}],
            },
            "identifier": [
                {
                    "use": "official",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/outcomes/",
                    "value": outcome_id,
                }
            ],
            "status": status_code,
            "code": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "263493007",
                        "display": "Clinical status (attribute)",
                    }
                ],
                "text": "Clinical status",
            },
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
        }

        # effectiveDateTime
        try:
            entity["_effectiveDateTime"] = {
                "extension": [
                    {
                        "extension": [
                            {
                                "url": "event",
                                "valueCodeableConcept": {
                                    "coding": [
                                        {
                                            "system": "http://snomed.info/sct",
                                            "code": "3950001",
                                            "display": "Birth",
                                        }
                                    ]
                                },
                            },
                            {"url": "relationship", "valueCode": "after"},
                            {
                                "url": "offset",
                                "valueDuration": {
                                    "value": int(event_age_days),
                                    "unit": "day",
                                    "system": "http://unitsofmeasure.org",
                                    "code": "d",
                                },
                            },
                        ],
                        "url": "http://hl7.org/fhir/StructureDefinition/relative-date",
                    }
                ]
            }
        except:
            pass

        # valueCodeableConcept
        if vital_status:
            value = {"text": vital_status}
            if code_coding.get(vital_status):
                value.setdefault("coding", []).append(code_coding[vital_status])
            entity["valueCodeableConcept"] = value

        return entity

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @abstractmethod
    def submit(cls, host, body):
        pass
