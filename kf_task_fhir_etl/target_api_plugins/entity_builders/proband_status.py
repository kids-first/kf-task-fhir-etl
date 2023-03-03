"""
Builds FHIR Observation resources (https://www.hl7.org/fhir/observation.html)
from rows of tabular participant proband status data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import Patient
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/observation-status
status_code = "final"

# http://terminology.hl7.org/CodeSystem/v2-0136
value_coding = {
    constants.COMMON.TRUE: {
        "system": "http://terminology.hl7.org/CodeSystem/v2-0136",
        "code": "Y",
        "display": "Yes",
    },
    constants.COMMON.FALSE: {
        "system": "http://terminology.hl7.org/CodeSystem/v2-0136",
        "code": "N",
        "display": "No",
    },
}


class ProbandStatus:
    class_name = "proband_status"
    api_path = "Observation"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        patient_id = not_none(get_target_id_from_record(Patient, record))
        proband_status = not_none(record[CONCEPT.PARTICIPANT.IS_PROBAND])
        assert proband_status in ["True", "False"]

        return {
            "code": "http://snomed.info/sct|85900004",
            "subject": f"{Patient.api_path}/{patient_id}",
        }

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        proband_status = record[CONCEPT.PARTICIPANT.IS_PROBAND]

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
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/participants?is_proband=",
                    "value": bool(proband_status),
                }
            ],
            "status": status_code,
            "code": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "85900004",
                        "display": "Proband (finding)",
                    }
                ],
                "text": "Proband status",
            },
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
            "valueCodeableConcept": {
                "coding": [value_coding[proband_status]],
                "text": proband_status,
            },
        }

        return entity

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @abstractmethod
    def submit(cls, host, body):
        pass
