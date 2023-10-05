"""
Builds FHIR Condition resources (https://www.hl7.org/fhir/condition.html) from
rows of tabular participant phenotype data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.common.constants import MISSING_DATA_VALUES
from kf_task_fhir_etl.target_api_plugins.entity_builders import Patient
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/condition-ver-status
verification_status_coding = {
    constants.PHENOTYPE.OBSERVED.YES: {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "confirmed",
        "display": "Confirmed",
    },
    constants.PHENOTYPE.OBSERVED.NO: {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "refuted",
        "display": "Refuted",
    },
}


class Phenotype:
    class_name = "phenotype"
    api_path = "Condition"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        assert not_none(get_target_id_from_record(Patient, record))
        return {
            "_tag": record[CONCEPT.STUDY.TARGET_SERVICE_ID],
            "identifier": not_none(record[CONCEPT.PHENOTYPE.TARGET_SERVICE_ID]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        phenotype_id = record[CONCEPT.PHENOTYPE.TARGET_SERVICE_ID]
        observed = record[CONCEPT.PHENOTYPE.OBSERVED]
        name = record[CONCEPT.PHENOTYPE.NAME]
        hpo_id = record.get(CONCEPT.PHENOTYPE.HPO_ID)
        snomed_id = record.get(CONCEPT.PHENOTYPE.SNOMED_ID)
        event_age_days = record.get(CONCEPT.PHENOTYPE.EVENT_AGE_DAYS)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [
                    "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/phenotype",
                ],
                "tag": [
                    {
                        "system": "https://kf-api-dataservice.kidsfirstdrc.org/studies/",
                        "code": study_id,
                    }
                ],
            },
            "identifier": [
                {
                    "use": "official",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/phenotypes/",
                    "value": phenotype_id,
                }
            ],
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
        }

        # verificationStatus
        verification_status = {"text": observed}
        if verification_status_coding.get(observed):
            verification_status.setdefault("coding", []).append(
                verification_status_coding[observed]
            )
        entity["verificationStatus"] = verification_status

        # code
        code = {"text": name}
        if hpo_id and hpo_id not in MISSING_DATA_VALUES:
            code.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/hp.owl",
                    "code": hpo_id,
                }
            )
        if snomed_id and snomed_id not in MISSING_DATA_VALUES:
            code.setdefault("coding", []).append(
                {
                    "system": "http://snomed.info/sct",
                    "code": snomed_id,
                }
            )
        entity["code"] = code

        # recordedDate
        try:
            entity["_recordedDate"] = {
                "extension": [
                    {
                        "extension": [
                            {
                                "url": "target",
                                "valueReference": {
                                    "reference": "/".join(
                                        [
                                            Patient.api_path,
                                            not_none(
                                                get_target_id_from_record(
                                                    Patient, record
                                                )
                                            ),
                                        ]
                                    )
                                },
                            },
                            {
                                "url": "targetPath",
                                "valueString": "birthDate",
                            },
                            {
                                "url": "relationship",
                                "valueCode": "after",
                            },
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
                        "url": "http://hl7.org/fhir/StructureDefinition/cqf-relativeDateTime",
                    }
                ]
            }
        except:
            pass

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
