"""
Builds FHIR Condition resources (https://www.hl7.org/fhir/condition.html) from
rows of tabular participant disease data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import Patient
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/condition-ver-status
verification_status_coding = {
    constants.COMMON.TRUE: {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "confirmed",
        "display": "Confirmed",
    },
    constants.COMMON.FALSE: {
        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
        "code": "refuted",
        "display": "Refuted",
    },
}

missing_data_values = {
    constants.COMMON.CANNOT_COLLECT,
    constants.COMMON.NO_MATCH,
    constants.COMMON.NOT_ABLE_TO_PROVIDE,
    constants.COMMON.NOT_AVAILABLE,
    constants.COMMON.NOT_APPLICABLE,
    constants.COMMON.NOT_REPORTED,
    constants.COMMON.OTHER,
    constants.COMMON.UNKNOWN,
}


class Disease:
    class_name = "disease"
    api_path = "Condition"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        diagnosis_id = record[CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID]
        affected_status = record.get(CONCEPT.PARTICIPANT.IS_AFFECTED_UNDER_STUDY)
        name = record[CONCEPT.DIAGNOSIS.NAME]
        mondo_id = record.get(CONCEPT.DIAGNOSIS.MONDO_ID)
        icd_id = record.get(CONCEPT.DIAGNOSIS.ICD_ID)
        ncit_id = record.get(CONCEPT.DIAGNOSIS.NCIT_ID)
        tumor_location = record.get(CONCEPT.DIAGNOSIS.TUMOR_LOCATION)
        uberon_id = record.get(CONCEPT.DIAGNOSIS.UBERON_TUMOR_LOCATION_ID)
        event_age_days = record.get(CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [
                    "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/disease"
                ],
                "tag": [{"code": study_id}],
            },
            "identifier": [
                {
                    "use": "official",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/diagnoses/",
                    "value": diagnosis_id,
                }
            ],
            "clinicalStatus": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                        "code": "active",
                        "display": "Active",
                    }
                ],
                "text": "Active",
            },
            "category": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/condition-category",
                            "code": "encounter-diagnosis",
                            "display": "Encounter Diagnosis",
                        }
                    ]
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
        if affected_status:
            verification_status = {"text": affected_status}
            if verification_status_coding.get(affected_status):
                verification_status.setdefault("coding", []).append(
                    verification_status_coding[affected_status]
                )
            entity["verificationStatus"] = verification_status

        # code
        code = {"text": name}
        if mondo_id and mondo_id not in missing_data_values:
            code.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/mondo.owl",
                    "code": mondo_id,
                }
            )
        if icd_id and icd_id not in missing_data_values:
            code.setdefault("coding", []).append(
                {
                    "system": "https://www.who.int/classifications/classification-of-diseases",
                    "code": icd_id,
                }
            )
        if ncit_id and ncit_id not in missing_data_values:
            code.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/ncit.owl",
                    "code": ncit_id,
                }
            )
        entity["code"] = code

        # bodySite
        body_site = {}
        if tumor_location:
            body_site["text"] = tumor_location
        if uberon_id and uberon_id not in missing_data_values:
            body_site.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/uberon.owl",
                    "code": uberon_id,
                }
            )
        if body_site:
            entity.setdefault("bodySite", []).append(body_site)

        # recordedDate
        try:
            entity["_recordedDate"] = {
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

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
