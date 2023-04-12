"""
Builds FHIR Patient resources (https://www.hl7.org/fhir/patient.html) from rows
of tabular demographics data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# https://hl7.org/fhir/us/core/ValueSet-omb-race-category.html
omb_race_category = {
    constants.RACE.NATIVE_AMERICAN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "1002-5",
            "display": "American Indian or Alaska Native",
        },
    },
    constants.RACE.ASIAN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2028-9",
            "display": "Asian",
        },
    },
    constants.RACE.BLACK: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2054-5",
            "display": "Black or African American",
        },
    },
    constants.RACE.PACIFIC: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2076-8",
            "display": "Native Hawaiian or Other Pacific Islander",
        },
    },
    constants.RACE.WHITE: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2106-3",
            "display": "White",
        },
    },
    constants.COMMON.OTHER: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "OTH",
            "display": "other",
        },
    },
    constants.COMMON.NOT_AVAILABLE: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "NAVU",
            "display": "not available",
        },
    },
    constants.COMMON.UNKNOWN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "UNK",
            "display": "unknown",
        },
    },
}

# https://hl7.org/fhir/us/core/ValueSet-omb-ethnicity-category.html
omb_ethnicity_category = {
    constants.ETHNICITY.HISPANIC: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2135-2",
            "display": "Hispanic or Latino",
        },
    },
    constants.ETHNICITY.NON_HISPANIC: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "urn:oid:2.16.840.1.113883.6.238",
            "code": "2186-5",
            "display": "Not Hispanic or Latino",
        },
    },
    constants.COMMON.UNKNOWN: {
        "url": "ombCategory",
        "valueCoding": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
            "code": "UNK",
            "display": "unknown",
        },
    },
}

# http://hl7.org/fhir/R4/codesystem-administrative-gender.html
administrative_gender_code = {
    constants.GENDER.MALE: "male",
    constants.GENDER.FEMALE: "female",
    # constants.COMMON.NOT_AVAILABLE: "unknown",
    # constants.COMMON.NOT_REPORTED: "unknown",
    constants.COMMON.UNKNOWN: "unknown",
    constants.COMMON.OTHER: "other",
}


class Patient:
    class_name = "patient"
    api_path = "Patient"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        participant_id = record[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
        external_id = record.get(CONCEPT.PARTICIPANT.ID)
        race = record.get(CONCEPT.PARTICIPANT.RACE)
        ethnicity = record.get(CONCEPT.PARTICIPANT.ETHNICITY)
        gender = record.get(CONCEPT.PARTICIPANT.GENDER)

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [f"http://hl7.org/fhir/StructureDefinition/{cls.api_path}"],
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
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/participants/",
                    "value": participant_id,
                }
            ],
        }

        # identifier
        if external_id:
            entity["identifier"].append(
                {
                    "use": "secondary",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/participants?external_id=",
                    "value": external_id,
                }
            )

        # US Core Race
        us_core_race = {}
        if race:
            us_core_race.update(
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                    "extension": [{"url": "text", "valueString": race}],
                }
            )
            if omb_race_category.get(race):
                us_core_race["extension"].append(omb_race_category[race])
        if us_core_race:
            entity.setdefault("extension", []).append(us_core_race)

        # US Core Ethnicity
        us_core_ethnicity = {}
        if ethnicity:
            us_core_ethnicity.update(
                {
                    "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                    "extension": [{"url": "text", "valueString": ethnicity}],
                }
            )
            if omb_ethnicity_category.get(ethnicity):
                us_core_ethnicity["extension"].append(omb_ethnicity_category[ethnicity])
        if us_core_ethnicity:
            entity.setdefault("extension", []).append(us_core_ethnicity)

        # Gender
        if administrative_gender_code.get(gender):
            entity["gender"] = administrative_gender_code[gender]

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
