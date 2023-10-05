"""
Builds FHIR Group resources (https://www.hl7.org/fhir/group.html) from rows
of tabular data.
"""
from abc import abstractmethod

import pandas as pd

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import Patient
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

type_code = {
    constants.SPECIES.DOG: "animal",
    constants.SPECIES.FLY: "animal",
    constants.SPECIES.HUMAN: "person",
    constants.SPECIES.MOUSE: "animal",
}


class Family:
    class_name = "family"
    api_path = "Group"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def transform_records_list(cls, records_list):
        return [
            {
                CONCEPT.STUDY.TARGET_SERVICE_ID: study_id,
                CONCEPT.FAMILY.TARGET_SERVICE_ID: family_id,
                CONCEPT.PARTICIPANT.SPECIES: group.get(
                    CONCEPT.PARTICIPANT.SPECIES
                ).unique()[0],
                CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: group.get(
                    CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
                ).unique(),
            }
            for (study_id, family_id), group in pd.DataFrame(records_list).groupby(
                [
                    CONCEPT.STUDY.TARGET_SERVICE_ID,
                    CONCEPT.FAMILY.TARGET_SERVICE_ID,
                ]
            )
        ]

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {
            "_tag": record[CONCEPT.STUDY.TARGET_SERVICE_ID],
            "identifier": not_none(record[CONCEPT.FAMILY.TARGET_SERVICE_ID]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        family_id = record[CONCEPT.FAMILY.TARGET_SERVICE_ID]
        external_id = record.get(CONCEPT.FAMILY.ID)
        species = record.get(CONCEPT.PARTICIPANT.SPECIES)

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
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/families/",
                    "value": family_id,
                }
            ],
            "type": type_code.get(species) or "person",
            "actual": True,
            "code": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                        "code": "FAMMEMB",
                        "display": "family member",
                    },
                ]
            },
        }

        # identifier
        if external_id:
            entity["identifier"].append(
                {
                    "use": "secondary",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/families?external_id=",
                    "value": external_id,
                }
            )

        # member
        member = []
        for participant_id in record.get(CONCEPT.PARTICIPANT.TARGET_SERVICE_ID, []):
            try:
                patient_id = not_none(
                    get_target_id_from_record(
                        Patient,
                        {
                            CONCEPT.STUDY.TARGET_SERVICE_ID: study_id,
                            CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: participant_id,
                        },
                    )
                )
                member.append(
                    {
                        "entity": {"reference": f"{Patient.api_path}/{patient_id}"},
                        "inactive": False,
                    }
                )
            except:
                pass
        if member:
            entity["quantity"] = len(member)
            entity["member"] = member

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
