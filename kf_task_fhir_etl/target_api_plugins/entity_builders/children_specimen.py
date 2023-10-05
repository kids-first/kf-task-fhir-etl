"""
Builds FHIR Specimen resources (https://www.hl7.org/fhir/specimen.html)
from rows of tabular participant biospecimen data (children).
"""
from abc import abstractmethod

import pandas as pd

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import (
    Patient,
    ParentalSpecimen,
)
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/specimen-status
status_code = "unavailable"

type_coding = {
    constants.SEQUENCING.ANALYTE.DNA: [
        {
            "system": "http://purl.obolibrary.org/obo/obi.owl",
            "code": "OBI:0001051",
            "display": "DNA extract",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "DNA",
            "display": "DNA",
        },
    ],
    constants.SEQUENCING.ANALYTE.RNA: [
        {
            "system": "http://purl.obolibrary.org/obo/obi.owl",
            "code": "OBI:0000880",
            "display": "RNA extract",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "RNA",
            "display": "RNA",
        },
    ],
}


class ChildrenSpecimen:
    class_name = "children_specimen"
    api_path = "Specimen"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        assert not_none(get_target_id_from_record(ParentalSpecimen, record))
        biospecimen_id = not_none(record[CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID])

        return {
            "_tag": record[CONCEPT.STUDY.TARGET_SERVICE_ID],
            "identifier:exact": f"{biospecimen_id}",
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        biospecimen_id = record[CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID]
        consent_type = record.get(CONCEPT.BIOSPECIMEN.CONSENT_SHORT_NAME)
        dbgap_consent_code = record.get(CONCEPT.BIOSPECIMEN.DBGAP_STYLE_CONSENT_CODE)
        external_aliquot_id = record.get(CONCEPT.BIOSPECIMEN.ID)
        analyte_type = record[CONCEPT.BIOSPECIMEN.ANALYTE]
        event_age_days = record.get(CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS)
        volume_ul = record.get(CONCEPT.BIOSPECIMEN.VOLUME_UL)

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
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/biospecimens/",
                    "use": "official",
                    "value": biospecimen_id,
                },
            ],
            "status": status_code,
            "subject": {
                "reference": "/".join(
                    [
                        Patient.api_path,
                        not_none(get_target_id_from_record(Patient, record)),
                    ]
                )
            },
            "parent": [
                {
                    "reference": "/".join(
                        [
                            ParentalSpecimen.api_path,
                            not_none(
                                get_target_id_from_record(ParentalSpecimen, record)
                            ),
                        ]
                    )
                }
            ],
        }

        # meta.security
        if consent_type:
            entity["meta"].setdefault("security", []).append(
                {
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/biospecimens?consent_type=",
                    "code": consent_type,
                }
            )
        if dbgap_consent_code:
            entity["meta"].setdefault("security", []).append(
                {
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/biospecimens?dbgap_consent_code=",
                    "code": dbgap_consent_code,
                }
            )

        # identifier
        if external_aliquot_id:
            entity["identifier"].append(
                {
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/biospecimens?external_aliquot_id=",
                    "use": "secondary",
                    "value": external_aliquot_id,
                }
            )

        # type
        specimen_type = {"text": analyte_type}
        if type_coding.get(analyte_type):
            specimen_type["coding"] = type_coding[analyte_type]
        entity["type"] = specimen_type

        # collection
        collection = {}

        # collection.collectedDateTime
        try:
            collection["_collectedDateTime"] = {
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

        # collection.quantity
        try:
            collection["quantity"] = {
                "value": float(volume_ul),
                "unit": "microliter",
                "system": "http://unitsofmeasure.org",
                "code": "uL",
            }
        except:
            pass

        if collection:
            entity["collection"] = collection

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
