"""
Builds FHIR Specimen resources (https://www.hl7.org/fhir/specimen.html)
from rows of tabular participant biospecimen data (parental).
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import Patient
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/specimen-status
status_code = "unavailable"

type_coding = {
    "Amniocytes": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C118138",
            "display": "Reactive Amniocyte",
        }
    ],
    "amniotic fluid": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C13188",
            "display": " Amniotic Fluid",
        }
    ],
    "blood": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C17610",
            "display": "Blood Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Peripheral-Whole-Blood",
            "display": "Peripheral Whole Blood",
        },
    ],
    constants.SPECIMEN.COMPOSITION.BLOOD: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C17610",
            "display": "Blood Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Peripheral-Whole-Blood",
            "display": "Peripheral Whole Blood",
        },
    ],
    "Blood": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C17610",
            "display": "Blood Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Peripheral-Whole-Blood",
            "display": "Peripheral Whole Blood",
        },
    ],
    "Blood Derived Cancer - Bone Marrow, Post-treatment": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C164009",
            "display": "Bone Marrow Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Bone-Marrow",
            "display": "Bone Marrow",
        },
    ],
    "Blood Derived Cancer - Peripheral Blood, Post-treatment": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C17610",
            "display": "Blood Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Peripheral-Whole-Blood",
            "display": "Peripheral Whole Blood",
        },
    ],
    "Blood EDTA": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C158462",
            "display": "EDTA Blood Cell Fraction",
        }
    ],
    "Blood-Lymphocyte": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12535",
            "display": "Lymphocyte",
        }
    ],
    "bone": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12366",
            "display": "Bone",
        }
    ],
    constants.SPECIMEN.COMPOSITION.BONE: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12366",
            "display": "Bone",
        }
    ],
    "Bone marrow": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C164009",
            "display": "Bone Marrow Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Bone-Marrow",
            "display": "Bone Marrow",
        },
    ],
    constants.SPECIMEN.COMPOSITION.BONE_MARROW: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C164009",
            "display": "Bone Marrow Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Bone-Marrow",
            "display": "Bone Marrow",
        },
    ],
    "brain": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12439",
            "display": "Brain",
        }
    ],
    "Brain Tissue": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12439",
            "display": "Brain",
        }
    ],
    "Buccal": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C172264",
            "display": "Buccal Cell Sample",
        }
    ],
    "Buccal Cell Normal": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C172264",
            "display": "Buccal Cell Sample",
        }
    ],
    constants.SPECIMEN.COMPOSITION.BUCCAL_SWAB: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C113747",
            "display": "Buccal Swab",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Buccal-Cells",
            "display": "Buccal Cells",
        },
    ],
    "Buccal Mucosa": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12505",
            "display": "Buccal Mucosa",
        }
    ],
    "Buffy Coat": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C84507",
            "display": "Buffy Coat",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Buffy-Coat",
            "display": "Buffy Coat",
        },
    ],
    "Cartilage": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12373",
            "display": "Cartilage",
        }
    ],
    "Cell Freeze": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12508",
            "display": "Cell",
        }
    ],
    "Cells": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12508",
            "display": "Cell",
        }
    ],
    "Cerebral Spinal Fluid": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C185194",
            "display": "Cerebrospinal Fluid Sample",
        }
    ],
    "Cheek Swab": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C113747",
            "display": "Buccal Swab",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Buccal-Cells",
            "display": "Buccal Cells",
        },
    ],
    "chest wall": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C62484",
            "display": "Chest Wall",
        }
    ],
    "Cyst Fluid": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C2978",
            "display": "Cyst",
        }
    ],
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
    "dura": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C32488",
            "display": "Dura Mater",
        }
    ],
    constants.SPECIMEN.COMPOSITION.EBVI: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C73940",
            "display": "EBV-Positive Neoplastic Cells Present",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Epstein-Bar-Virus-Immortalized-Cells",
            "display": "Epstein Bar Virus Immortalized Cells",
        },
    ],
    "Epstein-Barr Virus Immortalized Cells": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C163993",
            "display": "EBV Immortalized Lymphocytes",
        }
    ],
    "Fetal Tissue Liver": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C34169",
            "display": "Fetal Liver",
        }
    ],
    "Fetal Tissue Unspecified": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C17730",
            "display": "Fetal Tissue",
        }
    ],
    "Fibroblast": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12482",
            "display": "Fibroblast",
        }
    ],
    constants.SPECIMEN.COMPOSITION.FIBROBLASTS: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12482",
            "display": "Fibroblast",
        }
    ],
    "Fibroblasts from Bone Marrow Normal": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12482",
            "display": "Fibroblast",
        }
    ],
    "groin": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12726",
            "display": "Inguinal Region",
        }
    ],
    "Hair": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C32705",
            "display": "Hair",
        }
    ],
    constants.SPECIMEN.COMPOSITION.LINE: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C156445",
            "display": "Derived Cell Line",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Derived-Cell-Line",
            "display": "Derived Cell Line",
        },
    ],
    "LCL": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C32941",
            "display": "Lateral Ligament",
        }
    ],
    "Leukocyte": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12529",
            "display": "Leukocyte",
        }
    ],
    "lung": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C33024",
            "display": "Lung Tissue",
        }
    ],
    "lymph node": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12745",
            "display": "Lymph Node",
        }
    ],
    constants.SPECIMEN.COMPOSITION.LYMPHOCYTES: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12535",
        "display": "Lymphocyte",
    },
    "marrow": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C164009",
            "display": "Bone Marrow Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Bone-Marrow",
            "display": "Bone Marrow",
        },
    ],
    "mediastinum": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12748",
            "display": "Mediastinum",
        }
    ],
    constants.SPECIMEN.COMPOSITION.MNC: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C178965",
            "display": "Peripheral Blood Mononuclear Cell Sample",
        }
    ],
    "muscle": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12435",
            "display": "Muscle Tissue",
        }
    ],
    "Muscle": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12435",
            "display": "Muscle Tissue",
        }
    ],
    "Myocyte": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C12612",
            "display": "Muscle Cell",
        }
    ],
    "Negative Lymph Node": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C36174",
            "display": "Negative Lymph Node",
        }
    ],
    "Patient Derived Xenograft": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C122936",
            "display": "Patient Derived Xenograft",
        }
    ],
    "PBMC": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C178965",
            "display": "Peripheral Blood Mononuclear Cell Sample",
        }
    ],
    "Peripheral blood": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C17610",
            "display": "Blood Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Peripheral-Whole-Blood",
            "display": "Peripheral Whole Blood",
        },
    ],
    "Plasma": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C185204",
            "display": "Plasma Sample",
        }
    ],
    "Primary Blood Derived Cancer - Bone Marrow": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C164009",
            "display": "Bone Marrow Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Bone-Marrow",
            "display": "Bone Marrow",
        },
    ],
    "Primary Blood Derived Cancer - Peripheral Blood": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C17610",
            "display": "Blood Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Peripheral-Whole-Blood",
            "display": "Peripheral Whole Blood",
        },
    ],
    "Recurrent Blood Derived Cancer - Peripheral Blood": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C17610",
            "display": "Blood Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Peripheral-Whole-Blood",
            "display": "Peripheral Whole Blood",
        },
    ],
    constants.SPECIMEN.COMPOSITION.SALIVA: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C174119",
            "display": "Saliva Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Saliva",
            "display": "Saliva",
        },
    ],
    "saliva": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C174119",
            "display": "Saliva Sample",
        },
        {
            "system": "https://includedcc.org/fhir/code-systems/sample_types",
            "code": "Saliva",
            "display": "Saliva",
        },
    ],
    "Serum": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C178987",
            "display": "Serum Sample",
        }
    ],
    "skin": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C33563",
            "display": "Skin Tissue",
        }
    ],
    constants.SPECIMEN.COMPOSITION.TISSUE: [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C164014",
            "display": "Solid Tissue Specimen",
        }
    ],
    "Tissue Cell Culture": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C17201",
            "display": "Tissue Culture",
        }
    ],
    "Tissue FFPE": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C172265",
            "display": "Formalin-Fixed Paraffin-Embedded Tissue Sample",
        }
    ],
    "Tissue Flash Frozen": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C158417",
            "display": "Frozen Tissue",
        }
    ],
    "Tissue Freezing Media": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C158417",
            "display": "Frozen Tissue",
        }
    ],
    "Tissue Perineum": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C33301",
            "display": "Perineum",
        }
    ],
    "Tumor": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C18009",
            "display": "Tumor Tissue",
        }
    ],
    "Vascular tissue": [
        {
            "system": "http://purl.obolibrary.org/obo/ncit.owl",
            "code": "C33853",
            "display": "Vascular Smooth Muscle Tissue",
        }
    ],
}


class ParentalSpecimen:
    class_name = "parental_specimen"
    api_path = "Specimen"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        assert not_none(get_target_id_from_record(Patient, record))
        biospecimen_id = not_none(record[CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID])
        composition = not_none(record[CONCEPT.BIOSPECIMEN.COMPOSITION])

        return {
            "_tag": record[CONCEPT.STUDY.TARGET_SERVICE_ID],
            "identifier:exact": f"{biospecimen_id}_{composition.replace(' ', '_')}",
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
        external_sample_id = record.get(CONCEPT.BIOSPECIMEN_GROUP.ID)
        # tissue_type = record.get(CONCEPT.BIOSPECIMEN.TISSUE_TYPE)
        composition = record[CONCEPT.BIOSPECIMEN.COMPOSITION]
        # ncit_id_tissue_type = record.get(CONCEPT.BIOSPECIMEN.NCIT_TISSUE_TYPE_ID)
        event_age_days = record.get(CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS)
        volume_ul = record.get(CONCEPT.BIOSPECIMEN.VOLUME_UL)
        sample_procurement = record.get(CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT)
        anatomy_site = record.get(CONCEPT.BIOSPECIMEN.ANATOMY_SITE)
        uberon_anatomy_site_id = record.get(CONCEPT.BIOSPECIMEN.UBERON_ANATOMY_SITE_ID)
        ncit_anatomy_site_id = record.get(CONCEPT.BIOSPECIMEN.NCIT_ANATOMY_SITE_ID)

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
                    "value": f"{biospecimen_id}_{composition.replace(' ', '_')}",
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
        if external_sample_id:
            entity["identifier"].append(
                {
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/biospecimens?external_sample_id=",
                    "use": "secondary",
                    "value": external_sample_id,
                }
            )

        # type
        specimen_type = {"text": composition}
        if type_coding.get(composition):
            specimen_type["coding"] = type_coding[composition]
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

        # method
        method = {}
        if sample_procurement:
            method["text"] = sample_procurement
            # if collection_method_coding.get(sample_procurement):
            #     method.setdefault("coding", []).append(
            #         collection_method_coding[sample_procurement]
            #     )
        if method:
            collection["method"] = method

        # bodySite
        body_site = {}
        if anatomy_site:
            body_site["text"] = anatomy_site
        if uberon_anatomy_site_id:
            body_site_coding = {"code": uberon_anatomy_site_id}
            if uberon_anatomy_site_id.startswith("UBERON:"):
                body_site_coding["system"] = "http://purl.obolibrary.org/obo/uberon.owl"
            elif uberon_anatomy_site_id.startswith("EFO:"):
                body_site_coding["system"] = "http://www.ebi.ac.uk/efo/efo.owl"
            body_site.setdefault("coding", []).append(body_site_coding)
        if ncit_anatomy_site_id and ncit_anatomy_site_id.startswith("NCIT:"):
            body_site.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/ncit.owl",
                    "code": ncit_anatomy_site_id,
                }
            )
        if body_site:
            collection["bodySite"] = body_site

        if collection:
            entity["collection"] = collection

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
