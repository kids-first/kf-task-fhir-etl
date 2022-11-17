"""
Builds FHIR Specimen resources (https://www.hl7.org/fhir/specimen.html)
from rows of tabular participant biospecimen data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import Patient
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/specimen-status
status_code = "unavailable"

# http://snomed.info/sct
composition_dict = {
    "Amniocytes": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C118138",
        "display": "Reactive Amniocyte",
    },
    "amniotic fluid": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C13188",
        "display": " Amniotic Fluid",
    },
    "blood": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C17610",
        "display": "Blood Sample",
    },
    "Blood": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C17610",
        "display": "Blood Sample",
    },
    "Blood Derived Cancer - Bone Marrow, Post-treatment": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C164009",
        "display": "Bone Marrow Sample",
    },
    "Blood Derived Cancer - Peripheral Blood, Post-treatment": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C173496",
        "display": "Peripheral Blood",
    },
    "Blood EDTA": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C158462",
        "display": "EDTA Blood Cell Fraction",
    },
    "Blood-Lymphocyte": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12535",
        "display": "Lymphocyte",
    },
    "bone": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12366",
        "display": "Bone",
    },
    constants.SPECIMEN.COMPOSITION.BONE: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12366",
        "display": "Bone",
    },
    "Bone marrow": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C164009",
        "display": "Bone Marrow Sample",
    },
    constants.SPECIMEN.COMPOSITION.BONE_MARROW: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C164009",
        "display": "Bone Marrow Sample",
    },
    "brain": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12439",
        "display": "Brain",
    },
    "Brain Tissue": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12439",
        "display": "Brain",
    },
    "Buccal": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C172264",
        "display": "Buccal Cell Sample",
    },
    "Buccal Cell Normal": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C172264",
        "display": "Buccal Cell Sample",
    },
    constants.SPECIMEN.COMPOSITION.BUCCAL_SWAB: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C113747",
        "display": "Buccal Swab",
    },
    "Buccal Mucosa": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12505",
        "display": "Buccal Mucosa",
    },
    "Buffy Coat": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C84507",
        "display": "Buffy Coat",
    },
    "Cartilage": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12373",
        "display": "Cartilage",
    },
    "Cell Freeze": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12508",
        "display": "Cell",
    },
    "Cells": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12508",
        "display": "Cell",
    },
    "Cerebral Spinal Fluid": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C185194",
        "display": "Cerebrospinal Fluid Sample",
    },
    "Cheek Swab": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C113747",
        "display": "Buccal Swab",
    },
    "chest wall": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C62484",
        "display": "Chest Wall",
    },
    "Cyst Fluid": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C2978",
        "display": "Cyst",
    },
    constants.SEQUENCING.ANALYTE.DNA: {
        "system": "http://purl.obolibrary.org/obo/obi.owl",
        "code": "OBI:0001051",
        "display": "DNA extract",
    },
    "dura": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C32488",
        "display": "Dura Mater",
    },
    "Epstein-Barr Virus Immortalized Cells": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C163993",
        "display": "EBV Immortalized Lymphocytes",
    },
    "Fetal Tissue Liver": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C34169",
        "display": "Fetal Liver",
    },
    "Fetal Tissue Unspecified": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C17730",
        "display": "Fetal Tissue",
    },
    "Fibroblast": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12482",
        "display": "Fibroblast",
    },
    constants.SPECIMEN.COMPOSITION.FIBROBLASTS: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12482",
        "display": "Fibroblast",
    },
    "Fibroblasts from Bone Marrow Normal": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12482",
        "display": "Fibroblast",
    },
    "groin": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12726",
        "display": "Inguinal Region",
    },
    "Hair": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C32705",
        "display": "Hair",
    },
    constants.SPECIMEN.COMPOSITION.LINE: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C156445",
        "display": "Derived Cell Line",
    },
    "LCL": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C32941",
        "display": "Lateral Ligament",
    },
    "Leukocyte": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12529",
        "display": "Leukocyte",
    },
    "lung": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C33024",
        "display": "Lung Tissue",
    },
    "lymph node": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12745",
        "display": "Lymph Node",
    },
    constants.SPECIMEN.COMPOSITION.LYMPHOCYTES: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12535",
        "display": "Lymphocyte",
    },
    "marrow": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C164009",
        "display": "Bone Marrow Sample",
    },
    "mediastinum": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12748",
        "display": "Mediastinum",
    },
    constants.SPECIMEN.COMPOSITION.MNC: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C178965",
        "display": "Peripheral Blood Mononuclear Cell Sample",
    },
    "muscle": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12435",
        "display": "Muscle Tissue",
    },
    "Muscle": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12435",
        "display": "Muscle Tissue",
    },
    "Myocyte": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C12612",
        "display": "Muscle Cell",
    },
    "Negative Lymph Node": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C36174",
        "display": "Negative Lymph Node",
    },
    "Patient Derived Xenograft": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C122936",
        "display": "Patient Derived Xenograft",
    },
    "PBMC": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C178965",
        "display": "Peripheral Blood Mononuclear Cell Sample",
    },
    "Peripheral blood": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C173496",
        "display": "Peripheral Blood",
    },
    constants.SPECIMEN.COMPOSITION.BLOOD: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C173496",
        "display": "Peripheral Blood",
    },
    "Plasma": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C185204",
        "display": "Plasma Sample",
    },
    "Primary Blood Derived Cancer - Bone Marrow": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C164009",
        "display": "Bone Marrow Sample",
    },
    "Primary Blood Derived Cancer - Peripheral Blood": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C173496",
        "display": "Peripheral Blood",
    },
    "Recurrent Blood Derived Cancer - Peripheral Blood": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C173496",
        "display": "Peripheral Blood",
    },
    "saliva": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C174119",
        "display": "Saliva Sample",
    },
    constants.SPECIMEN.COMPOSITION.SALIVA: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C174119",
        "display": "Saliva Sample",
    },
    "Serum": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C178987",
        "display": "Serum Sample",
    },
    "skin": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C33563",
        "display": "Skin Tissue",
    },
    constants.SPECIMEN.COMPOSITION.TISSUE: {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C164014",
        "display": "Solid Tissue Specimen",
    },
    "Tissue Cell Culture": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C17201",
        "display": "Tissue Culture",
    },
    "Tissue FFPE": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C172265",
        "display": "Formalin-Fixed Paraffin-Embedded Tissue Sample",
    },
    "Tissue Flash Frozen": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C158417",
        "display": "Frozen Tissue",
    },
    "Tissue Freezing Media": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C158417",
        "display": "Frozen Tissue",
    },
    "Tissue Perineum": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C33301",
        "display": "Perineum",
    },
    "Tumor": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C18009",
        "display": "Tumor Tissue",
    },
    "Vascular tissue": {
        "system": "http://purl.obolibrary.org/obo/ncit.owl",
        "code": "C33853",
        "display": "Vascular Smooth Muscle Tissue",
    },
}

# http://snomed.info/sct
analyte_type_dict = {
    constants.SEQUENCING.ANALYTE.DNA: {
        "system": "http://purl.obolibrary.org/obo/obi.owl",
        "code": "OBI:0001051",
        "display": "DNA extract",
    },
    constants.SEQUENCING.ANALYTE.RNA: {
        "system": "http://purl.obolibrary.org/obo/obi.owl",
        "code": "OBI:0000880",
        "display": "RNA extract",
    },
}

# http://hl7.org/fhir/ValueSet/specimen-collection-method
collection_method_coding = {
    constants.SPECIMEN.SAMPLE_PROCUREMENT.AUTOPSY: {
        "system": "http://snomed.info/sct",
        "code": "29240004",
        "display": "Autopsy examination (procedure)",
    },
    constants.SPECIMEN.SAMPLE_PROCUREMENT.BIOPSY: {
        "system": "http://snomed.info/sct",
        "code": "86273004",
        "display": "Biopsy (procedure)",
    },
    "Blood Collection - Maternal": {
        "system": "http://snomed.info/sct",
        "code": "396540005",
        "display": "Phlebotomy (procedure)",
    },
    "Blood Collection - Paternal": {
        "system": "http://snomed.info/sct",
        "code": "396540005",
        "display": "Phlebotomy (procedure)",
    },
    "Blood Collection - Proband": {
        "system": "http://snomed.info/sct",
        "code": "396540005",
        "display": "Phlebotomy (procedure)",
    },
    constants.SPECIMEN.SAMPLE_PROCUREMENT.BLOOD_DRAW: {
        "system": "http://snomed.info/sct",
        "code": "396540005",
        "display": "Phlebotomy (procedure)",
    },
    constants.SPECIMEN.SAMPLE_PROCUREMENT.BONE_MARROW_ASPIRATION: {
        "system": "http://snomed.info/sct",
        "code": "49401003",
        "display": "Bone marrow aspiration procedure (procedure)",
    },
    "Saliva Collection - Maternal": {
        "system": "http://snomed.info/sct",
        "code": "225098009",
        "display": "Collection of sample of saliva (procedure)",
    },
    "Saliva Collection - Paternal": {
        "system": "http://snomed.info/sct",
        "code": "225098009",
        "display": "Collection of sample of saliva (procedure)",
    },
    "Saliva Collection - Proband": {
        "system": "http://snomed.info/sct",
        "code": "225098009",
        "display": "Collection of sample of saliva (procedure)",
    },
    "Surgical Resections": {
        "system": "http://snomed.info/sct",
        "code": "65801008",
        "display": "Excision (procedure)",
    },
}


class Specimen:
    class_name = "specimen"
    api_path = "Specimen"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        biospecimen_id = record[CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID]
        external_aliquot_id = record[CONCEPT.BIOSPECIMEN.ID]
        tissue_type = record.get(CONCEPT.BIOSPECIMEN.TISSUE_TYPE)
        composition = record.get(CONCEPT.BIOSPECIMEN.COMPOSITION)
        analyte = record[CONCEPT.BIOSPECIMEN.ANALYTE]
        ncit_id_tissue_type = record.get(CONCEPT.BIOSPECIMEN.NCIT_TISSUE_TYPE_ID)
        # tumor_descriptor = record.get(CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR)
        event_age_days = record.get(CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS)
        volume_ul = record.get(CONCEPT.BIOSPECIMEN.VOLUME_UL)
        sample_procurement = record.get(CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT)
        anatomy_site = record.get(CONCEPT.BIOSPECIMEN.ANATOMY_SITE)
        uberon_anatomy_site_id = record.get(CONCEPT.BIOSPECIMEN.UBERON_ANATOMY_SITE_ID)
        ncit_anatomy_site_id = record.get(CONCEPT.BIOSPECIMEN.NCIT_ANATOMY_SITE_ID)
        # spatial_descriptor = record.get(CONCEPT.BIOSPECIMEN.SPATIAL_DESCRIPTOR)

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
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/biospecimens/",
                    "value": biospecimen_id,
                }
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

        # identifier
        if external_aliquot_id:
            entity["identifier"].append(
                {
                    "use": "secondary",
                    "value": external_aliquot_id,
                }
            )

        # type - tissue_type, ncit_id_tissue_type, composition, and analyte
        specimen_type = {}
        if tissue_type:
            specimen_type = {"text": tissue_type}
        if ncit_id_tissue_type and ncit_id_tissue_type.startswith("NCIT:"):
            specimen_type.setdefault("coding", []).append(
                {
                    "system": "http://purl.obolibrary.org/obo/ncit.owl",
                    "code": ncit_id_tissue_type,
                }
            )
        if composition_dict.get(composition):
            specimen_type.setdefault("coding", []).append(composition_dict[composition])
        if analyte_type_dict.get(analyte):
            specimen_type.setdefault("coding", []).append(analyte_type_dict[analyte])
        if specimen_type:
            entity["type"] = specimen_type

        # collection
        collection = {}

        # collectedDateTime
        try:
            collection["_collectedDateTime"] = {
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

        # quantity
        try:
            collection["quantity"] = {
                "value": float(volume_ul),
                "unit": "microliters",
                "system": "http://unitsofmeasure.org",
                "code": "uL",
            }
        except:
            pass

        # method
        method = {}
        if sample_procurement:
            method["text"] = sample_procurement
            if collection_method_coding.get(sample_procurement):
                method.setdefault("coding", []).append(
                    collection_method_coding[sample_procurement]
                )
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
