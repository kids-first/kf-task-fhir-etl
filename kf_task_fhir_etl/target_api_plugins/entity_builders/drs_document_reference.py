"""
Builds FHIR DocumentReference resources (http://hl7.org/fhir/R4/documentreference.html) 
from rows of tabular genomic file data.
"""
import os
from abc import abstractmethod

import pandas as pd

from requests import RequestException

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import Patient, Specimen
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids
from d3b_utils.requests_retry import Session

KF_API_DATASERVICE_URL = (
    os.getenv("KF_API_DATASERVICE_URL")
    or "https://kf-api-dataservice.kidsfirstdrc.org/"
)
DRS_HOSTNAME = "drs://data.kidsfirstdrc.org/"

# http://hl7.org/fhir/ValueSet/document-reference-status
status_code = "current"

# http://hl7.org/fhir/ValueSet/composition-status
doc_status_code = "final"

# https://includedcc.org/fhir/code-systems/data_types
type_coding = {
    constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads",
        "display": "Aligned Reads",
    },
    constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads-Index",
        "display": "Aligned Reads Index",
    },
    "Annotated Gene Fusion": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Annotated-Gene-Fusion",
        "display": "Annotated Gene Fusion",
    },
    "Gene Expression Quantification": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Gene-Expression-Quantifications",
        "display": "Gene Expression Quantifications",
    },
    constants.GENOMIC_FILE.DATA_TYPE.GENE_FUSIONS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Gene-Fusions",
        "display": "Gene Fusions",
    },
    "Isoform Expression Quantifications": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Isoform-Expression-Quantifications",
        "display": "Isoform Expression Quantifications",
    },
    constants.GENOMIC_FILE.DATA_TYPE.GVCF: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF",
        "display": "gVCF",
    },
    constants.GENOMIC_FILE.DATA_TYPE.GVCF_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF-Index",
        "display": "gVCF Index",
    },
    "Raw Gene Fusion": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Raw-Gene-Fusions",
        "display": "Raw Gene Fusions",
    },
    "Simple Nucleotide Variations": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations",
        "display": "Simple Nucleotide Variations",
    },
    "Somatic Copy Number Variation": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Copy-Number-Variations",
        "display": "Somatic Copy Number Variations",
    },
    constants.GENOMIC_FILE.DATA_TYPE.SOMATIC_COPY_NUMBER_VARIATIONS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Copy-Number-Variations",
        "display": "Somatic Copy Number Variations",
    },
    constants.GENOMIC_FILE.DATA_TYPE.SOMATIC_STRUCTURAL_VARIATIONS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations",
        "display": "Somatic Structural Variations",
    },
    constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Variant-Calls",
        "display": "Variant Calls",
    },
    constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Variant-Calls-Index",
        "display": "Variant Calls Index",
    },
}

# https://includedcc.org/fhir/code-systems/experimental_strategies
experimental_strategy_coding = {
    constants.SEQUENCING.STRATEGY.RNA: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "RNA-Seq",
        "display": "RNA-Seq",
    },
    constants.SEQUENCING.STRATEGY.WGS: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "WGS",
        "display": "Whole Genome Sequencing",
    },
}

# https://includedcc.org/fhir/code-systems/data_categories
data_cateogry_coding = {
    constants.SEQUENCING.STRATEGY.RNA: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    constants.SEQUENCING.STRATEGY.WGS: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
}

# https://includedcc.org/fhir/code-systems/data_access_types
data_access_coding = {
    True: {
        "system": "https://includedcc.org/fhir/code-systems/data_access_types",
        "code": "controlled",
        "display": "Controlled",
    },
    False: {
        "system": "https://includedcc.org/fhir/code-systems/data_access_types",
        "code": "registered",
        "display": "Registered",
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


class DRSDocumentReference:
    class_name = "drs_document_reference"
    api_path = "DocumentReference"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def transform_records_list(cls, records_list):
        records = pd.DataFrame(records_list)
        by = [
            CONCEPT.STUDY.TARGET_SERVICE_ID,
            CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
        ]
        if records.get(CONCEPT.SEQUENCING.TARGET_SERVICE_ID) is not None:
            by.append(CONCEPT.SEQUENCING.TARGET_SERVICE_ID)

        transfromed_records_list = []
        for names, group in records.groupby(by=by):
            transfromed_record = {
                CONCEPT.STUDY.TARGET_SERVICE_ID: names[0],
                CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID: names[1],
                CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: group.get(
                    CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
                ).unique(),
                CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID: group.get(
                    CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID
                ).unique(),
            }

            try:
                transfromed_record.update(
                    {
                        CONCEPT.SEQUENCING.TARGET_SERVICE_ID: names[2],
                        CONCEPT.SEQUENCING.STRATEGY: group.get(
                            CONCEPT.SEQUENCING.STRATEGY
                        ).unique()[0],
                    }
                )
            except:
                pass

            transfromed_records_list.append(transfromed_record)

        return transfromed_records_list

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        return {"identifier": not_none(record[CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID])}

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        genomic_file_id = record[CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID]
        strategy = record.get(CONCEPT.SEQUENCING.STRATEGY)
        participant_id_list = record[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
        biospecimen_id_list = record[CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID]

        # GET Indexd metadata
        base_url = KF_API_DATASERVICE_URL.rstrip("/")
        url = f"{base_url}/genomic-files/{genomic_file_id}"
        resp = Session().get(url, headers={"Content-Type": "application/json"})
        try:
            resp.raise_for_status()
        except:
            raise RequestException(f"{resp.text}")

        genomic_file = resp.json().get("results")

        controlled_access = genomic_file.get("controlled_access")
        data_type = genomic_file.get("data_type")
        latest_did = genomic_file.get("latest_did")
        file_format = genomic_file.get("file_format")
        acl_list = genomic_file.get("acl")
        # url_list = genomic_file.get("urls")
        size = genomic_file.get("size")
        hash_dict = genomic_file.get("hashes")
        file_name = genomic_file.get("file_name")

        # TEMPORARY: Impute date_type
        if (
            data_type
            in {
                "Simple Nucleotide Variations",
                constants.GENOMIC_FILE.DATA_TYPE.SOMATIC_STRUCTURAL_VARIATIONS,
            }
            and file_format == "tbi"
        ):
            data_type = f"{data_type} Index"

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [
                    "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/drs-document-reference"
                ],
                "tag": [{"code": study_id}],
            },
            "identifier": [
                {
                    "use": "official",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/genomic-files/",
                    "value": genomic_file_id,
                }
            ],
            "status": status_code,
            "docStatus": doc_status_code,
        }

        # type
        if data_type:
            doc_type = {"text": data_type}
            if type_coding.get(data_type):
                doc_type.setdefault("coding", []).append(type_coding[data_type])
            entity["type"] = doc_type

        # category
        category = []
        if strategy:
            # Experimental strategy
            experimental_strategy = {"text": strategy}
            if experimental_strategy_coding.get(strategy):
                experimental_strategy.setdefault("coding", []).append(
                    experimental_strategy_coding[strategy]
                )
            category.append(experimental_strategy)

            # Data Category
            data_cateogry = {"text": strategy}
            if data_cateogry_coding.get(strategy):
                data_cateogry.setdefault("coding", []).append(
                    data_cateogry_coding[strategy]
                )
            category.append(data_cateogry)
        if category:
            entity["category"] = category

        # subject
        # TODO: handle multi-Patient DocumentReference resources
        if len(participant_id_list) == 1:
            subject_id = not_none(
                get_target_id_from_record(
                    Patient,
                    {CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: participant_id_list[0]},
                )
            )
            entity["subject"] = {"reference": f"{Patient.api_path}/{subject_id}"}

        # securityLabel
        security_label_list = []
        security_label = {"text": controlled_access}
        if data_access_coding.get(controlled_access):
            security_label.setdefault("coding", []).append(
                data_access_coding[controlled_access]
            )
        security_label_list.append(security_label)
        if acl_list:
            for acl in acl_list:
                security_label = {"text": acl}
                if len(acl.split(".")) > 1:
                    security_label.setdefault("coding", []).append(
                        {"code": acl.split(".")[1]}
                    )
                security_label_list.append(security_label)
        if security_label_list:
            entity["securityLabel"] = security_label_list

        # content
        content_list = []

        # DRS content
        content = {}

        # format
        if file_format and file_format not in missing_data_values:
            content["format"] = {"display": file_format}

        # attachment
        attachment = {}

        # size
        try:
            attachment.setdefault("extension", []).append(
                {
                    "url": "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/file-size",
                    "valueDecimal": int(size),
                }
            )
        except:
            pass

        # hash
        if hash_dict:
            for algorithm, hash_value in hash_dict.items():
                attachment.setdefault("extension", []).append(
                    {
                        "url": "https://nih-ncpi.github.io/ncpi-fhir-ig/StructureDefinition/hashes",
                        "valueCodeableConcept": {
                            "coding": [{"display": algorithm}],
                            "text": hash_value,
                        },
                    }
                )

        # url
        if latest_did:
            attachment["url"] = f"{DRS_HOSTNAME.rstrip('/')}/{latest_did}"

        # title
        if file_name:
            attachment["title"] = file_name.split("/")[-1]

        if attachment:
            content["attachment"] = attachment

        if content:
            content_list.append(content)

        if content_list:
            entity["content"] = content_list

        # context.related
        if data_type not in {
            constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS_INDEX,
            constants.GENOMIC_FILE.DATA_TYPE.GVCF_INDEX,
            "Simple Nucleotide Variations Index",
            "Somatic Structural Variations Index",
            constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS_INDEX,
        }:
            related = []
            for biospecimen_id in biospecimen_id_list:
                specimen_id = not_none(
                    get_target_id_from_record(
                        Specimen,
                        {CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID: biospecimen_id},
                    )
                )
                related.append({"reference": f"{Specimen.api_path}/{specimen_id}"})
            if related:
                entity.setdefault("context", {})["related"] = related

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
