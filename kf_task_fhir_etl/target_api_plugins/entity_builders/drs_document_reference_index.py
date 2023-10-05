"""
Builds FHIR DocumentReference resources (http://hl7.org/fhir/R4/documentreference.html) 
from rows of tabular genomic file index data.
"""
import os
from abc import abstractmethod

import pandas as pd

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.common.constants import MISSING_DATA_VALUES
from kf_task_fhir_etl.target_api_plugins.entity_builders import (
    Patient,
    DRSDocumentReference,
)
from kf_task_fhir_etl.common.utils import (
    not_none,
    drop_none,
    yield_resource_ids,
    get_dataservice_entity,
)

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
    constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads-Index",
        "display": "Aligned Reads Index",
    },
    "Annotated Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations-Index",
        "display": "Simple Nucleotide Variations Index",
    },
    "Annotated Variant Call Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations-Index",
        "display": "Simple Nucleotide Variations Index",
    },
    "Consensus Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Simple-Nucleotide-Variations-Index",
        "display": "Simple Nucleotide Variations Index",
    },
    "Genome Aligned Read Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Aligned-Reads-Index",
        "display": "Aligned Reads Index",
    },
    "Genomic Variant Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF-Index",
        "display": "gVCF Index",
    },
    constants.GENOMIC_FILE.DATA_TYPE.GVCF_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "gVCF-Index",
        "display": "gVCF Index",
    },
    "Masked Consensus Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations-Index",
        "display": "Somatic Simple Nucleotide Variations Index",
    },
    "Masked Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations-Index",
        "display": "Somatic Simple Nucleotide Variations Index",
    },
    "Pre-pass Somatic Structural Variation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations-Index",
        "display": "Somatic Structural Variations Index",
    },
    "Raw Germline Structural Variation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Germline-Structural-Variations-Index",
        "display": "Germline Structural Variations Index",
    },
    "Raw Simple Somatic Mutation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Simple-Nucleotide-Variations-Index",
        "display": "Somatic Simple Nucleotide Variations Index",
    },
    "Raw Somatic Structural Variation Index": {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Somatic-Structural-Variations-Index",
        "display": "Somatic Structural Variations Index",
    },
    constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS_INDEX: {
        "system": "https://includedcc.org/fhir/code-systems/data_types",
        "code": "Variant-Calls-Index",
        "display": "Variant Calls Index",
    },
}

# https://includedcc.org/fhir/code-systems/experimental_strategies
experimental_strategy_coding = {
    constants.SEQUENCING.STRATEGY.LINKED_WGS: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "Linked-Read-WGS",
        "display": "Linked-Read WGS",
    },
    constants.SEQUENCING.STRATEGY.METHYL: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "Methylation",
        "display": "Methylation",
    },
    constants.SEQUENCING.STRATEGY.MRNA: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "miRNA-Seq",
        "display": "MicroRNA Sequencing",
    },
    constants.SEQUENCING.STRATEGY.RNA: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "RNA-Seq",
        "display": "RNA Sequencing",
    },
    "scRNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "scRNA-Seq",
        "display": "Single-Cell RNA Sequencing",
    },
    "snRNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "snRNA-Seq",
        "display": "Single-Nucleus RNA Sequencing",
    },
    constants.SEQUENCING.STRATEGY.TARGETED: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "Targeted-Sequencing",
        "display": "Targeted Sequencing",
    },
    constants.SEQUENCING.STRATEGY.WGS: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "WGS",
        "display": "Whole Genome Sequencing",
    },
    constants.SEQUENCING.STRATEGY.WXS: {
        "system": "https://includedcc.org/fhir/code-systems/experimental_strategies",
        "code": "WXS",
        "display": "Whole Exome Sequencing",
    },
}

# https://includedcc.org/fhir/code-systems/data_categories
data_cateogry_coding = {
    constants.SEQUENCING.STRATEGY.LINKED_WGS: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    constants.SEQUENCING.STRATEGY.METHYL: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    constants.SEQUENCING.STRATEGY.MRNA: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    "scRNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    "snRNA-Seq": {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    constants.SEQUENCING.STRATEGY.RNA: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Transcriptomics",
        "display": "Transcriptomics",
    },
    constants.SEQUENCING.STRATEGY.TARGETED: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    constants.SEQUENCING.STRATEGY.WGS: {
        "system": "https://includedcc.org/fhir/code-systems/data_categories",
        "code": "Genomics",
        "display": "Genomics",
    },
    constants.SEQUENCING.STRATEGY.WXS: {
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


class DRSDocumentReferenceIndex:
    class_name = "drs_document_reference_index"
    api_path = "DocumentReference"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def transform_records_list(cls, records_list):
        records = pd.DataFrame(records_list)
        outer_by = "file_group"
        records[outer_by] = records[CONCEPT.GENOMIC_FILE.ID].apply(
            lambda x: x.split(".")[0] if isinstance(x, str) else x
        )

        transfromed_records_list = []
        for outer_names, outer_group in records.groupby(by=outer_by):
            inner_by = [
                CONCEPT.STUDY.TARGET_SERVICE_ID,
                CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                CONCEPT.GENOMIC_FILE.DATA_TYPE,
            ]
            if outer_group.get(CONCEPT.SEQUENCING.TARGET_SERVICE_ID) is not None:
                inner_by.append(CONCEPT.SEQUENCING.TARGET_SERVICE_ID)

            relates_to_list = []
            for inner_names, inner_group in outer_group.groupby(by=inner_by):
                if inner_names[2] not in {
                    constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS,
                    constants.GENOMIC_FILE.DATA_TYPE.GVCF,
                    constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS,
                }:
                    continue
                relates_to_list.append(inner_names[1])
            relates_to_list = list(set(relates_to_list))

            for inner_names, inner_group in outer_group.groupby(by=inner_by):
                if inner_names[2] not in {
                    constants.GENOMIC_FILE.DATA_TYPE.ALIGNED_READS_INDEX,
                    constants.GENOMIC_FILE.DATA_TYPE.GVCF_INDEX,
                    constants.GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS_INDEX,
                }:
                    continue

                transfromed_record = {
                    CONCEPT.STUDY.TARGET_SERVICE_ID: inner_names[0],
                    CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID: inner_names[1],
                    CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: inner_group.get(
                        CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
                    ).drop_duplicates(),
                    "RELATES_TO": relates_to_list,
                }

                try:
                    transfromed_record.update(
                        {
                            CONCEPT.SEQUENCING.TARGET_SERVICE_ID: inner_names[3],
                            CONCEPT.SEQUENCING.STRATEGY: inner_group.get(
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
        return {
            "_tag": record[CONCEPT.STUDY.TARGET_SERVICE_ID],
            "identifier": not_none(record[CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID]),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.STUDY.TARGET_SERVICE_ID]
        genomic_file_id = record[CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID]
        strategy = record.get(CONCEPT.SEQUENCING.STRATEGY)
        participant_id_list = record[CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]

        # Get genomic file entity
        base_url = KF_API_DATASERVICE_URL.rstrip("/")
        genomic_file = get_dataservice_entity(
            base_url, f"/genomic-files/{genomic_file_id}"
        )["results"]
        acl_list = genomic_file.get("acl")
        controlled_access = genomic_file.get("controlled_access")
        data_type = genomic_file.get("data_type")
        file_format = genomic_file.get("file_format")
        file_name = genomic_file.get("file_name")
        hash_dict = genomic_file.get("hashes")
        latest_did = genomic_file.get("latest_did")
        size = genomic_file.get("size")
        url_list = genomic_file.get("urls")

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
                    "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/ncpi-drs-document-reference"
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
        if participant_id_list.nunique() == 1:
            try:
                patient_id = not_none(
                    get_target_id_from_record(
                        Patient,
                        {
                            CONCEPT.STUDY.TARGET_SERVICE_ID: study_id,
                            CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: participant_id_list.tolist()[
                                0
                            ],
                        },
                    )
                )
                entity["subject"] = {"reference": f"{Patient.api_path}/{patient_id}"}
            except:
                pass

        # relatesTo
        for relates_to in record.get("RELATES_TO"):
            try:
                document_reference_id = not_none(
                    get_target_id_from_record(
                        DRSDocumentReference,
                        {
                            CONCEPT.STUDY.TARGET_SERVICE_ID: study_id,
                            CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID: relates_to,
                        },
                    )
                )
                entity.setdefault("relatesTo", []).append(
                    {
                        "code": "transforms",
                        "target": {
                            "reference": f"{DRSDocumentReference.api_path}/{document_reference_id}"
                        },
                    }
                )
            except:
                pass

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
        if file_format and file_format not in MISSING_DATA_VALUES:
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

        # File content, i.e., path to S3
        if url_list:
            for url in url_list:
                content_list.append({"attachment": {"url": url}})

        if content_list:
            entity["content"] = content_list

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
