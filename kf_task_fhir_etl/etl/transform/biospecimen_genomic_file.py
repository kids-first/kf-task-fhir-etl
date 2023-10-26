
import logging
import pandas

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import merge_wo_duplicates

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, biospecimens, genomic_files):
    logger.info(
        f"🏭 Transforming biospecimen_genomic_files ..."
    )
    biospecimen_genomic_files = dataservice_entity_dfs_dict.get(
        "biospecimen-genomic-files"
    )
    study_merged_df = None
    if biospecimen_genomic_files is not None:
        columns = {
            "biospecimen_id": CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
            "genomic_file_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            "external_id": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.ID,
            "kf_id": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.TARGET_SERVICE_ID,
            "visible": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE,
        }
        biospecimen_genomic_files = biospecimen_genomic_files[
            list(columns.keys())
        ]
        biospecimen_genomic_files = biospecimen_genomic_files.rename(
            columns=columns
        )
        biospecimen_genomic_files = biospecimen_genomic_files[
            biospecimen_genomic_files[CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE]
            == True
        ]
        biospecimens = biospecimens[
            [
                CONCEPT.STUDY.TARGET_SERVICE_ID,
                CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
                CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
                CONCEPT.BIOSPECIMEN.COMPOSITION,
                CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR,
            ]
        ]
        if not biospecimen_genomic_files.empty:
            biospecimens = merge_wo_duplicates(
                biospecimens,
                biospecimen_genomic_files,
                how="inner",
                on=CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
            )
            genomic_files = merge_wo_duplicates(
                genomic_files,
                biospecimen_genomic_files,
                how="inner",
                on=CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            )
            cols = [c for c in genomic_files.columns if not c.startswith("BIOSPECIMEN")]
            study_merged_df = merge_wo_duplicates(
                genomic_files[cols],
                biospecimens,
                how="left",
                on=CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            )

    return study_merged_df, biospecimen_genomic_files
