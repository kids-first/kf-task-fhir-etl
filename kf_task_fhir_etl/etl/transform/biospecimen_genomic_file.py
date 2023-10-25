
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, study_merged_df):
    logger.info(
        f"🏭 Transforming biospecimen_genomic_files ..."
    )
    biospecimen_genomic_files = dataservice_entity_dfs_dict.get(
        "biospecimen-genomic-files"
    )
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
        if not biospecimen_genomic_files.empty:
            study_merged_df = outer_merge(
                study_merged_df,
                biospecimen_genomic_files,
                with_merge_detail_dfs=False,
                on=CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
            )
    return study_merged_df
