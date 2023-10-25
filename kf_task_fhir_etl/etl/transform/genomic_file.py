
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, study_merged_df):
    logger.info(
        f"🏭 Transforming genomic_files ..."
    )
    genomic_files = dataservice_entity_dfs_dict.get("genomic-files")
    if genomic_files is not None:
        columns = {
            "availability": CONCEPT.GENOMIC_FILE.AVAILABILITY,
            "controlled_access": CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS,
            "data_type": CONCEPT.GENOMIC_FILE.DATA_TYPE,
            "external_id": CONCEPT.GENOMIC_FILE.ID,
            "file_format": CONCEPT.GENOMIC_FILE.FILE_FORMAT,
            "is_harmonized": CONCEPT.GENOMIC_FILE.HARMONIZED,
            "kf_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            "latest_did": "GENOMIC_FILE|LATEST_DID",
            "reference_genome": CONCEPT.GENOMIC_FILE.REFERENCE_GENOME,
            "visible": CONCEPT.GENOMIC_FILE.VISIBLE,
        }
        genomic_files = genomic_files[list(columns.keys())]
        genomic_files = genomic_files.rename(columns=columns)
        genomic_files = genomic_files[
            genomic_files[CONCEPT.GENOMIC_FILE.VISIBLE] == True
        ]
        if not genomic_files.empty:
            study_merged_df = outer_merge(
                study_merged_df,
                genomic_files,
                with_merge_detail_dfs=False,
                on=CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            )

    return study_merged_df
