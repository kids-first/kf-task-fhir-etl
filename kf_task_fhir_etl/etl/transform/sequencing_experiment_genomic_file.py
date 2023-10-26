
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, study_merged_df):
    logger.info(
        f"🏭 Transforming sequencing_experiment_genomic_files ..."
    )
    sequencing_experiment_genomic_files = dataservice_entity_dfs_dict.get(
        "sequencing-experiment-genomic-files"
    )
    if sequencing_experiment_genomic_files is not None:
        columns = {
            "sequencing_experiment_id": CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
            "genomic_file_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            "external_id": CONCEPT.SEQUENCING_GENOMIC_FILE.ID,
            "kf_id": CONCEPT.SEQUENCING_GENOMIC_FILE.TARGET_SERVICE_ID,
            "visible": CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE,
        }
        sequencing_experiment_genomic_files = (
            sequencing_experiment_genomic_files[list(columns.keys())]
        )
        sequencing_experiment_genomic_files = (
            sequencing_experiment_genomic_files.rename(columns=columns)
        )
        sequencing_experiment_genomic_files = (
            sequencing_experiment_genomic_files[
                sequencing_experiment_genomic_files[
                    CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE
                ]
                == True
            ]
        )
        if not sequencing_experiment_genomic_files.empty:
            study_merged_df = outer_merge(
                study_merged_df,
                sequencing_experiment_genomic_files,
                with_merge_detail_dfs=False,
                on=CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
            )

    return study_merged_df, sequencing_experiment_genomic_files
