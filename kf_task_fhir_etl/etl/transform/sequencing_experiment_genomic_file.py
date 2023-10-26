
import logging
import pandas

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import merge_wo_duplicates
from kf_task_fhir_etl import utils

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, sequencing_experiments, genomic_files):
    logger.info(
        f"🏭 Transforming sequencing_experiment_genomic_files ..."
    )
    sequencing_experiment_genomic_files = dataservice_entity_dfs_dict.get(
        "sequencing-experiment-genomic-files"
    )
    study_merged_df = None
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
        sequencing_experiments = sequencing_experiments[
            [
                CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
                CONCEPT.SEQUENCING.STRATEGY,
            ]
        ]
    if utils.df_exists(sequencing_experiment_genomic_files):
        sequencing_experiments = merge_wo_duplicates(
            sequencing_experiments,
            sequencing_experiment_genomic_files,
            how="inner",
            on=CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
        )
        study_merged_df = merge_wo_duplicates(
            genomic_files,
            sequencing_experiments,
            how="left",
            on=CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
        )

    if utils.df_exists(study_merged_df):
        df = study_merged_df
    else:
        df = genomic_files

    return df, sequencing_experiment_genomic_files
