
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

from kf_task_fhir_etl import utils

logger = logging.getLogger(__name__)

def build_df(
    dataservice_entity_dfs_dict, study_merged_df, sequencing_experiment_genomic_files
):
    logger.info(
        f"🏭 Transforming sequencing_experiments ..."
    )
    sequencing_experiments = dataservice_entity_dfs_dict.get("sequencing-experiments")
    if (
        utils.df_exists(sequencing_experiment_genomic_files)
        and utils.df_exists(sequencing_experiments)
    ):
        columns = {
            "experiment_strategy": CONCEPT.SEQUENCING.STRATEGY,
            "external_id": CONCEPT.SEQUENCING.ID,
            "kf_id": CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
            "visible": CONCEPT.SEQUENCING.VISIBLE,
        }
        sequencing_experiments = sequencing_experiments[list(columns.keys())]
        sequencing_experiments = sequencing_experiments.rename(columns=columns)
        sequencing_experiments = sequencing_experiments[
            sequencing_experiments[CONCEPT.SEQUENCING.VISIBLE] == True
        ]
        if not sequencing_experiments.empty:
            study_merged_df = outer_merge(
                study_merged_df,
                sequencing_experiments,
                with_merge_detail_dfs=False,
                on=CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
            )

    return study_merged_df
