
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

from kf_task_fhir_etl import utils

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict):
    logger.info(
        f"🏭 Transforming sequencing_experiments ..."
    )
    sequencing_experiments = dataservice_entity_dfs_dict.get("sequencing-experiments")
    if sequencing_experiments is not None:
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

    return sequencing_experiments
