
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, participants):
    logger.info(
        f"🏭 Transforming outcomes ..."
    )
    outcomes = dataservice_entity_dfs_dict.get("outcomes")
    study_merged_df = None
    if outcomes is not None:
        columns = {
            "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "age_at_event_days": CONCEPT.OUTCOME.EVENT_AGE_DAYS,
            "disease_related": CONCEPT.OUTCOME.DISEASE_RELATED,
            "external_id": CONCEPT.OUTCOME.ID,
            "kf_id": CONCEPT.OUTCOME.TARGET_SERVICE_ID,
            "visible": CONCEPT.OUTCOME.VISIBLE,
            "vital_status": CONCEPT.OUTCOME.VITAL_STATUS,
        }
        outcomes = outcomes[list(columns.keys())]
        outcomes = outcomes.rename(columns=columns)
        outcomes = outcomes[outcomes[CONCEPT.OUTCOME.VISIBLE] == True]
        if not outcomes.empty:
            study_merged_df = outer_merge(
                participants,
                outcomes,
                with_merge_detail_dfs=False,
                how="inner",
                on=CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            )

    return study_merged_df
