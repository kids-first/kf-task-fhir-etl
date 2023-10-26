
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, studies):
    logger.info(
        f"🏭 Transforming participants ..."
    )
    participants = dataservice_entity_dfs_dict.get("participants")

    if participants is not None:
        columns = {
            "family_id": CONCEPT.FAMILY.TARGET_SERVICE_ID,
            "study_id": CONCEPT.STUDY.TARGET_SERVICE_ID,
            "affected_status": CONCEPT.PARTICIPANT.IS_AFFECTED_UNDER_STUDY,
            "diagnosis_category": CONCEPT.STUDY.CATEGORY,
            "ethnicity": CONCEPT.PARTICIPANT.ETHNICITY,
            "external_id": CONCEPT.PARTICIPANT.ID,
            "gender": CONCEPT.PARTICIPANT.GENDER,
            "is_proband": CONCEPT.PARTICIPANT.IS_PROBAND,
            "kf_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "race": CONCEPT.PARTICIPANT.RACE,
            "species": CONCEPT.PARTICIPANT.SPECIES,
            "visible": CONCEPT.PARTICIPANT.VISIBLE,
        }
        participants = participants[list(columns.keys())]
        participants = participants.rename(columns=columns)
        participants = participants[
            participants[CONCEPT.PARTICIPANT.VISIBLE] == True
        ]
        if not participants.empty:
            study_merged_df = outer_merge(
                studies,
                participants,
                how="inner",
                with_merge_detail_dfs=False,
                on=CONCEPT.STUDY.TARGET_SERVICE_ID,
            )
    return study_merged_df
