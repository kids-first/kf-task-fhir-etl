
import logging
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import merge_wo_duplicates

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, studies):
    logger.info(
        f"🏭 Transforming studies ..."
    )
    investigators = dataservice_entity_dfs_dict.get("investigators")

    study_merged_df = None
    if investigators is not None:
        columns = {
            "external_id": CONCEPT.INVESTIGATOR.ID,
            "institution": CONCEPT.INVESTIGATOR.INSTITUTION,
            "kf_id": CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID,
            "name": CONCEPT.INVESTIGATOR.NAME,
            "visible": CONCEPT.INVESTIGATOR.VISIBLE,
        }
        investigators = investigators[list(columns.keys())]
        investigators = investigators.rename(columns=columns)
        investigators = investigators[
            investigators[CONCEPT.INVESTIGATOR.VISIBLE] == True
        ]
        if not investigators.empty:
            study_merged_df = merge_wo_duplicates(
                studies,
                investigators,
                how="inner",
                on=CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID,
            )
    return study_merged_df
