import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, participants):
    logger.info(
        f"🏭 Transforming families ..."
    )
    families = dataservice_entity_dfs_dict.get("families")

    if families is not None:
        columns = {
            "external_id": CONCEPT.FAMILY.ID,
            "kf_id": CONCEPT.FAMILY.TARGET_SERVICE_ID,
            "visible": CONCEPT.FAMILY.VISIBLE,
        }
        families = families[list(columns.keys())]
        families = families.rename(columns=columns)
        families = families[families[CONCEPT.FAMILY.VISIBLE] == True]
        if not families.empty:
            study_merged_df = outer_merge(
                participants,
                families,
                how="inner",
                with_merge_detail_dfs=False,
                on=CONCEPT.FAMILY.TARGET_SERVICE_ID,
            )
            study_merged_df.drop_duplicates(
                CONCEPT.FAMILY.TARGET_SERVICE_ID,
                inplace=True
            )

    return study_merged_df
