
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge


def build_df(dataservice_entity_dfs_dict, studies):
    investigators = dataservice_entity_dfs_dict.get("investigators")

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
            study_merged_df = outer_merge(
                studies,
                investigators,
                with_merge_detail_dfs=False,
                on=CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID,
            )
    return study_merged_df
