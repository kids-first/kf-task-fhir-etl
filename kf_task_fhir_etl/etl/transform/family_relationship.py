
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_lib_data_ingest.common.misc import clean_up_df

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict):
    logger.info(
        f"🏭 Transforming family relationships ..."
    )
    family_relationships = dataservice_entity_dfs_dict.get("family-relationships")

    if family_relationships is not None:
        columns = {
            "participant1_id": CONCEPT.FAMILY_RELATIONSHIP.PERSON1.TARGET_SERVICE_ID,
            "participant2_id": CONCEPT.FAMILY_RELATIONSHIP.PERSON2.TARGET_SERVICE_ID,
            "external_id": CONCEPT.FAMILY_RELATIONSHIP.ID,
            "kf_id": CONCEPT.FAMILY_RELATIONSHIP.TARGET_SERVICE_ID,
            "participant1_to_participant2_relation": CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2,
            "visible": CONCEPT.FAMILY_RELATIONSHIP.VISIBLE,
        }
        family_relationships = family_relationships[list(columns.keys())]
        family_relationships = family_relationships.rename(columns=columns)
        family_relationships = family_relationships[
            family_relationships[CONCEPT.FAMILY_RELATIONSHIP.VISIBLE] == True
        ]
        if not family_relationships.empty:
            family_relationships = clean_up_df(
                family_relationships
            )

    return family_relationships
