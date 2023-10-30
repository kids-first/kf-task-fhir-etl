
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import merge_wo_duplicates

from kf_task_fhir_etl import utils

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, participants, families):
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
        
        family_col = CONCEPT.FAMILY.TARGET_SERVICE_ID
        participant_col = CONCEPT.PARTICIPANT.TARGET_SERVICE_ID
        participants = participants[[participant_col, family_col]]
        person1_col = CONCEPT.FAMILY_RELATIONSHIP.PERSON1.TARGET_SERVICE_ID
        person2_col = CONCEPT.FAMILY_RELATIONSHIP.PERSON2.TARGET_SERVICE_ID
        
        person1 = participants.rename(
            columns={participant_col : person1_col},
        )
        person2 = participants.rename(
            columns={participant_col : person2_col},
        )

        # Only take participants that are visible AND
        # are part of visible families
        if utils.df_exists(families):
            families = families[[CONCEPT.FAMILY.TARGET_SERVICE_ID]]
            person1 = merge_wo_duplicates(
                families,
                person1,
                on=CONCEPT.FAMILY.TARGET_SERVICE_ID
            )
            person2 = merge_wo_duplicates(
                families,
                person2,
                on=CONCEPT.FAMILY.TARGET_SERVICE_ID
            )
        
        family_relationships = merge_wo_duplicates(
            family_relationships,
            person1,
            on=person1_col
        )
        family_relationships = merge_wo_duplicates(
            family_relationships,
            person2,
            on=person2_col
        )

    return family_relationships
