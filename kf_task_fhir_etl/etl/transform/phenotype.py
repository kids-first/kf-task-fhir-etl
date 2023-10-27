
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import merge_wo_duplicates

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, participants):
    logger.info(
        f"🏭 Transforming phenotypes ..."
    )
    study_merged_df = None
    phenotypes = dataservice_entity_dfs_dict.get("phenotypes")
    if phenotypes is not None:
        columns = {
            "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "age_at_event_days": CONCEPT.PHENOTYPE.EVENT_AGE_DAYS,
            "external_id": CONCEPT.PHENOTYPE.ID,
            "hpo_id_phenotype": CONCEPT.PHENOTYPE.HPO_ID,
            "kf_id": CONCEPT.PHENOTYPE.TARGET_SERVICE_ID,
            "observed": CONCEPT.PHENOTYPE.OBSERVED,
            "snomed_id_phenotype": CONCEPT.PHENOTYPE.SNOMED_ID,
            "source_text_phenotype": CONCEPT.PHENOTYPE.NAME,
            "visible": CONCEPT.PHENOTYPE.VISIBLE,
        }
        phenotypes = phenotypes[list(columns.keys())]
        phenotypes = phenotypes.rename(columns=columns)
        phenotypes = phenotypes[phenotypes[CONCEPT.PHENOTYPE.VISIBLE] == True]
        if not phenotypes.empty:
            study_merged_df = merge_wo_duplicates(
                participants,
                phenotypes,
                how="inner",
                on=CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            )
    return study_merged_df
