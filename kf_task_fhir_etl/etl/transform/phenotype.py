
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, study_merged_df):
    logger.info(
        f"🏭 Transforming phenotypes ..."
    )
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
            study_merged_df = outer_merge(
                study_merged_df,
                phenotypes,
                with_merge_detail_dfs=False,
                on=CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            )
    return study_merged_df
