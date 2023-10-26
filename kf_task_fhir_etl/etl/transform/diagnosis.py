
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, participants):
    logger.info(
        f"🏭 Transforming diagnoses ..."
    )
    diagnoses = dataservice_entity_dfs_dict.get("diagnoses")
    study_merged_df = None
    if diagnoses is not None:
        columns = {
            "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "age_at_event_days": CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS,
            "diagnosis_category": CONCEPT.DIAGNOSIS.CATEGORY,
            "external_id": CONCEPT.DIAGNOSIS.ID,
            "icd_id_diagnosis": CONCEPT.DIAGNOSIS.ICD_ID,
            "kf_id": CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
            "mondo_id_diagnosis": CONCEPT.DIAGNOSIS.MONDO_ID,
            "ncit_id_diagnosis": CONCEPT.DIAGNOSIS.NCIT_ID,
            "source_text_diagnosis": CONCEPT.DIAGNOSIS.NAME,
            "source_text_tumor_location": CONCEPT.DIAGNOSIS.TUMOR_LOCATION,
            "uberon_id_tumor_location": CONCEPT.DIAGNOSIS.UBERON_TUMOR_LOCATION_ID,
            "spatial_descriptor": CONCEPT.DIAGNOSIS.SPATIAL_DESCRIPTOR,
            "visible": CONCEPT.DIAGNOSIS.VISIBLE,
        }
        diagnoses = diagnoses[list(columns.keys())]
        diagnoses = diagnoses.rename(columns=columns)
        diagnoses = diagnoses[diagnoses[CONCEPT.DIAGNOSIS.VISIBLE] == True]
        if not diagnoses.empty:
            study_merged_df = outer_merge(
                participants,
                diagnoses,
                with_merge_detail_dfs=False,
                how="inner",
                on=CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            )
    return study_merged_df
