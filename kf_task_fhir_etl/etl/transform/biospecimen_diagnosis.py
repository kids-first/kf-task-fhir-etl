
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, study_merged_df):
    logger.info(
        f"🏭 Transforming biospecimen_diagnoses ..."
    )
    biospecimen_diagnoses = dataservice_entity_dfs_dict.get("biospecimen-diagnoses")
    if biospecimen_diagnoses is not None:
        columns = {
            "biospecimen_id": CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
            "diagnosis_id": CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
            "external_id": CONCEPT.BIOSPECIMEN_DIAGNOSIS.ID,
            "kf_id": CONCEPT.BIOSPECIMEN_DIAGNOSIS.TARGET_SERVICE_ID,
            "visible": CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBLE,
        }
        biospecimen_diagnoses = biospecimen_diagnoses[list(columns.keys())]
        biospecimen_diagnoses = biospecimen_diagnoses.rename(columns=columns)
        biospecimen_diagnoses = biospecimen_diagnoses[
            biospecimen_diagnoses[CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBLE] == True
        ]
        if not biospecimen_diagnoses.empty:
            study_merged_df = outer_merge(
                study_merged_df,
                biospecimen_diagnoses,
                with_merge_detail_dfs=False,
                on=CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
            )

    return study_merged_df
