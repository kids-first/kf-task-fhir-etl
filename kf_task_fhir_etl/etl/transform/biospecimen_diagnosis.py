
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import merge_wo_duplicates

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, biospecimens, diagnoses):
    logger.info(
        f"🏭 Transforming biospecimen_diagnoses ..."
    )
    biospecimen_diagnoses = dataservice_entity_dfs_dict.get("biospecimen-diagnoses")
    study_merged_df = None

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
        d_cols =[
                   CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
                   CONCEPT.STUDY.TARGET_SERVICE_ID,
                   CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID
            ]

        diagnoses = diagnoses[d_cols]
        biospecimens = biospecimens[
            [
                CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
                CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR,
                CONCEPT.BIOSPECIMEN.COMPOSITION
            ]
        ]
        if not biospecimen_diagnoses.empty:
            biospecimens = merge_wo_duplicates(
                biospecimen_diagnoses,
                biospecimens,
                how="inner",
                on=CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
            )
            diagnoses = merge_wo_duplicates(
                biospecimen_diagnoses,
                diagnoses,
                how="inner",
                on=CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
            )
            study_merged_df = merge_wo_duplicates(
                biospecimens,
                diagnoses[d_cols],
                how="left",
                on=CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
            )

    return study_merged_df
