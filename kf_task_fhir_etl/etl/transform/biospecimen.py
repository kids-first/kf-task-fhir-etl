
import logging

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, participants):
    logger.info(
        f"🏭 Transforming biospecimens ..."
    )
    study_merged_df = None
    biospecimens = dataservice_entity_dfs_dict.get("biospecimens")

    if biospecimens is not None:
        columns = {
            "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            "sequencing_center_id": CONCEPT.SEQUENCING.CENTER.TARGET_SERVICE_ID,
            "age_at_event_days": CONCEPT.BIOSPECIMEN.EVENT_AGE_DAYS,
            "analyte_type": CONCEPT.BIOSPECIMEN.ANALYTE,
            "composition": CONCEPT.BIOSPECIMEN.COMPOSITION,
            "consent_type": CONCEPT.BIOSPECIMEN.CONSENT_SHORT_NAME,
            "dbgap_consent_code": CONCEPT.BIOSPECIMEN.DBGAP_STYLE_CONSENT_CODE,
            "external_aliquot_id": CONCEPT.BIOSPECIMEN.ID,
            "external_sample_id": CONCEPT.BIOSPECIMEN_GROUP.ID,
            "kf_id": CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
            "method_of_sample_procurement": CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT,
            "ncit_id_anatomical_site": CONCEPT.BIOSPECIMEN.NCIT_ANATOMY_SITE_ID,
            "ncit_id_tissue_type": CONCEPT.BIOSPECIMEN.NCIT_TISSUE_TYPE_ID,
            "source_text_anatomical_site": CONCEPT.BIOSPECIMEN.ANATOMY_SITE,
            "source_text_tissue_type": CONCEPT.BIOSPECIMEN.TISSUE_TYPE,
            "source_text_tumor_descriptor": CONCEPT.BIOSPECIMEN.TUMOR_DESCRIPTOR,
            "spatial_descriptor": CONCEPT.BIOSPECIMEN.SPATIAL_DESCRIPTOR,
            "uberon_id_anatomical_site": CONCEPT.BIOSPECIMEN.UBERON_ANATOMY_SITE_ID,
            "visible": CONCEPT.BIOSPECIMEN.VISIBLE,
            "volume_ul": CONCEPT.BIOSPECIMEN.VOLUME_UL,
        }
        biospecimens = biospecimens[list(columns.keys())]
        biospecimens = biospecimens.rename(columns=columns)
        biospecimens = biospecimens[
            biospecimens[CONCEPT.BIOSPECIMEN.VISIBLE] == True
        ]
        if not biospecimens.empty:
            study_merged_df = outer_merge(
                participants,
                biospecimens,
                with_merge_detail_dfs=False,
                how="inner",
                on=CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
            )

    return study_merged_df
