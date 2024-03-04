
import logging
from kf_lib_data_ingest.common.concept_schema import CONCEPT

logger = logging.getLogger(__name__)

def build_df(dataservice_entity_dfs_dict, study_id):
    logger.info(
        f"🏭 Transforming studies ..."
    )
    studies = dataservice_entity_dfs_dict.get("studies")
    if studies is not None:
        columns = {
            "investigator_id": CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID,
            "attribution": CONCEPT.STUDY.ATTRIBUTION,
            "data_access_authority": CONCEPT.STUDY.AUTHORITY,
            "domain": "STUDY|DOMAIN",
            "external_id": CONCEPT.STUDY.ID,
            "kf_id": CONCEPT.STUDY.TARGET_SERVICE_ID,
            "name": CONCEPT.STUDY.NAME,
            "program": "STUDY|PROGRAM",
            "release_status": CONCEPT.STUDY.RELEASE_STATUS,
            "short_code": "STUDY|SHORT_CODE",
            "short_name": CONCEPT.STUDY.SHORT_NAME,
            "version": CONCEPT.STUDY.VERSION,
            "visible": CONCEPT.STUDY.VISIBLE,
        }
        studies = studies[list(columns.keys())]
        studies = studies.rename(columns=columns)
        studies = studies[studies[CONCEPT.STUDY.VISIBLE] == True]
        studies = studies[studies[CONCEPT.STUDY.TARGET_SERVICE_ID] == study_id]

    return studies

