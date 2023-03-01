import logging
import os
import time
from collections import defaultdict

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
import pandas as pd

from kf_utils.dataservice.descendants import *
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_task_fhir_etl.target_api_plugins.entity_builders import (
    Practitioner,
    Organization,
    PractitionerRole,
    Patient,
    ProbandStatus,
    FamilyRelationship,
    Family,
    ResearchStudy,
    ResearchSubject,
    Disease,
    Phenotype,
    VitalStatus,
    SequencingCenter,
    Specimen,
    Histopathology,
    DRSDocumentReference,
)
from kf_task_fhir_etl.target_api_plugins.kf_api_fhir_service import all_targets
from kf_lib_data_ingest.config import DEFAULT_KEY
from kf_lib_data_ingest.etl.load.load_v2 import LoadStage
from kf_task_fhir_etl.config import ROOT_DIR
from kf_lib_data_ingest.common.misc import clean_up_df

logging.basicConfig(level=logging.INFO)

DOTENV_PATH = find_dotenv()
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)

USE_ASYNC = True
CACHE_DIR = "./.data"


class Ingest:
    def __init__(self, kf_study_ids):
        """A constructor method.

        :param kf_study_ids: a list of KF study IDs
        :type kf_study_ids: list
        """
        self.kf_study_ids = kf_study_ids
        self.kf_dataservice_db_url = os.getenv("KF_DATASERVICE_DB_URL")
        self.all_targets = defaultdict()

    def _create_snapshot(self, kf_study_ids):
        """Creates a study's snapshot from the KF dataservice DB.

        :param kf_study_ids: a list of KF study IDs
        :type kf_study_ids: list
        :return: a snapshot of KF studies
        :rtype: dict
        """
        con = create_engine(self.kf_dataservice_db_url)
        snapshot = defaultdict()
        expected, found = len(kf_study_ids), 0

        # Loop over KF study IDs
        for kf_study_id in self.kf_study_ids:
            # study
            study = pd.read_sql(
                f"SELECT * FROM study WHERE kf_id = '{kf_study_id}'", con
            )
            if not study.shape[0] > 0:
                raise Exception(f"{kf_study_id} not found")

            # investigator
            investigator_id = study.investigator_id.tolist()[0]
            investigator = None
            if investigator_id:
                investigator = pd.read_sql(
                    f"SELECT * FROM investigator WHERE kf_id = '{investigator_id}'", con
                )

            # descendants
            descendants = find_descendants_by_kfids(
                self.kf_dataservice_db_url,
                "studies",
                kf_study_id,
                ignore_gfs_with_hidden_external_contribs=False,
                kfids_only=False,
            )
            descendants["studies"] = study
            if investigator is not None:
                descendants["investigators"] = investigator

            # Cache a study in memory
            snapshot[kf_study_id] = descendants
            found += 1

        assert expected == found, f"Found {found} study(ies) but expected {expected}"

        return snapshot

    def extract(self):
        """Extracts records.

        :return: A dictionary mapping an endpoint to records
        :rtype: dict
        """
        snapshot = self._create_snapshot(self.kf_study_ids)
        mapped_df_dict = defaultdict()

        for kf_study_id, descendants in snapshot.items():
            logging.info(f"  ⏳ Extracting {kf_study_id}")

            # Loop over descendants
            for endpoint, records in descendants.items():
                df = None
                if endpoint in {"investigators", "studies"}:
                    df = records
                else:
                    df = pd.DataFrame.from_dict(records, orient="index")
                df = df.drop(columns=["uuid", "created_at", "modified_at"])
                mapped_df_dict.setdefault(kf_study_id, {})[endpoint] = df
                logging.info(f"    📁 {endpoint} {df.shape}")

            logging.info(f"  ✅ Extracted {kf_study_id}")

        return mapped_df_dict

    def transform(self, mapped_df_dict):
        """Transforms records.

        :param mapped_df_dict: An output from the above exract stage
        :type mapped_df_dict: dict
        :return: A dictionary of outer-merged data frames
        :rtype: dict
        """
        merged_df_dict = defaultdict()

        for kf_study_id, study_mapped_df_dict in mapped_df_dict.items():
            logging.info(f"  ⏳ Transforming {kf_study_id}")
            merged_df_dict.setdefault(kf_study_id, {})
            study_merged_df, study_all_targets = None, set()

            # studies
            studies = study_mapped_df_dict.get("studies")
            if studies is not None:
                studies = studies.rename(
                    columns={
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
                )
                study_all_targets.add(ResearchStudy)

            # investigators
            investigators = study_mapped_df_dict.get("investigators")
            if investigators is not None:
                investigators = investigators.rename(
                    columns={
                        "external_id": CONCEPT.INVESTIGATOR.ID,
                        "institution": CONCEPT.INVESTIGATOR.INSTITUTION,
                        "kf_id": CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID,
                        "name": CONCEPT.INVESTIGATOR.NAME,
                        "visible": CONCEPT.INVESTIGATOR.VISIBLE,
                    }
                )
                study_merged_df = outer_merge(
                    studies,
                    investigators,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.INVESTIGATOR.TARGET_SERVICE_ID,
                )
                study_all_targets.update(
                    [
                        Practitioner,
                        Organization,
                        PractitionerRole,
                    ]
                )

            # participants
            participants = study_mapped_df_dict.get("participants")
            if participants is not None:
                participants = participants.rename(
                    columns={
                        "family_id": CONCEPT.FAMILY.TARGET_SERVICE_ID,
                        "study_id": CONCEPT.STUDY.TARGET_SERVICE_ID,
                        "affected_status": CONCEPT.PARTICIPANT.IS_AFFECTED_UNDER_STUDY,
                        "diagnosis_category": CONCEPT.STUDY.CATEGORY,
                        "ethnicity": CONCEPT.PARTICIPANT.ETHNICITY,
                        "external_id": CONCEPT.PARTICIPANT.ID,
                        "gender": CONCEPT.PARTICIPANT.GENDER,
                        "is_proband": CONCEPT.PARTICIPANT.IS_PROBAND,
                        "kf_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
                        "race": CONCEPT.PARTICIPANT.RACE,
                        "species": CONCEPT.PARTICIPANT.SPECIES,
                        "visible": CONCEPT.PARTICIPANT.VISIBLE,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df if study_merged_df is not None else studies,
                    participants,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.STUDY.TARGET_SERVICE_ID,
                )
                study_all_targets.update(
                    [
                        Patient,
                        ProbandStatus,
                        ResearchSubject,
                    ]
                )

            # families
            families = study_mapped_df_dict.get("families")
            if families is not None:
                families = families.rename(
                    columns={
                        "external_id": CONCEPT.FAMILY.ID,
                        "kf_id": CONCEPT.FAMILY.TARGET_SERVICE_ID,
                        "visible": CONCEPT.FAMILY.VISIBLE,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df,
                    families,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.FAMILY.TARGET_SERVICE_ID,
                )
                study_all_targets.add(Family)

            # family-relationships
            family_relationships = study_mapped_df_dict.get(
                "family-relationships")
            if family_relationships is not None:
                family_relationships = family_relationships.rename(
                    columns={
                        "participant1_id": CONCEPT.FAMILY_RELATIONSHIP.PERSON1.TARGET_SERVICE_ID,
                        "participant2_id": CONCEPT.FAMILY_RELATIONSHIP.PERSON2.TARGET_SERVICE_ID,
                        "external_id": CONCEPT.FAMILY_RELATIONSHIP.ID,
                        "kf_id": CONCEPT.FAMILY_RELATIONSHIP.TARGET_SERVICE_ID,
                        "participant1_to_participant2_relation": CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2,
                        "visible": CONCEPT.FAMILY_RELATIONSHIP.VISIBLE,
                    }
                )
                merged_df_dict[kf_study_id]["family_relationship"] = clean_up_df(
                    family_relationships
                )
                study_all_targets.add(FamilyRelationship)

            # diagnoses
            diagnoses = study_mapped_df_dict.get("diagnoses")
            if diagnoses is not None:
                diagnoses = diagnoses.rename(
                    columns={
                        "external_id": CONCEPT.DIAGNOSIS.ID,
                        "source_text_diagnosis": CONCEPT.DIAGNOSIS.NAME,
                        "diagnosis_category": CONCEPT.DIAGNOSIS.CATEGORY,
                        "source_text_tumor_location": CONCEPT.DIAGNOSIS.TUMOR_LOCATION,
                        "age_at_event_days": CONCEPT.DIAGNOSIS.EVENT_AGE_DAYS,
                        "mondo_id_diagnosis": CONCEPT.DIAGNOSIS.MONDO_ID,
                        "icd_id_diagnosis": CONCEPT.DIAGNOSIS.ICD_ID,
                        "uberon_id_tumor_location": CONCEPT.DIAGNOSIS.UBERON_TUMOR_LOCATION_ID,
                        "ncit_id_diagnosis": CONCEPT.DIAGNOSIS.NCIT_ID,
                        "spatial_descriptor": CONCEPT.DIAGNOSIS.SPATIAL_DESCRIPTOR,
                        "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
                        "kf_id": CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
                        "visible": CONCEPT.DIAGNOSIS.VISIBLE,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df,
                    diagnoses,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
                )
                study_all_targets.add(Disease)

            # phenotypes
            phenotypes = study_mapped_df_dict.get("phenotypes")
            if phenotypes is not None:
                phenotypes = phenotypes.rename(
                    columns={
                        "external_id": CONCEPT.PHENOTYPE.ID,
                        "source_text_phenotype": CONCEPT.PHENOTYPE.NAME,
                        "hpo_id_phenotype": CONCEPT.PHENOTYPE.HPO_ID,
                        "snomed_id_phenotype": CONCEPT.PHENOTYPE.SNOMED_ID,
                        "observed": CONCEPT.PHENOTYPE.OBSERVED,
                        "age_at_event_days": CONCEPT.PHENOTYPE.EVENT_AGE_DAYS,
                        "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
                        "kf_id": CONCEPT.PHENOTYPE.TARGET_SERVICE_ID,
                        "visible": CONCEPT.PHENOTYPE.VISIBLE,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df,
                    phenotypes,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
                )
                study_all_targets.add(Phenotype)

            # outcomes
            outcomes = study_mapped_df_dict.get("outcomes")
            if outcomes is not None:
                outcomes = outcomes.rename(
                    columns={
                        "participant_id": CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
                        "age_at_event_days": CONCEPT.OUTCOME.EVENT_AGE_DAYS,
                        "disease_related": CONCEPT.OUTCOME.DISEASE_RELATED,
                        "external_id": CONCEPT.OUTCOME.ID,
                        "kf_id": CONCEPT.OUTCOME.TARGET_SERVICE_ID,
                        "visible": CONCEPT.OUTCOME.VISIBLE,
                        "vital_status": CONCEPT.OUTCOME.VITAL_STATUS,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df,
                    outcomes,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.PARTICIPANT.TARGET_SERVICE_ID,
                )
                study_all_targets.add(VitalStatus)

            # biospecimen-diagnoses
            biospecimen_diagnoses = study_mapped_df_dict.get(
                "biospecimen-diagnoses")
            if biospecimen_diagnoses is not None:
                biospecimen_diagnoses = biospecimen_diagnoses.rename(
                    columns={
                        "biospecimen_id": CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
                        "diagnosis_id": CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
                        "external_id": CONCEPT.BIOSPECIMEN_DIAGNOSIS.ID,
                        "kf_id": CONCEPT.BIOSPECIMEN_DIAGNOSIS.TARGET_SERVICE_ID,
                        "visible": CONCEPT.BIOSPECIMEN_DIAGNOSIS.VISIBLE,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df,
                    biospecimen_diagnoses,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.DIAGNOSIS.TARGET_SERVICE_ID,
                )

            # biospecimens
            biospecimens = study_mapped_df_dict.get("biospecimens")
            if biospecimens is not None:
                biospecimens = biospecimens.rename(
                    columns={
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
                        "method_of_smaple_procurement": CONCEPT.BIOSPECIMEN.SAMPLE_PROCUREMENT,
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
                )
                on = [CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
                study_all_targets.update(
                    [
                        SequencingCenter,
                        Specimen,
                    ]
                )

                if biospecimen_diagnoses is not None:
                    on.append(CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID)
                    study_all_targets.add(Histopathology)

                study_merged_df = outer_merge(
                    study_merged_df, biospecimens, with_merge_detail_dfs=False, on=on
                )

            # biospecimen-genomic-files
            biospecimen_genomic_files = study_mapped_df_dict.get(
                "biospecimen-genomic-files"
            )
            if biospecimen_genomic_files is not None:
                biospecimen_genomic_files = biospecimen_genomic_files.rename(
                    columns={
                        "genomic_file_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                        "biospecimen_id": CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
                        "kf_id": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.TARGET_SERVICE_ID,
                        "visible": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE,
                        "external_id": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.ID,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df,
                    biospecimen_genomic_files,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
                )

            # genomic-files
            genomic_files = study_mapped_df_dict.get("genomic-files")
            if genomic_files is not None:
                genomic_files = genomic_files.rename(
                    columns={
                        "latest_did": "GENOMIC_FILE|LATEST_DID",
                        "external_id": CONCEPT.GENOMIC_FILE.ID,
                        "data_type": CONCEPT.GENOMIC_FILE.DATA_TYPE,
                        "file_format": CONCEPT.GENOMIC_FILE.FILE_FORMAT,
                        "is_harmonized": CONCEPT.GENOMIC_FILE.HARMONIZED,
                        "reference_genome": CONCEPT.GENOMIC_FILE.REFERENCE_GENOME,
                        "controlled_access": CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS,
                        "availability": CONCEPT.GENOMIC_FILE.AVAILABILITY,
                        "kf_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                        "visible": CONCEPT.GENOMIC_FILE.VISIBLE,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df,
                    genomic_files,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                )
                study_all_targets.add(DRSDocumentReference)

            # sequencing-experiment-genomic-files
            sequencing_experiment_genomic_files = study_mapped_df_dict.get(
                "sequencing-experiment-genomic-files"
            )
            if sequencing_experiment_genomic_files is not None:
                sequencing_experiment_genomic_files = sequencing_experiment_genomic_files.rename(
                    columns={
                        "external_id": CONCEPT.SEQUENCING_GENOMIC_FILE.ID,
                        "sequencing_experiment_id": CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
                        "genomic_file_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                        "kf_id": CONCEPT.SEQUENCING_GENOMIC_FILE.TARGET_SERVICE_ID,
                        "visible": CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df,
                    sequencing_experiment_genomic_files,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                )

            # sequencing-experiments
            sequencing_experiments = study_mapped_df_dict.get(
                "sequencing-experiments")
            if (
                sequencing_experiment_genomic_files is not None
                and sequencing_experiments is not None
            ):
                sequencing_experiments = sequencing_experiments.rename(
                    columns={
                        "experiment_strategy": CONCEPT.SEQUENCING.STRATEGY,
                        "external_id": CONCEPT.SEQUENCING.ID,
                        "kf_id": CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
                        "visible": CONCEPT.SEQUENCING.VISIBLE,
                    }
                )
                study_merged_df = outer_merge(
                    study_merged_df,
                    sequencing_experiments,
                    with_merge_detail_dfs=False,
                    on=CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
                )

            merged_df_dict[kf_study_id][DEFAULT_KEY] = clean_up_df(
                study_merged_df)

            self.all_targets[kf_study_id] = [
                target for target in all_targets if target in study_all_targets
            ]

            logging.info(f"  ✅ Transformed {kf_study_id}")

        return merged_df_dict

    def load(self, merged_df_dict):
        """Loads records.

        :param merged_df_dict: An output from the above transform stage
        :type merged_df_dict: dict
        """
        target_api_config_path = os.path.join(
            ROOT_DIR, "target_api_plugins", "kf_api_fhir_service.py"
        )

        for kf_study_id in merged_df_dict:
            logging.info(f"  ⏳ Loading {kf_study_id}")

            LoadStage(
                target_api_config_path,
                os.getenv("KF_API_FHIR_SERVICE_URL"),
                [cls.class_name for cls in self.all_targets[kf_study_id]],
                kf_study_id,
                cache_dir=CACHE_DIR,
                use_async=USE_ASYNC,
            ).run(merged_df_dict[kf_study_id])

            logging.info(f"  ✅ Loaded {kf_study_id}")

    def run(self):
        """Runs an ingest pipeline."""
        logging.info(f"🚚 Start ingesting {self.kf_study_ids}")
        start = time.time()

        # Extract
        mapped_df_dict = self.extract()

        # Transform
        merged_df_dict = self.transform(mapped_df_dict)

        # Load
        self.load(merged_df_dict)

        end = time.time()

        timedelta = end - start
        m, s = divmod(timedelta, 60)
        h, m = divmod(m, 60)

        logging.info(
            f"✅ Finished ingesting {self.kf_study_ids} "
            f"Time elapsed: {h} hours {m} minutes {s} seconds."
        )
