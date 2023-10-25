import logging, os, time, shutil
from collections import defaultdict

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
import pandas as pd

from kf_utils.dataservice.descendants import *

from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_lib_data_ingest.common.pandas_utils import outer_merge
from kf_lib_data_ingest.config import DEFAULT_KEY
from kf_lib_data_ingest.etl.load.load_v2 import LoadStage
from kf_lib_data_ingest.common.misc import clean_up_df

from kf_task_fhir_etl.config import ROOT_DIR, DATA_DIR
from kf_task_fhir_etl import utils
from kf_task_fhir_etl.etl.transform import (
    study,
    investigator,
    participant,
    family,
    family_relationship,
    biospecimen,
    biospecimen_diagnosis,
    biospecimen_genomic_file,
    diagnosis,
    phenotype,
    outcome,
    sequencing_experiment,
    sequencing_experiment_genomic_file,
    genomic_file
)
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
    ParentalSpecimen,
    ChildrenSpecimen,
    Histopathology,
    DRSDocumentReference,
    DRSDocumentReferenceIndex,
)
from kf_task_fhir_etl.target_api_plugins.kf_api_fhir_service import all_targets


logger = logging.getLogger(__name__)

DOTENV_PATH = find_dotenv()
if DOTENV_PATH:
    load_dotenv(DOTENV_PATH)


def elapsed_time_hms(start_time):
    """Get time elapsed since start_time in hh:mm:ss str format

    :param start_time: The starting time from which to calc elapsed time
    :type start_time: datetime.datetime obj
    :returns: a time string formatted as hh:mm:ss
    :rtype: str
    """
    elapsed = time.time() - start_time
    return time.strftime("%H:%M:%S", time.gmtime(elapsed))


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
            logger.info(f"  ⏳ Extracting {kf_study_id}")

            # Loop over descendants
            for endpoint, records in descendants.items():
                df = None
                if endpoint in {"investigators", "studies"}:
                    df = records
                else:
                    df = pd.DataFrame.from_dict(records, orient="index")
                mapped_df_dict.setdefault(kf_study_id, {})[endpoint] = df
                logger.info(f"    📁 {endpoint} {df.shape}")

            logger.info(f"  ✅ Extracted {kf_study_id}")

        return mapped_df_dict

    def transform(self, mapped_df_dict):
        """Transforms records.

        :param mapped_df_dict: An output from the above exract stage
        :type mapped_df_dict: dict
        :return: A dictionary of outer-merged data frames
        :rtype: dict
        """
        merged_df_dict = defaultdict()

        for kf_study_id, dataservice_entity_dfs_dict in mapped_df_dict.items():
            logger.info(f"  ⏳ Transforming {kf_study_id}")
            merged_df_dict.setdefault(kf_study_id, {})
            study_merged_df, study_all_targets = None, set()

            # studies
            studies = study.build_df(dataservice_entity_dfs_dict)
            if utils.df_exists(studies):
                study_all_targets.add(ResearchStudy)

            # investigators
            study_merged_df = investigator.build_df(
                dataservice_entity_dfs_dict, studies
            )
            investigators = dataservice_entity_dfs_dict.get("investigators")
            if utils.df_exists(investigators):
                study_all_targets.update(
                    [
                        Practitioner,
                        Organization,
                        PractitionerRole,
                    ]
                )

            # participants
            study_merged_df = participant.build_df(
                dataservice_entity_dfs_dict, study_merged_df, studies
            )
            participants = dataservice_entity_dfs_dict.get("participants")
            if utils.df_exists(participants):
                study_all_targets.update(
                    [
                        Patient,
                        ProbandStatus,
                        ResearchSubject,
                    ]
                )

            # family-relationships
            family_relationships = family_relationship.build_df(
                dataservice_entity_dfs_dict
            )
            if utils.df_exists(family_relationships):
                merged_df_dict[kf_study_id]["family_relationship"] = family_relationships 
                study_all_targets.add(FamilyRelationship)

            # families
            study_merged_df = family.build_df(
                dataservice_entity_dfs_dict, study_merged_df
            )
            families = dataservice_entity_dfs_dict.get("families")
            if utils.df_exists(families):
                study_all_targets.add(Family)

            # diagnoses
            study_merged_df = diagnosis.build_df(
                dataservice_entity_dfs_dict, study_merged_df
            )
            diagnoses = dataservice_entity_dfs_dict.get("diagnoses")
            if utils.df_exists(diagnoses):
                study_all_targets.add(Disease)

            # phenotypes
            study_merged_df = phenotype.build_df(
                dataservice_entity_dfs_dict, study_merged_df
            )
            phenotypes = dataservice_entity_dfs_dict.get("phenotypes")
            if utils.df_exists(phenotypes):
                study_all_targets.add(Phenotype)

            # outcomes
            study_merged_df = outcome.build_df(
                dataservice_entity_dfs_dict, study_merged_df
            )
            outcomes = dataservice_entity_dfs_dict.get("outcomes")
            if utils.df_exists(outcomes):
                study_all_targets.add(VitalStatus)

            # biospecimen-diagnoses
            study_merged_df, biospecimen_diagnoses = biospecimen_diagnosis.build_df(
                dataservice_entity_dfs_dict, study_merged_df
            )

            # biospecimens
            biospecimens = biospecimen.build_df(
                dataservice_entity_dfs_dict, study_merged_df
            )
            if utils.df_exists(biospecimens):
               on = [CONCEPT.PARTICIPANT.TARGET_SERVICE_ID]
               study_all_targets.update(
                   [
                       SequencingCenter,
                       ParentalSpecimen,
                       ChildrenSpecimen,
                   ]
               )

               if utils.df_exists(biospecimen_diagnoses):
                   on.append(CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID)
                   study_all_targets.add(Histopathology)

               study_merged_df = outer_merge(
                   study_merged_df,
                   biospecimens,
                   with_merge_detail_dfs=False,
                   on=on,
               )

            # biospecimen-genomic-files
            biospecimen_genomic_files = dataservice_entity_dfs_dict.get(
                "biospecimen-genomic-files"
            )
            if biospecimen_genomic_files is not None:
                columns = {
                    "biospecimen_id": CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
                    "genomic_file_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                    "external_id": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.ID,
                    "kf_id": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.TARGET_SERVICE_ID,
                    "visible": CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE,
                }
                biospecimen_genomic_files = biospecimen_genomic_files[
                    list(columns.keys())
                ]
                biospecimen_genomic_files = biospecimen_genomic_files.rename(
                    columns=columns
                )
                biospecimen_genomic_files = biospecimen_genomic_files[
                    biospecimen_genomic_files[CONCEPT.BIOSPECIMEN_GENOMIC_FILE.VISIBLE]
                    == True
                ]
                if not biospecimen_genomic_files.empty:
                    study_merged_df = outer_merge(
                        study_merged_df,
                        biospecimen_genomic_files,
                        with_merge_detail_dfs=False,
                        on=CONCEPT.BIOSPECIMEN.TARGET_SERVICE_ID,
                    )

            # genomic-files
            genomic_files = dataservice_entity_dfs_dict.get("genomic-files")
            if genomic_files is not None:
                columns = {
                    "availability": CONCEPT.GENOMIC_FILE.AVAILABILITY,
                    "controlled_access": CONCEPT.GENOMIC_FILE.CONTROLLED_ACCESS,
                    "data_type": CONCEPT.GENOMIC_FILE.DATA_TYPE,
                    "external_id": CONCEPT.GENOMIC_FILE.ID,
                    "file_format": CONCEPT.GENOMIC_FILE.FILE_FORMAT,
                    "is_harmonized": CONCEPT.GENOMIC_FILE.HARMONIZED,
                    "kf_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                    "latest_did": "GENOMIC_FILE|LATEST_DID",
                    "reference_genome": CONCEPT.GENOMIC_FILE.REFERENCE_GENOME,
                    "visible": CONCEPT.GENOMIC_FILE.VISIBLE,
                }
                genomic_files = genomic_files[list(columns.keys())]
                genomic_files = genomic_files.rename(columns=columns)
                genomic_files = genomic_files[
                    genomic_files[CONCEPT.GENOMIC_FILE.VISIBLE] == True
                ]
                if not genomic_files.empty:
                    study_merged_df = outer_merge(
                        study_merged_df,
                        genomic_files,
                        with_merge_detail_dfs=False,
                        on=CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                    )
                    study_all_targets.update(
                        [
                            DRSDocumentReference,
                            # Below is an intermediate solution for index files
                            DRSDocumentReferenceIndex,
                        ]
                    )

            # sequencing-experiment-genomic-files
            sequencing_experiment_genomic_files = dataservice_entity_dfs_dict.get(
                "sequencing-experiment-genomic-files"
            )
            if sequencing_experiment_genomic_files is not None:
                columns = {
                    "sequencing_experiment_id": CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
                    "genomic_file_id": CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                    "external_id": CONCEPT.SEQUENCING_GENOMIC_FILE.ID,
                    "kf_id": CONCEPT.SEQUENCING_GENOMIC_FILE.TARGET_SERVICE_ID,
                    "visible": CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE,
                }
                sequencing_experiment_genomic_files = (
                    sequencing_experiment_genomic_files[list(columns.keys())]
                )
                sequencing_experiment_genomic_files = (
                    sequencing_experiment_genomic_files.rename(columns=columns)
                )
                sequencing_experiment_genomic_files = (
                    sequencing_experiment_genomic_files[
                        sequencing_experiment_genomic_files[
                            CONCEPT.SEQUENCING_GENOMIC_FILE.VISIBLE
                        ]
                        == True
                    ]
                )
                if not sequencing_experiment_genomic_files.empty:
                    study_merged_df = outer_merge(
                        study_merged_df,
                        sequencing_experiment_genomic_files,
                        with_merge_detail_dfs=False,
                        on=CONCEPT.GENOMIC_FILE.TARGET_SERVICE_ID,
                    )

            # sequencing-experiments
            sequencing_experiments = dataservice_entity_dfs_dict.get("sequencing-experiments")
            if (
                sequencing_experiment_genomic_files is not None
                and sequencing_experiments is not None
            ):
                columns = {
                    "experiment_strategy": CONCEPT.SEQUENCING.STRATEGY,
                    "external_id": CONCEPT.SEQUENCING.ID,
                    "kf_id": CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
                    "visible": CONCEPT.SEQUENCING.VISIBLE,
                }
                sequencing_experiments = sequencing_experiments[list(columns.keys())]
                sequencing_experiments = sequencing_experiments.rename(columns=columns)
                sequencing_experiments = sequencing_experiments[
                    sequencing_experiments[CONCEPT.SEQUENCING.VISIBLE] == True
                ]
                if not sequencing_experiments.empty:
                    study_merged_df = outer_merge(
                        study_merged_df,
                        sequencing_experiments,
                        with_merge_detail_dfs=False,
                        on=CONCEPT.SEQUENCING.TARGET_SERVICE_ID,
                    )

            # Clean up merged data frame
            merged_df_dict[kf_study_id][DEFAULT_KEY] = clean_up_df(study_merged_df)

            self.all_targets[kf_study_id] = [
                target for target in all_targets if target in study_all_targets
            ]

            logger.info(f"  ✅ Transformed {kf_study_id}")

        return merged_df_dict

    def load(self, merged_df_dict, dry_run=False):
        """Loads records.

        :param merged_df_dict: An output from the above transform stage
        :type merged_df_dict: dict
        """
        target_api_config_path = os.path.join(
            ROOT_DIR, "kf_task_fhir_etl", "target_api_plugins", "kf_api_fhir_service.py"
        )

        for kf_study_id in merged_df_dict:
            logger.info(f"  ⏳ Loading {kf_study_id}")

            LoadStage(
                target_api_config_path,
                os.getenv("KF_API_FHIR_SERVICE_URL"),
                [cls.class_name for cls in self.all_targets[kf_study_id]],
                kf_study_id,
                cache_dir="./",
                use_async=True,
                dry_run=dry_run,
            ).run(merged_df_dict[kf_study_id])

            logger.info(f"  ✅ Loaded {kf_study_id}")


    def run(self, dry_run=False, write_output=False, stages=None):
        """Runs an ingest pipeline."""
        if not stages:
            stages = ["e", "t", "l"]

        logger.info(f"🚚 Start ingesting {self.kf_study_ids}")
        start = time.time()

        # Extract and write output to file
        logger.info(f"🏭 Start extracting {self.kf_study_ids}")
        mapped_df_dict = self.extract()
        if write_output:
            data_dir = os.path.join(DATA_DIR, "extract")
            utils.write_study_tables(mapped_df_dict, data_dir) 

        # Transform and write output to file
        if "t" in stages:
            logger.info(f"🏭 Start transforming {self.kf_study_ids}")
            merged_df_dict = self.transform(mapped_df_dict)
            if write_output:
                data_dir = os.path.join(DATA_DIR, "transform")
                utils.write_study_tables(merged_df_dict, data_dir) 

        # Load
        if "l" in stages:
            self.load(merged_df_dict, dry_run=dry_run)

        logger.info(
            f"✅ Finished ingesting {self.kf_study_ids}; "
            f"Time elapsed: {elapsed_time_hms(start)}",
        )
