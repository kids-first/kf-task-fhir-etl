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
        entity_dfs_per_study = defaultdict()

        for kf_study_id, dataservice_entity_dfs_dict in mapped_df_dict.items():
            logger.info(f"  ⏳ Transforming {kf_study_id}")
            entity_dfs_per_study.setdefault(kf_study_id, {})
            study_merged_df, study_all_targets = None, set()

            # studies
            studies = study.build_df(dataservice_entity_dfs_dict, kf_study_id)
            if utils.df_exists(studies):
                targets = [ResearchStudy]
                study_all_targets.update(targets)
                for t in targets:
                    entity_dfs_per_study[kf_study_id][t.class_name] = studies

            # investigators
            investigators = investigator.build_df(
                dataservice_entity_dfs_dict, studies
            )
            if utils.df_exists(investigators):
                targets = [
                        Practitioner,
                        Organization,
                        PractitionerRole,
                    ]
                study_all_targets.update(targets)
                for t in targets:
                    entity_dfs_per_study[kf_study_id][t.class_name] = investigators

            # participants
            participants = participant.build_df(
                dataservice_entity_dfs_dict, studies
            )
            if utils.df_exists(participants):
                targets = [
                        Patient,
                        ProbandStatus,
                        ResearchSubject,
                    ]
                study_all_targets.update(targets)
                for t in targets:
                    entity_dfs_per_study[kf_study_id][t.class_name] = participants

            # family-relationships
            family_relationships = family_relationship.build_df(
                dataservice_entity_dfs_dict
            )
            if utils.df_exists(family_relationships):
                entity_dfs_per_study[kf_study_id]["family_relationship"] = family_relationships 
                study_all_targets.add(FamilyRelationship)

            # families
            families = family.build_df(
                dataservice_entity_dfs_dict, participants
            )
            if utils.df_exists(families):
                targets = [Family]
                study_all_targets.update(targets)
                for t in targets:
                    entity_dfs_per_study[kf_study_id][t.class_name] = families

            # diagnoses
            diagnoses = diagnosis.build_df(
                dataservice_entity_dfs_dict, participants
            )
            if utils.df_exists(diagnoses):
                targets = [Disease]
                study_all_targets.update(targets)
                for t in targets:
                    entity_dfs_per_study[kf_study_id][t.class_name] = diagnoses

            # phenotypes
            phenotypes = phenotype.build_df(
                dataservice_entity_dfs_dict, participants
            )
            if utils.df_exists(phenotypes):
                targets = [Phenotype]
                study_all_targets.update(targets)
                for t in targets:
                    entity_dfs_per_study[kf_study_id][t.class_name] = phenotypes

            # outcomes
            outcomes = outcome.build_df(
                dataservice_entity_dfs_dict, participants
            )
            if utils.df_exists(outcomes):
                targets = [VitalStatus]
                study_all_targets.update(targets)
                for t in targets:
                    entity_dfs_per_study[kf_study_id][t.class_name] = outcomes

            # biospecimens
            biospecimens = biospecimen.build_df(
                dataservice_entity_dfs_dict, participants
            )
            if utils.df_exists(biospecimens):
                targets = [
                       SequencingCenter,
                       ParentalSpecimen,
                       ChildrenSpecimen,
                   ]
                study_all_targets.update(targets)
                for t in targets:
                    entity_dfs_per_study[kf_study_id][t.class_name] = biospecimens

            # biospecimen-diagnoses
            if utils.df_exists(biospecimens) and utils.df_exists(diagnoses):
                biospecimen_diagnoses = biospecimen_diagnosis.build_df(
                    dataservice_entity_dfs_dict, biospecimens, diagnoses
                )

                if utils.df_exists(biospecimen_diagnoses):
                    targets = [Histopathology]
                    study_all_targets.update(targets)
                    for t in targets:
                        entity_dfs_per_study[kf_study_id][t.class_name] = biospecimen_diagnoses


            ## biospecimen-genomic-files
            #study_merged_df = biospecimen_genomic_file.build_df(
            #    dataservice_entity_dfs_dict, study_merged_df
            #)

            ## genomic-files
            #study_merged_df = genomic_file.build_df(
            #    dataservice_entity_dfs_dict, study_merged_df
            #)
            #genomic_files = dataservice_entity_dfs_dict.get("genomic-files")
            ##if utils.df_exists(genomic_files):
            ##    study_all_targets.update(
            ##        [
            ##            DRSDocumentReference,
            ##            # Below is an intermediate solution for index files
            ##            DRSDocumentReferenceIndex,
            ##        ]
            ##    )

            ## sequencing-experiment-genomic-files
            #study_merged_df, seq_gfs = sequencing_experiment_genomic_file.build_df(
            #    dataservice_entity_dfs_dict, study_merged_df
            #)

            ## sequencing-experiments
            #study_merged_df = sequencing_experiment.build_df(
            #    dataservice_entity_dfs_dict, study_merged_df, seq_gfs
            #)

            # Clean up merged data frame
            # entity_dfs_per_study[kf_study_id][DEFAULT_KEY] = study_merged_df
            for study_id, entity_dfs in entity_dfs_per_study.items():
                for entity_type, df in entity_dfs.items():
                    logger.info(
                        f"🧼 Cleaning up {entity_type} df,"
                        f" found {df.shape[0]} records"
                    )
                    entity_dfs[entity_type] = clean_up_df(df)

            self.all_targets[kf_study_id] = [
                target for target in all_targets if target in study_all_targets
            ]

            logger.info(f"  ✅ Transformed {kf_study_id}")

        return entity_dfs_per_study

    def load(self, entity_dfs_per_study, dry_run=False):
        """Loads records.

        :param entity_dfs_per_study: An output from the above transform stage
        :type entity_dfs_per_study: dict
        """
        target_api_config_path = os.path.join(
            ROOT_DIR, "kf_task_fhir_etl", "target_api_plugins", "kf_api_fhir_service.py"
        )

        for kf_study_id in entity_dfs_per_study:
            logger.info(f"  ⏳ Loading {kf_study_id}")

            LoadStage(
                target_api_config_path,
                os.getenv("KF_API_FHIR_SERVICE_URL"),
                [cls.class_name for cls in self.all_targets[kf_study_id]],
                kf_study_id,
                cache_dir="./",
                use_async=True,
                dry_run=dry_run,
            ).run(entity_dfs_per_study[kf_study_id])

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
            entity_dfs_per_study = self.transform(mapped_df_dict)
            if write_output:
                data_dir = os.path.join(DATA_DIR, "transform")
                utils.write_study_tables(entity_dfs_per_study, data_dir) 

        # Load
        if "l" in stages:
            self.load(entity_dfs_per_study, dry_run=dry_run)

        logger.info(
            f"✅ Finished ingesting {self.kf_study_ids}; "
            f"Time elapsed: {elapsed_time_hms(start)}",
        )
