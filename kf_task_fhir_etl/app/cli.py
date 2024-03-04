"""
Entry point for the Kids First FHIR ETL Task Service Client
"""
from pprint import pformat
import click

from kf_lib_data_ingest.common.misc import multisplit

from kf_task_fhir_etl.config import init_logger
from kf_task_fhir_etl.etl.ingest import Ingest, DEFAULT_ENTITY_BUILDERS

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}

init_logger()

def validate_entity_builders(entity_builders):
    """
    Validate entity-builders CLI option
    """
    entity_builders = set(entity_builders)
    default = set(DEFAULT_ENTITY_BUILDERS)

    if not (entity_builders <= default):
        invalid = entity_builders - default
        raise click.BadParameter(
            f"Invalid entity_builders: {pformat(invalid)}"
            f" Must be one of {pformat(default)}"
        )


def validate_stages(stages):
    """
    Validate stages CLI option
    """
    stages = list(stages) 
    if not all(s in set("etl") for s in stages):
        raise click.BadParameter(
        f"Invalid stages value {stages}. Must one or more chars in 'etl'"
    )


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
def cli():
    pass


@click.command()
@click.option('--entity-builders')
@click.option('--stages', default="etl")
@click.option('--write-output', is_flag=True)
@click.option('--dry-run', is_flag=True)
@click.argument("kf_study_ids", required=True, nargs=-1)
def fhir_etl(kf_study_ids, dry_run, write_output, stages, entity_builders):
    """
    Ingest a Kids First study(ies) into a FHIR server.

    \b
    Arguments:
        \b
        KF_STUDY_IDS - a KF study ID(s) concatenated by whitespace, e.g., SD_BHJXBDQK SD_M3DBXD12
    """
    validate_stages(stages)
    if entity_builders:
        entity_builders = multisplit(entity_builders, [",", ";"])
        validate_entity_builders(entity_builders)
    else:
        entity_builders = DEFAULT_ENTITY_BUILDERS

    ingest = Ingest(kf_study_ids)
    ingest.run(
        dry_run=dry_run, write_output=write_output, stages=stages,
        entity_builders=entity_builders
    )


cli.add_command(fhir_etl)
