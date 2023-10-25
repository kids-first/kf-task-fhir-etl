"""
Entry point for the Kids First FHIR ETL Task Service Client
"""
import click

from kf_task_fhir_etl.config import init_logger
from kf_task_fhir_etl.etl.ingest import Ingest

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}

init_logger()

@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
def cli():
    pass


@click.command()
@click.option('--stages', default="etl")
@click.option('--write-output', is_flag=True)
@click.option('--dry-run', is_flag=True)
@click.argument("kf_study_ids", required=True, nargs=-1)
def fhir_etl(kf_study_ids, dry_run, write_output, stages):
    """
    Ingest a Kids First study(ies) into a FHIR server.

    \b
    Arguments:
        \b
        KF_STUDY_IDS - a KF study ID(s) concatenated by whitespace, e.g., SD_BHJXBDQK SD_M3DBXD12
    """
    stages = list(stages) 
    assert all(s in set("etl") for s in stages), (
        f"Invalid stages value {stages}. Must one or more chars in 'etl'"
    )

    ingest = Ingest(kf_study_ids)
    ingest.run(dry_run=dry_run, write_output=write_output, stages=stages)


cli.add_command(fhir_etl)
