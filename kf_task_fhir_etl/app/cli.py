"""
Entry point for the Kids First FHIR ETL Task Service Client
"""
import click

from kf_task_fhir_etl.etl.ingest import Ingest

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option()
def cli():
    pass


@click.command()
@click.argument("kf_study_ids", required=True, nargs=-1)
def fhir_etl(kf_study_ids):
    """
    Ingest a Kids First study(ies) into a FHIR server.

    \b
    Arguments:
        \b
        KF_STUDY_IDS - a KF study ID(s) concatenated by whitespace, e.g., SD_BHJXBDQK SD_M3DBXD12
    """
    ingest = Ingest(kf_study_ids)
    ingest.run()


cli.add_command(fhir_etl)
