"""
Entry point for the v2 processing system
"""

import importlib.resources as resources
import time
from pathlib import Path
from typing import Optional

import click

import carrottransform.tools.sources as sources
from carrottransform import require
from carrottransform.tools.args import PathArg
from carrottransform.tools.file_helpers import (
    check_dir_isvalid,
)
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.orchestrator import V2ProcessingOrchestrator
from carrottransform.tools.types import DBConnParams

logger = logger_setup()


# Common options shared by both modes
def common_options(func):
    """Decorator for common options used by both folder and db modes"""
    func = click.option(
        "--rules-file",
        type=PathArg,
        required=True,
        help="v2 json file containing mapping rules",
    )(func)
    func = click.option(
        "--omop-ddl-file",
        type=PathArg,
        required=False,
        help="File containing OHDSI ddl statements for OMOP tables",
    )(func)
    func = click.option(
        "--omop-version",
        required=False,
        help="Quoted string containing omop version - eg '5.3'",
    )(func)

    func = click.option(
        "--person",
        envvar="PERSON",
        required=True,
        help="File or table containing person_ids in the first column",
    )(func)
    return func


import carrottransform.tools.outputs as outputs


def process_common_logic(
    inputs: sources.SourceObject,
    output: outputs.OutputTarget,
    rules_file: Path,
    write_mode: str,
    omop_ddl_file: Optional[Path],
    omop_version: Optional[str],
    person: str,
):
    assert not person.endswith(".csv"), (
        "don't call the person table .csv - just use their name"
    )

    """Common processing logic for both modes"""
    start_time = time.time()

    # this used to be a parameter; it's hard coded now but otherwise unchanged
    omop_config_file: Path = PathArg.convert("@carrot/config/config.json", None, None)
    require(omop_config_file.is_file())

    try:
        # default to 5.3 - value is onlu used for ddl fallback so nailing it in place
        if omop_version is None:
            omop_version = "5.3"

        #
        if omop_ddl_file is None:
            omop_ddl_file: Path = PathArg.convert(
                f"@carrot/config/OMOPCDM_postgresql_{omop_version}_ddl.sql", None, None
            )

        require(omop_ddl_file.is_file())

        # Create orchestrator and execute processing (pass explicit kwargs to satisfy typing)
        orchestrator = V2ProcessingOrchestrator(
            inputs=inputs,
            output=output,
            rules_file=rules_file,
            write_mode=write_mode,
            omop_ddl_file=omop_ddl_file,
            person=person,
            # rules_file=rules_file,
            # output_dir=output_dir,
            # input_dir=input_dir,
            # person_file=person_file,
            # person_table=person_table,
            # omop_ddl_file=omop_ddl_file,
            omop_config_file=omop_config_file,
            # write_mode=write_mode,
            # db_conn_params=db_conn_params,
        )

        logger.info(
            f"Loaded v2 mapping rules from: {rules_file} in {time.time() - start_time:.5f} secs"
        )

        result = orchestrator.execute_processing()

        if result.success:
            logger.info(
                f"V2 processing completed successfully in {time.time() - start_time:.5f} secs"
            )
        else:
            logger.error(f"V2 processing failed: {result.error_message}")
            exit(12)

    except Exception as e:
        import logging
        import traceback

        # Get the full stack trace as a string
        stack_trace = traceback.format_exc()
        # Write stack trace to file
        trace = Path("trace.txt").absolute()
        with trace.open("a") as f:
            f.write(f"Error occurred: {str(e)}\n")
            f.write("Full stack trace:\n")
            f.write(stack_trace)
            f.write("\n" + "=" * 50 + "\n")  # separator for multiple errors

        logger.error(f"V2 processing failed with error: {str(e)} (added to {trace=})")
        raise


@click.command()
@click.option(
    "--inputs",
    envvar="INPUTS",
    type=sources.SourceArgument,
    required=True,
    help="Input directory or database",
)
@click.option(
    "--output",
    envvar="OUTPUT",
    type=outputs.TargetArgument,
    # default=None,
    required=True,
    help="define the output directory for OMOP-format tsv files",
)
@common_options
def folder(
    inputs: sources.SourceObject,
    output: outputs.OutputTarget,
    rules_file: Path,
    person: str,
    omop_ddl_file: Optional[Path],
    omop_version: Optional[str],
):
    """Process data from folder input"""
    process_common_logic(
        rules_file=rules_file,
        output=output,
        omop_version=omop_version,
        inputs=inputs,
        person=person,
        write_mode="w",
        omop_ddl_file=omop_ddl_file,
    )


@click.command()
@click.option(
    "--person-table",
    required=True,
    help="Table containing person_ids in the first column",
)
@click.option("--username", required=True, help="Database username")
@click.option(
    "--password",
    required=True,
    help="Database password. Optional in Trino, but we will enforce this.",
)
@click.option(
    "--db-type",
    required=True,
    type=click.Choice(["postgres", "trino"]),
    help="Database type/driver that users want to access",
)
@click.option(
    "--schema",
    required=True,
    help="Database schema or input directory holding the input tables",
)
@click.option(
    "--db-name", required=True, help="Name of the Database or Catalog in Trino"
)
@click.option("--host", required=True, help="Database host")
@click.option("--port", required=True, type=int, help="Database port")
@common_options
def db(
    username: str,
    password: str,
    db_type: str,
    schema: str,
    db_name: str,
    host: str,
    port: int,
    rules_file: Path,
    output_dir: Path,
    write_mode: str,
    person_table: str,
    omop_ddl_file: Optional[Path],
    omop_version: Optional[str],
):
    """Process data from database input"""
    db_conn_params = DBConnParams(
        db_type=db_type,
        username=username,
        password=password,
        host=host,
        port=port,
        db_name=db_name,
        schema=schema,
    )

    process_common_logic(
        rules_file=rules_file,
        output_dir=output_dir,
        write_mode=write_mode,
        person_table=person_table,
        omop_ddl_file=omop_ddl_file,
        omop_version=omop_version,
        db_conn_params=db_conn_params,
    )


@click.group(help="V2 Commands for mapping data to the OMOP CommonDataModel (CDM).")
def run_v2():
    pass


# Add both commands to the group
run_v2.add_command(folder, "folder")
run_v2.add_command(db, "db")

if __name__ == "__main__":
    run_v2()
