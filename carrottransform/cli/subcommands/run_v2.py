"""
Entry point for the v2 processing system
"""

import time
from pathlib import Path
from typing import Optional

import click

from carrottransform.tools.args import PathArg
from carrottransform.tools.file_helpers import (
    check_dir_isvalid,
    set_omop_filenames,
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
        "--output-dir",
        type=PathArg,
        required=True,
        help="define the output directory for OMOP-format tsv files",
    )(func)
    func = click.option(
        "--write-mode",
        default="w",
        type=click.Choice(["w", "a"]),
        help="force write-mode on output files",
    )(func)
    func = click.option(
        "--omop-ddl-file",
        type=PathArg,
        required=False,
        help="File containing OHDSI ddl statements for OMOP tables",
    )(func)
    func = click.option(
        "--omop-config-file",
        type=PathArg,
        required=False,
        help="File containing additional / override json config for omop outputs",
    )(func)
    func = click.option(
        "--omop-version",
        required=False,
        help="Quoted string containing omop version - eg '5.3'",
    )(func)
    return func


def process_common_logic(
    rules_file: Path,
    output_dir: Path,
    write_mode: str,
    omop_ddl_file: Optional[Path],
    omop_config_file: Optional[Path],
    omop_version: Optional[str],
    person_file: Optional[Path] = None,
    person_table: Optional[str] = None,
    input_dir: Optional[Path] = None,
    db_conn_params: Optional[DBConnParams] = None,
):
    """Common processing logic for both modes"""
    start_time = time.time()

    try:
        # Resolve paths (exclude None values)
        paths_to_resolve = [
            rules_file,
            output_dir,
            person_file,
            omop_ddl_file,
            omop_config_file,
        ]
        if input_dir:
            paths_to_resolve.append(input_dir)

        # they're resolved by click now
        resolved_paths = paths_to_resolve
        # resolved_paths = resolve_paths(paths_to_resolve)

        # Update variables with resolved paths
        if resolved_paths[0] is None:
            raise ValueError("rules_file is required")
        if resolved_paths[1] is None:
            raise ValueError("output_dir is required")

        rules_file = resolved_paths[0]
        output_dir = resolved_paths[1]
        person_file = resolved_paths[2]
        omop_ddl_file = resolved_paths[3]
        omop_config_file = resolved_paths[4]

        if input_dir:
            input_dir = resolved_paths[5]

        # validate directories
        if input_dir:
            check_dir_isvalid(input_dir)
        check_dir_isvalid(output_dir, create_if_missing=True)

        # Set default OMOP file paths when not explicitly provided
        omop_config_file, omop_ddl_file = set_omop_filenames(
            omop_ddl_file, omop_config_file, omop_version
        )

        # Create orchestrator and execute processing (pass explicit kwargs to satisfy typing)
        orchestrator = V2ProcessingOrchestrator(
            rules_file=rules_file,
            output_dir=output_dir,
            input_dir=input_dir,
            person_file=person_file,
            person_table=person_table,
            omop_ddl_file=omop_ddl_file,
            omop_config_file=omop_config_file,
            write_mode=write_mode,
            db_conn_params=db_conn_params,
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

    except Exception as e:
        logger.error(f"V2 processing failed with error: {str(e)}")
        raise


@click.command()
@click.option(
    "--input-dir",
    type=PathArg,
    required=True,
    help="Input directory",
)
@click.option(
    "--person-file",
    type=PathArg,
    required=True,
    help="File containing person_ids in the first column",
)
@common_options
def folder(
    input_dir: Path,
    rules_file: Path,
    output_dir: Path,
    write_mode: str,
    person_file: Path,
    omop_ddl_file: Optional[Path],
    omop_config_file: Optional[Path],
    omop_version: Optional[str],
):
    """Process data from folder input"""
    process_common_logic(
        rules_file=rules_file,
        output_dir=output_dir,
        write_mode=write_mode,
        person_file=person_file,
        omop_ddl_file=omop_ddl_file,
        omop_config_file=omop_config_file,
        omop_version=omop_version,
        input_dir=input_dir,
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
    omop_config_file: Optional[Path],
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
        omop_config_file=omop_config_file,
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
