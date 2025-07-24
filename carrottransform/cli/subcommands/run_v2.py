"""
Entry point for the v2 processing system
"""

from pathlib import Path
from typing import Optional
import click
import time
from carrottransform.tools.click import PathArgs
from carrottransform.tools.file_helpers import (
    check_dir_isvalid,
    resolve_paths,
    set_omop_filenames,
)
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.orchestrator import V2ProcessingOrchestrator

logger = logger_setup()


@click.command()
@click.option(
    "--rules-file",
    type=PathArgs,
    required=True,
    help="v2 json file containing mapping rules",
)
@click.option(
    "--output-dir",
    type=PathArgs,
    required=True,
    help="define the output directory for OMOP-format tsv files",
)
@click.option(
    "--write-mode",
    default="w",
    type=click.Choice(["w", "a"]),
    help="force write-mode on output files",
)
@click.option(
    "--person-file",
    type=PathArgs,
    required=True,
    help="File containing person_ids in the first column",
)
@click.option(
    "--omop-ddl-file",
    type=PathArgs,
    required=False,
    help="File containing OHDSI ddl statements for OMOP tables",
)
@click.option(
    "--omop-config-file",
    type=PathArgs,
    required=False,
    help="File containing additional / override json config for omop outputs",
)
@click.option(
    "--omop-version",
    required=False,
    help="Quoted string containing omop version - eg '5.3'",
)
@click.option("--input-dir", type=PathArgs, required=True, help="Input directories")
def mapstream_v2(
    rules_file: Path,
    output_dir: Path,
    write_mode: str,
    person_file: Path,
    omop_ddl_file: Optional[Path],
    omop_config_file: Optional[Path],
    omop_version: Optional[str],
    input_dir: Path,
):
    """Map to OMOP output using v2 format rules - Refactored Implementation"""

    start_time = time.time()

    try:
        # Resolve paths
        resolved_paths = resolve_paths(
            [
                rules_file,
                output_dir,
                person_file,
                omop_ddl_file,
                omop_config_file,
                input_dir,
            ]
        )
        [
            rules_file,
            output_dir,
            person_file,
            omop_ddl_file,
            omop_config_file,
            input_dir,
        ] = resolved_paths  # type: ignore

        # Validate inputs
        check_dir_isvalid(input_dir)
        check_dir_isvalid(output_dir, create_if_missing=True)

        # Set default OMOP file paths when not explicitly provided
        omop_config_file, omop_ddl_file = set_omop_filenames(
            omop_ddl_file, omop_config_file, omop_version
        )

        # Create orchestrator and execute processing
        orchestrator = V2ProcessingOrchestrator(
            rules_file=rules_file,
            output_dir=output_dir,
            input_dir=input_dir,
            person_file=person_file,
            omop_ddl_file=omop_ddl_file,
            omop_config_file=omop_config_file,
            write_mode=write_mode,
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


@click.group(help="V2 Commands for mapping data to the OMOP CommonDataModel (CDM).")
def run_v2():
    pass


run_v2.add_command(mapstream_v2, "mapstream")

if __name__ == "__main__":
    run_v2()
