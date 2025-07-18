"""
V2 JSON Processing Module for OMOP CDM ETL

This module contains the processing logic for v2.json format rules,
providing a clean, maintainable approach to data transformation.
"""

import time
from pathlib import Path
import click
import carrottransform.tools as tools
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.click import PathArgs
from carrottransform.tools.file_helpers import (
    check_dir_isvalid,
    resolve_paths,
    set_omop_filenames,
)
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.person_helpers import (
    load_person_ids,
    set_saved_person_id_file,
)
from carrottransform.tools.core_v2 import process_v2_data


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
    omop_ddl_file: Path,
    omop_config_file: Path,
    omop_version: str,
    input_dir: Path,
):
    """
    Map to OMOP output using v2 format rules
    """

    start_time = time.time()

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

    # Set OMOP filenames
    omop_config_file, omop_ddl_file = set_omop_filenames(
        omop_ddl_file, omop_config_file, omop_version
    )

    # Initialize components
    omopcdm = OmopCDM(omop_ddl_file, omop_config_file)
    mappingrules = MappingRules(rules_file, omopcdm)

    if not mappingrules.is_v2_format:
        logger.error("Rules file is not in v2 format!")
        return

    metrics = tools.metrics.Metrics(mappingrules.get_dataset_name())

    logger.info(
        f"Loaded v2 mapping rules from: {rules_file} in {time.time() - start_time:.5f} secs"
    )

    # Setup person IDs
    saved_person_id_file = set_saved_person_id_file(None, output_dir)
    person_lookup, rejected_person_count = load_person_ids(
        saved_person_id_file, person_file, mappingrules, "N"
    )

    # Save person IDs
    with saved_person_id_file.open(mode="w") as fhpout:
        fhpout.write("SOURCE_SUBJECT\tTARGET_SUBJECT\n")
        for person_id, person_assigned_id in person_lookup.items():
            fhpout.write(f"{str(person_id)}\t{str(person_assigned_id)}\n")

    # Setup output files
    output_files = mappingrules.get_all_outfile_names()
    fhd = {}
    tgtcolmaps = {}
    record_numbers = {output_file: 1 for output_file in output_files}

    for tgtfile in output_files:
        fhd[tgtfile] = (output_dir / tgtfile).with_suffix(".tsv").open(mode=write_mode)
        if write_mode == "w":
            outhdr = omopcdm.get_omop_column_list(tgtfile)
            fhd[tgtfile].write("\t".join(outhdr) + "\n")
        tgtcolmaps[tgtfile] = omopcdm.get_omop_column_map(tgtfile)

    # Process data
    try:
        outcounts, rejidcounts = process_v2_data(
            mappingrules,
            omopcdm,
            input_dir,
            output_dir,
            person_lookup,
            record_numbers,
            fhd,
            tgtcolmaps,
            metrics,
        )

        # Log results
        logger.info(
            f"person_id stats: total loaded {len(person_lookup)}, reject count {rejected_person_count}"
        )
        for tgtfile, count in outcounts.items():
            logger.info(f"TARGET: {tgtfile}: output count {count}")

        # Write summary
        data_summary = metrics.get_mapstream_summary()
        with (output_dir / "summary_mapstream.tsv").open(mode="w") as dsfh:
            dsfh.write(data_summary)

        logger.info(f"V2 processing completed in {time.time() - start_time:.5f} secs")

    finally:
        # Close output files
        for fh in fhd.values():
            fh.close()


@click.group(help="V2 Commands for mapping data to the OMOP CommonDataModel (CDM).")
def run_v2():
    pass


run_v2.add_command(mapstream_v2, "mapstream")
if __name__ == "__main__":
    run_v2()
