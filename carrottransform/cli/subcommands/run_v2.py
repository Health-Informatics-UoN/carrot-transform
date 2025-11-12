"""
Entry point for the v2 processing system
"""

import csv
import importlib.resources as resources
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import click
from sqlalchemy.engine import Connection
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql.expression import select

import carrottransform.tools as tools
import carrottransform.tools.args as args
import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
from carrottransform import require
from carrottransform.tools.args import PathArg, common, person_rules_check_v2
from carrottransform.tools.date_helpers import normalise_to8601
from carrottransform.tools.db import EngineConnection
from carrottransform.tools.file_helpers import (
    OutputFileManager,
    check_dir_isvalid,
)
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.orchestrator import StreamProcessor, V2ProcessingOrchestrator
from carrottransform.tools.person_helpers import (
    load_person_ids_v2,
)
from carrottransform.tools.record_builder import RecordBuilderFactory
from carrottransform.tools.stream_helpers import StreamingLookupCache
from carrottransform.tools.types import (
    DBConnParams,
    ProcessingContext,
    ProcessingResult,
    RecordContext,
)

logger = logger_setup()


# Common options shared by both modes
def common_options(func):
    """Decorator for common options used by both folder and db modes"""
    func = args.common(func)

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

        # # Create orchestrator and execute processing (pass explicit kwargs to satisfy typing)
        # orchestrator = V2ProcessingOrchestrator(
        #     inputs=inputs,
        #     output=output,
        #     rules_file=rules_file,
        #     write_mode=write_mode,
        #     omop_ddl_file=omop_ddl_file,
        #     person=person,
        #     # rules_file=rules_file,
        #     # output_dir=output_dir,
        #     # input_dir=input_dir,
        #     # person_file=person_file,
        #     # person_table=person_table,
        #     # omop_ddl_file=omop_ddl_file,
        #     omop_config_file=omop_config_file,
        #     # write_mode=write_mode,
        #     # db_conn_params=db_conn_params,
        # )

        # logger.info(
        #     f"Loaded v2 mapping rules from: {rules_file} in {time.time() - start_time:.5f} secs"
        # )

        # result = orchestrator.execute_processing()

        result = v2_via_interfaces(
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


@click.group(help="V2 Commands for mapping data to the OMOP CommonDataModel (CDM).")
def run_v2():
    pass


# Add both commands to the group
run_v2.add_command(folder, "folder")


def v2_via_interfaces(
    inputs: sources.SourceObject,
    output: outputs.OutputTarget,
    rules_file: Path,
    write_mode: str,
    omop_ddl_file: Path,
    person: str,
    omop_config_file: Path,
):
    """Main orchestrator for the entire V2 processing pipeline"""
    """Execute the complete processing pipeline with fully inlined streaming logic"""

    # try to open the persons
    inputs.open(person)

    self__inputs: sources.SourceObject = inputs
    self__person: str = person
    self__output: outputs.OutputTarget = output
    self__rules_file: Path = rules_file
    self__write_mode: str = write_mode

    assert omop_ddl_file is not None, "omopddl/omop_ddl_file musn't be null"

    # self_rules_file = rules_file
    # self_output_dir = output_dir
    # self_input_dir = input_dir
    # self_person_file = person_file
    # self_person_table = person_table
    # self_omop_ddl_file = omop_ddl_file
    # self_omop_config_file = omop_config_file
    # self_write_mode = write_mode
    # self_db_conn_params = db_conn_params

    """Initialize all processing components"""
    self_omopcdm = OmopCDM(omop_ddl_file, omop_config_file)

    self_mappingrules = MappingRules(rules_file, self_omopcdm)

    if not self_mappingrules.is_v2_format:
        raise ValueError("Rules file is not in v2 format!")
    else:
        try:
            person_rules_check_v2(self__person, self_mappingrules)
        except Exception as e:
            logger.exception(f"Validation for person rules failed: {e}")
            raise e

    self_metrics = tools.metrics.Metrics(self_mappingrules.get_dataset_name())

    # Pre-compute lookup cache for efficient streaming
    self_lookup_cache = StreamingLookupCache(self_mappingrules, self_omopcdm)

    # --- INLINE START (was setup_person_lookup) ---

    logger.info("v2 assumes it is always `person_ids` and doesn't try to reuse ids")

    # compute the "real id" to the "anon id" mapping
    person_lookup, rejected_person_count = load_person_ids_v2(
        mappingrules=self_mappingrules,
        inputs=self__inputs,
        person=self__person,
        output=outputs.OutputTarget,
    )

    # now save the IDs
    id_out = self__output.start("person_ids", ["SOURCE_SUBJECT", "TARGET_SUBJECT"])

    for person_source_id, person_assigned_id in person_lookup.items():
        id_out.write([person_source_id, person_assigned_id])

    id_out.close()

    # --- INLINE END ---

    # Log results of person lookup
    logger.info(
        f"person_id stats: total loaded {len(person_lookup)}, reject count {rejected_person_count}"
    )

    # Setup output files - keep all open for streaming
    output_files = self_mappingrules.get_all_outfile_names()
    target_column_maps = {}
    file_handles = {}
    for output_name in output_files:
        output_header = self_omopcdm.get_omop_column_list(output_name)
        target_column_map = self_omopcdm.get_omop_column_map(output_name)

        if output_header is None:
            raise Exception(f"need columns for {output_name=}")
        if target_column_map is None:
            raise Exception(f"need column map for {output_name=}")

        file_handles[output_name] = self__output.start(output_name, output_header)
        target_column_maps[output_name] = target_column_map

    record_numbers = {output_file: 1 for output_file in output_files}

    result: ProcessingResult | None = None
    logger.info("Processing data...")
    total_output_counts = {outfile: 0 for outfile in output_files}
    input_files = self_mappingrules.get_all_infile_names()
    total_rejected_counts = {infile: 0 for infile in input_files}

    # --------------------------------------------------------------------------
    # INLINE former _process_input_file_stream22()
    # --------------------------------------------------------------------------
    for source_filename in input_files:
        if result is not None:
            break  # if we failed a file already, stop

        try:
            source = self__inputs.open(
                source_filename[:-4]
                if source_filename.endswith(".csv")
                else source_filename
            )

            logger.info(f"Streaming input file: {source_filename}")

            applicable_targets = self_lookup_cache.input_to_outputs.get(
                source_filename, set()
            )
            if not applicable_targets:
                logger.info(f"No mappings found for {source_filename}")
                output_counts = {}
                rejected_count = 0
            else:
                output_counts = {target: 0 for target in applicable_targets}
                rejected_count = 0

                file_meta = self_lookup_cache.file_metadata_cache[source_filename]
                if (
                    not file_meta["datetime_source"]
                    or not file_meta["person_id_source"]
                ):
                    logger.warning(
                        f"Missing date or person ID mapping for {source_filename}"
                    )
                else:
                    try:
                        column_headers = next(source)
                        input_column_map = self_omopcdm.get_column_map(column_headers)

                        datetime_col_idx = input_column_map.get(
                            file_meta["datetime_source"]
                        )
                        if datetime_col_idx is None:
                            logger.warning(
                                f"Date field {file_meta['datetime_source']} not found in {source_filename}"
                            )
                        else:
                            # Stream process each row directly (inlined)
                            for input_data in source:
                                # Increment input count
                                self_metrics.increment_key_count(
                                    source=source_filename,
                                    fieldname="all",
                                    tablename="all",
                                    concept_id="all",
                                    additional="",
                                    count_type="input_count",
                                )

                                # Normalize date
                                fulldate = normalise_to8601(
                                    input_data[datetime_col_idx]
                                )
                                if fulldate is None:
                                    self_metrics.increment_key_count(
                                        source=source_filename,
                                        fieldname="all",
                                        tablename="all",
                                        concept_id="all",
                                        additional="",
                                        count_type="input_date_fields",
                                    )
                                    rejected_count += 1
                                    continue

                                input_data[datetime_col_idx] = fulldate

                                # Process per target
                                for target_file in applicable_targets:
                                    v2_mapping = self_mappingrules.v2_mappings[
                                        target_file
                                    ][source_filename]
                                    target_column_map = target_column_maps[target_file]

                                    target_meta = (
                                        self_lookup_cache.target_metadata_cache[
                                            target_file
                                        ]
                                    )
                                    auto_num_col = target_meta["auto_num_col"]
                                    person_id_col = target_meta["person_id_col"]
                                    date_col_data = target_meta["date_col_data"]
                                    date_component_data = target_meta[
                                        "date_component_data"
                                    ]
                                    notnull_numeric_fields = target_meta[
                                        "notnull_numeric_fields"
                                    ]

                                    data_columns = file_meta["data_fields"].get(
                                        target_file, []
                                    )

                                    output_count_local = 0
                                    rejected_count_local = 0

                                    for data_column in data_columns:
                                        if data_column not in input_column_map:
                                            continue

                                        # Build the record directly
                                        context = RecordContext(
                                            tgtfilename=target_file,
                                            tgtcolmap=target_column_map,
                                            v2_mapping=v2_mapping,
                                            srcfield=data_column,
                                            srcdata=input_data,
                                            srccolmap=input_column_map,
                                            srcfilename=source_filename,
                                            omopcdm=self_omopcdm,
                                            metrics=self_metrics,
                                            person_lookup=person_lookup,
                                            record_numbers=record_numbers,
                                            file_handles=file_handles,
                                            auto_num_col=auto_num_col,
                                            person_id_col=person_id_col,
                                            date_col_data=date_col_data,
                                            date_component_data=date_component_data,
                                            notnull_numeric_fields=notnull_numeric_fields,
                                        )

                                        builder = RecordBuilderFactory.create_builder(
                                            context
                                        )
                                        result_obj = builder.build_records()

                                        self_metrics = result_obj.metrics

                                        if not result_obj.success:
                                            rejected_count_local += 1

                                        output_count_local += result_obj.record_count

                                    output_counts[target_file] += output_count_local
                                    rejected_count += rejected_count_local

                    except Exception as e:
                        logger.error(
                            f"Error streaming file {source_filename}: {str(e)}"
                        )

            # Update totals
            for target_file, count in output_counts.items():
                total_output_counts[target_file] += count
            total_rejected_counts[source_filename] = rejected_count

        except Exception as e:
            logger.error(f"Error processing file {source_filename}: {str(e)}")
            result = ProcessingResult(
                total_output_counts,
                total_rejected_counts,
                success=False,
                error_message=str(e),
            )
            break

    # --------------------------------------------------------------------------

    if result is None:
        result = ProcessingResult(total_output_counts, total_rejected_counts)

    # write outputs
    for target_file, count in result.output_counts.items():
        logger.info(f"TARGET: {target_file}: output count {count}")

    # Write summary
    data_summary = None
    for line in self_metrics.get_mapstream_summary().strip().split("\n"):
        row = line.split("\t")

        if data_summary is None:
            data_summary = self__output.start("summary_mapstream", row)
        else:
            data_summary.write(row)

    assert data_summary is not None
    data_summary.close()

    # close/flush these because we need the files on-diks for unit test valiation
    output.close()
    inputs.close()
    return result
