import csv
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.engine import Connection
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql.expression import select

import carrottransform.tools as tools
from carrottransform.tools.args import person_rules_check_v2
from carrottransform.tools.date_helpers import normalise_to8601
from carrottransform.tools.db import EngineConnection
from carrottransform.tools.file_helpers import OutputFileManager
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.person_helpers import (
    load_person_ids_v2,
    set_saved_person_id_file,
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


class StreamProcessor:
    """Efficient single-pass streaming processor"""

    def __init__(self, context: ProcessingContext, lookup_cache: StreamingLookupCache):
        self.context = context
        self.cache = lookup_cache

    def process_all_data(self) -> ProcessingResult:
        """Process all data with single-pass streaming approach"""
        logger.info("Processing data...")
        total_output_counts = {outfile: 0 for outfile in self.context.output_files}
        total_rejected_counts = {infile: 0 for infile in self.context.input_files}

        # Process each input file
        for source_filename in self.context.input_files:
            try:
                output_counts, rejected_count = self._process_input_file_stream(
                    source_filename,
                    self.context.db_connection,
                    self.context.schema,
                )

                # Update totals
                for target_file, count in output_counts.items():
                    total_output_counts[target_file] += count
                total_rejected_counts[source_filename] = rejected_count

            except Exception as e:
                logger.error(f"Error processing file {source_filename}: {str(e)}")
                return ProcessingResult(
                    total_output_counts,
                    total_rejected_counts,
                    success=False,
                    error_message=str(e),
                )

        return ProcessingResult(total_output_counts, total_rejected_counts)

    def _process_input_file_stream(
        self,
        source_filename: str,
        db_connection: Optional[Connection] = None,
        schema: Optional[str] = None,
    ) -> Tuple[Dict[str, int], int]:
        """Stream process a single input file with direct output writing"""
        logger.info(f"Streaming input file: {source_filename}")
        if db_connection is None:
            # Ensure we have a valid input directory in folder mode
            if self.context.input_dir is None:
                logger.warning(
                    "No input_dir provided; skipping file streaming for folder mode"
                )
                return {}, 0

            file_path = self.context.input_dir / source_filename
            if not file_path.exists():
                logger.warning(f"Input file not found: {source_filename}")
                return {}, 0

        # Get which output tables this input file can map to
        applicable_targets = self.cache.input_to_outputs.get(source_filename, set())
        if not applicable_targets:
            logger.info(f"No mappings found for {source_filename}")
            return {}, 0

        output_counts = {target: 0 for target in applicable_targets}
        rejected_count = 0

        # Get file metadata from cache
        file_meta = self.cache.file_metadata_cache[source_filename]
        if not file_meta["datetime_source"] or not file_meta["person_id_source"]:
            logger.warning(f"Missing date or person ID mapping for {source_filename}")
            return output_counts, rejected_count

        try:
            # TODO: refactor this logic to be more efficient
            if db_connection is None:
                with file_path.open(mode="r", encoding="utf-8-sig") as fh:
                    csv_reader = csv.reader(fh)
                    csv_column_headers = next(csv_reader)
                    input_column_map = self.context.omopcdm.get_column_map(
                        csv_column_headers
                    )

                    # Validate required columns exist
                    datetime_col_idx = input_column_map.get(
                        file_meta["datetime_source"]
                    )
                    if datetime_col_idx is None:
                        logger.warning(
                            f"Date field {file_meta['datetime_source']} not found in {source_filename}"
                        )
                        return output_counts, rejected_count

                    # Stream process each row
                    for input_data in csv_reader:
                        row_counts, row_rejected = self._process_single_row_stream(
                            source_filename,
                            input_data,
                            input_column_map,
                            applicable_targets,
                            datetime_col_idx,
                            file_meta,
                        )

                        for target, count in row_counts.items():
                            output_counts[target] += count
                        rejected_count += row_rejected
            else:
                source_table_model = Table(
                    source_filename.split(".")[0],
                    MetaData(schema=schema),
                    autoload_with=db_connection,
                )
                source_table_headers = source_table_model.columns.keys()
                source_table_data = db_connection.execute(
                    select(source_table_model)
                ).fetchall()
                input_column_map = self.context.omopcdm.get_column_map(
                    source_table_headers
                )

                # Validate required columns exist
                datetime_col_idx = input_column_map.get(file_meta["datetime_source"])
                if datetime_col_idx is None:
                    logger.warning(
                        f"Date field {file_meta['datetime_source']} not found in table {source_filename.split('.')[0]}"
                    )
                    return output_counts, rejected_count

                # Stream process each row
                for row in source_table_data:
                    # Convert DB row to a mutable list of strings
                    input_row = ["" if v is None else str(v) for v in row]
                    row_counts, row_rejected = self._process_single_row_stream(
                        source_filename,
                        input_row,
                        input_column_map,
                        applicable_targets,
                        datetime_col_idx,
                        file_meta,
                    )

                    for target, count in row_counts.items():
                        output_counts[target] += count
                    rejected_count += row_rejected

        except Exception as e:
            logger.error(f"Error streaming file {source_filename}: {str(e)}")

        return output_counts, rejected_count

    def _process_single_row_stream(
        self,
        source_filename: str,
        input_data: List[str],
        input_column_map: Dict[str, int],
        applicable_targets: Set[str],
        datetime_col_idx: int,
        file_meta: Dict[str, Any],
    ) -> Tuple[Dict[str, int], int]:
        """Process single row and write directly to all applicable output files"""

        # Increment input count
        self.context.metrics.increment_key_count(
            source=source_filename,
            fieldname="all",
            tablename="all",
            concept_id="all",
            additional="",
            count_type="input_count",
        )

        # Normalize date once
        fulldate = normalise_to8601(input_data[datetime_col_idx])
        if fulldate is None:
            self.context.metrics.increment_key_count(
                source=source_filename,
                fieldname="all",
                tablename="all",
                concept_id="all",
                additional="",
                count_type="input_date_fields",
            )
            return {}, 1

        input_data[datetime_col_idx] = fulldate

        row_output_counts = {}
        total_rejected = 0

        # Process this row for each applicable target table
        for target_file in applicable_targets:
            target_counts, target_rejected = self._process_row_for_target_stream(
                source_filename, input_data, input_column_map, target_file, file_meta
            )

            row_output_counts[target_file] = target_counts
            total_rejected += target_rejected

        return row_output_counts, total_rejected

    def _process_row_for_target_stream(
        self,
        source_filename: str,
        input_data: List[str],
        input_column_map: Dict[str, int],
        target_file: str,
        file_meta: Dict[str, Any],
    ) -> Tuple[int, int]:
        """Process row for specific target and write records directly"""

        v2_mapping = self.context.mappingrules.v2_mappings[target_file][source_filename]
        target_column_map = self.context.target_column_maps[target_file]

        # Get target metadata from cache
        target_meta = self.cache.target_metadata_cache[target_file]
        auto_num_col = target_meta["auto_num_col"]
        person_id_col = target_meta["person_id_col"]
        date_col_data = target_meta["date_col_data"]
        date_component_data = target_meta["date_component_data"]
        notnull_numeric_fields = target_meta["notnull_numeric_fields"]

        data_columns = file_meta["data_fields"].get(target_file, [])

        output_count = 0
        rejected_count = 0

        # Process each data column for this target
        for data_column in data_columns:
            if data_column not in input_column_map:
                continue

            column_output, column_rejected = self._process_data_column_stream(
                source_filename,
                input_data,
                input_column_map,
                target_file,
                v2_mapping,
                target_column_map,
                data_column,
                auto_num_col,
                person_id_col,
                date_col_data,
                date_component_data,
                notnull_numeric_fields,
            )

            output_count += column_output
            rejected_count += column_rejected

        return output_count, rejected_count

    def _process_data_column_stream(
        self,
        source_filename: str,
        input_data: List[str],
        input_column_map: Dict[str, int],
        target_file: str,
        v2_mapping,
        target_column_map: Dict[str, int],
        data_column: str,
        auto_num_col: Optional[str],
        person_id_col: str,
        date_col_data: Dict[str, str],
        date_component_data: Dict[str, Dict[str, str]],
        notnull_numeric_fields: List[str],
    ) -> Tuple[int, int]:
        """Process data column and write records directly to output"""

        rejected_count = 0
        # Create context for record building with direct write capability
        context = RecordContext(
            tgtfilename=target_file,
            tgtcolmap=target_column_map,
            v2_mapping=v2_mapping,
            srcfield=data_column,
            srcdata=input_data,
            srccolmap=input_column_map,
            srcfilename=source_filename,
            omopcdm=self.context.omopcdm,
            metrics=self.context.metrics,
            # Additional context for direct writing
            person_lookup=self.context.person_lookup,
            record_numbers=self.context.record_numbers,
            file_handles=self.context.file_handles,
            auto_num_col=auto_num_col,
            person_id_col=person_id_col,
            date_col_data=date_col_data,
            date_component_data=date_component_data,
            notnull_numeric_fields=notnull_numeric_fields,
        )

        # Build records
        builder = RecordBuilderFactory.create_builder(context)
        result = builder.build_records()

        # Update metrics
        self.context.metrics = result.metrics

        if not result.success:
            rejected_count += 1

        return result.record_count, rejected_count


class V2ProcessingOrchestrator:
    """Main orchestrator for the entire V2 processing pipeline"""

    def __init__(
        self,
        rules_file: Path,
        output_dir: Path,
        input_dir: Optional[Path],
        omop_ddl_file: Optional[Path],
        omop_config_file: Optional[Path],
        write_mode: str = "w",
        person_file: Optional[Path] = None,
        person_table: Optional[str] = None,
        db_conn_params: Optional[DBConnParams] = None,
    ):
        self.rules_file = rules_file
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.person_file = person_file
        self.person_table = person_table
        self.omop_ddl_file = omop_ddl_file
        self.omop_config_file = omop_config_file
        self.write_mode = write_mode
        self.db_conn_params = db_conn_params

        # Initialize components immediately
        self.initialize_components()

    def initialize_components(self):
        """Initialize all processing components"""
        self.omopcdm = OmopCDM(self.omop_ddl_file, self.omop_config_file)
        self.mappingrules = MappingRules(self.rules_file, self.omopcdm)

        if not self.mappingrules.is_v2_format:
            raise ValueError("Rules file is not in v2 format!")
        else:
            try:
                person_rules_check_v2(
                    self.person_file, self.person_table, self.mappingrules
                )
            except Exception as e:
                logger.exception(f"Validation for person rules failed: {e}")
                raise e

        self.metrics = tools.metrics.Metrics(self.mappingrules.get_dataset_name())
        self.output_manager = OutputFileManager(self.output_dir, self.omopcdm)

        # Pre-compute lookup cache for efficient streaming
        self.lookup_cache = StreamingLookupCache(self.mappingrules, self.omopcdm)

        if self.db_conn_params:
            self.engine_connection = EngineConnection(self.db_conn_params)

    def setup_person_lookup(self) -> Tuple[Dict[str, str], int]:
        """Setup person ID lookup and save mapping"""
        saved_person_id_file = set_saved_person_id_file(None, self.output_dir)
        connection = None
        schema = None
        if self.db_conn_params:
            connection = self.engine_connection.connect()
            schema = self.db_conn_params.schema

        person_lookup, rejected_person_count = load_person_ids_v2(
            saved_person_id_file,
            person_file=self.person_file,
            person_table_name=self.person_table,
            mappingrules=self.mappingrules,
            use_input_person_ids="N",
            db_connection=connection,
            schema=schema,
        )

        # Save person IDs
        with saved_person_id_file.open(mode="w") as fhpout:
            fhpout.write("SOURCE_SUBJECT\tTARGET_SUBJECT\n")
            for person_id, person_assigned_id in person_lookup.items():
                fhpout.write(f"{str(person_id)}\t{str(person_assigned_id)}\n")

        return person_lookup, rejected_person_count

    def execute_processing(self) -> ProcessingResult:
        """Execute the complete processing pipeline with efficient streaming"""

        try:
            # Setup person lookup
            person_lookup, rejected_person_count = self.setup_person_lookup()
            # Log results of person lookup
            logger.info(
                f"person_id stats: total loaded {len(person_lookup)}, reject count {rejected_person_count}"
            )
            # Setup output files - keep all open for streaming
            output_files = self.mappingrules.get_all_outfile_names()
            file_handles, target_column_maps = self.output_manager.setup_output_files(
                output_files, self.write_mode
            )

            # Create processing context
            context = ProcessingContext(
                mappingrules=self.mappingrules,
                omopcdm=self.omopcdm,
                input_dir=self.input_dir,
                person_lookup=person_lookup,
                record_numbers={output_file: 1 for output_file in output_files},
                file_handles=file_handles,
                target_column_maps=target_column_maps,
                metrics=self.metrics,
                db_connection=self.engine_connection.connect()
                if self.db_conn_params
                else None,
                schema=self.db_conn_params.schema if self.db_conn_params else None,
            )

            # Process data using efficient streaming approach
            processor = StreamProcessor(context, self.lookup_cache)
            result = processor.process_all_data()

            for target_file, count in result.output_counts.items():
                logger.info(f"TARGET: {target_file}: output count {count}")

            # Write summary
            data_summary = self.metrics.get_mapstream_summary()
            with (self.output_dir / "summary_mapstream.tsv").open(mode="w") as dsfh:
                dsfh.write(data_summary)

            return result

        finally:
            # Always close files
            if self.output_manager:
                self.output_manager.close_all_files()
