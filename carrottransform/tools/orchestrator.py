from collections.abc import Iterator
from pathlib import Path
from typing import Any, Set, Tuple

from case_insensitive_dict import CaseInsensitiveDict

import carrottransform.tools as tools
from carrottransform import require
from carrottransform.tools import args, outputs, person_helpers, sources
from carrottransform.tools.args import person_rules_check_v2, remove_csv_extension
from carrottransform.tools.date_helpers import normalise_to8601
from carrottransform.tools.file_helpers import OutputFileManager
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.record_builder import RecordBuilderFactory
from carrottransform.tools.stream_helpers import StreamingLookupCache
from carrottransform.tools.types import (
    ProcessingContext,
    ProcessingResult,
    RecordContext,
)

logger = logger_setup()


class StreamProcessor:
    """Efficient single-pass streaming processor"""

    def __init__(
        self,
        context: ProcessingContext,
        lookup_cache: StreamingLookupCache,
        source: sources.SourceObject,
    ):
        self.context = context
        self.cache = lookup_cache
        self._source = source

    def process_all_data(self) -> ProcessingResult:
        """Process all data with single-pass streaming approach"""
        logger.info("Processing data...")
        total_output_counts = {outfile: 0 for outfile in self.context.output_files}
        total_rejected_counts = {infile: 0 for infile in self.context.input_files}

        # Process each input file
        for source_filename in self.context.input_files:
            try:
                output_counts, rejected_count = self._process_input_file_stream(
                    source_filename
                )

                # Update totals
                for target_file, count in output_counts.items():
                    total_output_counts[target_file] += count
                total_rejected_counts[source_filename] = rejected_count

            except Exception as e:
                logger.error(f"Error processing file {source_filename}: {str(e)}")
                raise

        return ProcessingResult(total_output_counts, total_rejected_counts)

    def source_open(self, source_filename: str) -> Iterator[list[str]]:
        return self._source.open(remove_csv_extension(source_filename))

    def _process_input_file_stream(
        self,
        source_filename: str,
    ) -> Tuple[dict[str, int], int]:
        """Stream process a single input file with direct output writing"""

        logger.info(f"Streaming input file: {source_filename}")

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
            source = self.source_open(source_filename)
            column_headers = next(source)
            input_column_map = self.context.omopcdm.get_column_map(column_headers)

            # Validate required columns exist
            datetime_col_idx = input_column_map.get(file_meta["datetime_source"])
            if datetime_col_idx is None:
                logger.warning(
                    f"Date field {file_meta['datetime_source']} not found in {source_filename}"
                )
                return output_counts, rejected_count

            # Stream process each row
            for input_data in source:
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

        except Exception as e:
            logger.error(f"Error streaming file {source_filename}: {str(e)}")
            raise

        return output_counts, rejected_count

    def _process_single_row_stream(
        self,
        source_filename: str,
        input_data: list[str],
        input_column_map: CaseInsensitiveDict[str, int],
        applicable_targets: Set[str],
        datetime_col_idx: int,
        file_meta: dict[str, Any],
    ) -> Tuple[dict[str, int], int]:
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
        input_data: list[str],
        input_column_map: CaseInsensitiveDict[str, int],
        target_file: str,
        file_meta: dict[str, Any],
    ) -> Tuple[int, int]:
        """Process row for specific target and write records directly"""

        v2_mapping = self.context.mappingrules.v2_mappings[target_file][source_filename]
        target_column_map: CaseInsensitiveDict[str, int] = (
            self.context.target_column_maps[target_file]
        )

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
        input_data: list[str],
        input_column_map: CaseInsensitiveDict[str, int],
        target_file: str,
        v2_mapping,
        target_column_map: CaseInsensitiveDict[str, int],
        data_column: str,
        auto_num_col: str | None,
        person_id_col: str,
        date_col_data: dict[str, str],
        date_component_data: dict[str, dict[str, str]],
        notnull_numeric_fields: list[str],
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
        output: outputs.OutputTarget,
        inputs: sources.SourceObject,
        person: str,
        omop_ddl_file: Path,
        omop_config_file: Path,
        write_mode: str,
    ):
        self.rules_file = rules_file
        self._output = output
        self._inputs = inputs
        self._person = person
        self.omop_ddl_file = omop_ddl_file
        self.omop_config_file = omop_config_file
        self.write_mode = write_mode

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
                args.person_rules_check_v2_injected(
                    self._person, self.mappingrules, sources=self._inputs
                )
                person_rules_check_v2(
                    person_file=None,
                    person_table=self._person,
                    mappingrules=self.mappingrules,
                )
            except Exception as e:
                logger.exception(f"Validation for person rules failed: {e}")
                raise

        self.metrics = tools.metrics.Metrics(self.mappingrules.get_dataset_name())
        self.output_manager = OutputFileManager(self._output, self.omopcdm)

        # Pre-compute lookup cache for efficient streaming
        self.lookup_cache = StreamingLookupCache(self.mappingrules, self.omopcdm)

        self.engine_connection = None

    def setup_person_lookup(self) -> Tuple[dict[str, str], int]:
        """Setup person ID lookup and save mapping"""

        person_lookup, rejected_person_count = person_helpers.load_person_ids_v2_inject(
            mappingrules=self.mappingrules, inputs=self._inputs, person=self._person
        )

        # now save the IDs
        id_out = self._output.start("person_ids", ["SOURCE_SUBJECT", "TARGET_SUBJECT"])

        for person_source_id, person_assigned_id in person_lookup.items():
            id_out.write([person_source_id, person_assigned_id])

        id_out.close()

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
            target_column_maps = {}
            file_handles = {}
            for output_name in output_files:
                output_header = self.omopcdm.get_omop_column_list(output_name)
                target_column_map = self.omopcdm.get_omop_column_map(output_name)

                if output_header is None:
                    raise Exception(f"need columns for {output_name=}")
                if target_column_map is None:
                    raise Exception(f"need column map for {output_name=}")

                file_handles[output_name] = self._output.start(
                    output_name, output_header
                )
                target_column_maps[output_name] = target_column_map

            # Create processing context
            context = ProcessingContext(
                mappingrules=self.mappingrules,
                omopcdm=self.omopcdm,
                inputs=self._inputs,
                person_lookup=person_lookup,
                record_numbers={output_file: 1 for output_file in output_files},
                file_handles=file_handles,
                target_column_maps=target_column_maps,
                metrics=self.metrics,
            )

            # Process data using efficient streaming approach
            processor = StreamProcessor(context, self.lookup_cache, self._inputs)
            result = processor.process_all_data()

            for target_file, count in result.output_counts.items():
                logger.info(f"TARGET: {target_file}: output count {count}")

            # Write summary
            data_summary = None
            for line in self.metrics.get_mapstream_summary().strip().split("\n"):
                row = line.split("\t")

                if data_summary is None:
                    data_summary = self._output.start("summary_mapstream", row)
                else:
                    data_summary.write(row)

            require(data_summary is not None)
            if data_summary is not None:
                data_summary.close()
                data_summary = None

            return result

        finally:
            # Always close files
            if self.output_manager:
                self.output_manager.close_all_files()
