from typing import Dict, List, Optional
from carrottransform.tools.types import (
    ProcessingResult,
    ProcessingContext,
    RecordContext,
)
from carrottransform.tools.mapping_types import V2TableMapping
from carrottransform.tools.record_builder import RecordBuilderFactory


class RowProcessor:
    """Handles processing of individual rows across all target tables"""

    def __init__(self, context: ProcessingContext):
        self.context = context

    def process_row_for_all_targets(
        self,
        source_filename: str,
        input_data: List[str],
        input_column_map: Dict[str, int],
        data_fields: Dict[str, List[str]],
    ) -> ProcessingResult:
        """Process a single row for all target tables"""
        output_counts = {outfile: 0 for outfile in self.context.output_files}
        rejected_id_counts = {source_filename: 0}

        for target_file in self.context.output_files:
            if not self._has_mapping_for_target(target_file, source_filename):
                continue

            target_result = self._process_row_for_target(
                source_filename, input_data, input_column_map, target_file, data_fields
            )

            output_counts[target_file] += target_result.output_counts.get(
                target_file, 0
            )
            rejected_id_counts[source_filename] += target_result.rejected_id_counts.get(
                source_filename, 0
            )

        return ProcessingResult(output_counts, rejected_id_counts)

    def _has_mapping_for_target(self, target_file: str, source_filename: str) -> bool:
        """Check if there's a mapping for this target file and source file combination"""
        return (
            target_file in self.context.mappingrules.v2_mappings
            and source_filename in self.context.mappingrules.v2_mappings[target_file]
        )

    def _process_row_for_target(
        self,
        source_filename: str,
        input_data: List[str],
        input_column_map: Dict[str, int],
        target_file: str,
        data_fields: Dict[str, List[str]],
    ) -> ProcessingResult:
        """Process a single row for a specific target table"""
        v2_mapping = self.context.mappingrules.v2_mappings[target_file][source_filename]
        target_column_map = self.context.target_column_maps[target_file]

        # Get metadata for this target
        auto_num_col = self.context.omopcdm.get_omop_auto_number_field(target_file)
        person_id_col = self.context.omopcdm.get_omop_person_id_field(target_file)

        data_columns = data_fields.get(target_file, [])

        output_count = 0
        rejected_count = 0

        # Process each data column
        for data_column in data_columns:
            if data_column not in input_column_map:
                continue

            column_result = self._process_data_column(
                source_filename,
                input_data,
                input_column_map,
                target_file,
                v2_mapping,
                target_column_map,
                data_column,
                auto_num_col,
                person_id_col,
            )

            output_count += column_result.output_counts.get(target_file, 0)
            rejected_count += column_result.rejected_id_counts.get(source_filename, 0)

        return ProcessingResult(
            {target_file: output_count}, {source_filename: rejected_count}
        )

    def _process_data_column(
        self,
        source_filename: str,
        input_data: List[str],
        input_column_map: Dict[str, int],
        target_file: str,
        v2_mapping: V2TableMapping,
        target_column_map: Dict[str, int],
        data_column: str,
        auto_num_col: Optional[str],
        person_id_col: str,
    ) -> ProcessingResult:
        """Process a single data column"""

        # Create context object
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
        )

        # Create appropriate builder and build records
        builder = RecordBuilderFactory.create_builder(context)
        result = builder.build_records()

        # Update metrics
        self.context.metrics = result.metrics

        if not result.build_records:
            return ProcessingResult({}, {})

        output_count = 0
        rejected_count = 0

        # Process each output record
        for output_record in result.records:
            record_result = self._process_output_record(
                output_record,
                target_file,
                target_column_map,
                auto_num_col,
                person_id_col,
                source_filename,
                data_column,
            )

            if record_result.success:
                output_count += 1
            else:
                rejected_count += 1

        return ProcessingResult(
            {target_file: output_count}, {source_filename: rejected_count}
        )

    def _process_output_record(
        self,
        output_record: List[str],
        target_file: str,
        target_column_map: Dict[str, int],
        auto_num_col: Optional[str],
        person_id_col: str,
        source_filename: str,
        data_column: str,
    ) -> ProcessingResult:
        """Process a single output record"""
        # Set auto-increment ID
        if auto_num_col is not None:
            output_record[target_column_map[auto_num_col]] = str(
                self.context.record_numbers[target_file]
            )
            self.context.record_numbers[target_file] += 1

        # Map person ID
        person_id = output_record[target_column_map[person_id_col]]
        if person_id in self.context.person_lookup:
            output_record[target_column_map[person_id_col]] = (
                self.context.person_lookup[person_id]
            )

            # Update metrics
            self.context.metrics.increment_with_datacol(
                source_path=source_filename,
                target_file=target_file,
                datacol=data_column,
                out_record=output_record,
            )

            # Write to output file
            self.context.file_handles[target_file].write(
                "\t".join(output_record) + "\n"
            )

            return ProcessingResult({target_file: 1}, {})
        else:
            # Invalid person ID
            self.context.metrics.increment_key_count(
                source=source_filename,
                fieldname="all",
                tablename=target_file,
                concept_id="all",
                additional="",
                count_type="invalid_person_ids",
            )
            return ProcessingResult({}, {source_filename: 1})
