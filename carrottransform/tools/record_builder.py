from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Set, Tuple

from carrottransform.tools.concept_helpers import (
    generate_combinations,
    get_value_mapping,
)
from carrottransform.tools.date_helpers import get_datetime_value
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.mapping_types import ConceptMapping
from carrottransform.tools.types import RecordContext, RecordResult
from carrottransform.tools.validation import valid_value

logger = logger_setup()


class TargetRecordBuilder(ABC):
    """Base class for building target records"""

    def __init__(self, context: RecordContext):
        self.context = context

    @abstractmethod
    def build_records(self) -> RecordResult:
        """Build target records - must be implemented by subclasses"""
        pass

    def create_empty_record(self) -> List[str]:
        """Create an empty target record with proper initialization"""
        tgtarray = [""] * len(self.context.tgtcolmap)

        # Initialize numeric fields to 0
        for req_integer in self.context.notnull_numeric_fields:
            if req_integer in self.context.tgtcolmap:
                tgtarray[self.context.tgtcolmap[req_integer]] = "0"

        return tgtarray

    def apply_concept_mapping(self, tgtarray: List[str], concept_combo: Dict[str, int]):
        """Apply a single concept combination to target array"""
        for dest_field, concept_id in concept_combo.items():
            if dest_field in self.context.tgtcolmap:
                tgtarray[self.context.tgtcolmap[dest_field]] = str(concept_id)

    def apply_original_value_mappings(
        self, tgtarray: List[str], original_value_fields: List[str], source_value: str
    ):
        """Apply original value mappings (direct field copying)"""
        for dest_field in original_value_fields:
            if dest_field in self.context.tgtcolmap:
                tgtarray[self.context.tgtcolmap[dest_field]] = source_value

    def apply_person_id_mapping(self, tgtarray: List[str]):
        """Apply person ID mapping"""
        if not self.context.v2_mapping.person_id_mapping:
            return

        person_id_mapping = self.context.v2_mapping.person_id_mapping
        if (
            person_id_mapping.dest_field in self.context.tgtcolmap
            and person_id_mapping.source_field in self.context.srccolmap
        ):
            person_id = self.context.srcdata[
                self.context.srccolmap[person_id_mapping.source_field]
            ]
            tgtarray[self.context.tgtcolmap[person_id_mapping.dest_field]] = person_id

    def apply_date_mappings(self, tgtarray: List[str]) -> bool:
        """Apply date mappings with proper error handling"""
        if not self.context.v2_mapping.date_mapping:
            return True

        date_mapping = self.context.v2_mapping.date_mapping

        if date_mapping.source_field not in self.context.srccolmap:
            logger.warning(
                f"Date mapping source field not found in source data: {date_mapping.source_field}"
            )
            return True

        source_date = self.context.srcdata[
            self.context.srccolmap[date_mapping.source_field]
        ]

        for dest_field in date_mapping.dest_fields:
            if dest_field in self.context.tgtcolmap:
                if not self._apply_single_date_field(tgtarray, dest_field, source_date):
                    return False

        return True

    def _apply_single_date_field(
        self, tgtarray: List[str], dest_field: str, source_date: str
    ) -> bool:
        """Apply a single date field mapping"""
        # Handle date component fields (birth dates with year/month/day)
        if dest_field in self.context.date_component_data:
            dt = get_datetime_value(source_date.split(" ")[0])
            if dt is None:
                self.context.metrics.increment_key_count(
                    source=self.context.srcfilename,
                    fieldname=self.context.srcfield,
                    tablename=self.context.tgtfilename,
                    concept_id="all",
                    additional="",
                    count_type="invalid_date_fields",
                )
                logger.warning(f"Invalid date fields: {self.context.srcfield}")
                return False

            # Set individual date components
            component_info = self.context.date_component_data[dest_field]
            if (
                "year" in component_info
                and component_info["year"] in self.context.tgtcolmap
            ):
                tgtarray[self.context.tgtcolmap[component_info["year"]]] = str(dt.year)
            if (
                "month" in component_info
                and component_info["month"] in self.context.tgtcolmap
            ):
                tgtarray[self.context.tgtcolmap[component_info["month"]]] = str(
                    dt.month
                )
            if (
                "day" in component_info
                and component_info["day"] in self.context.tgtcolmap
            ):
                tgtarray[self.context.tgtcolmap[component_info["day"]]] = str(dt.day)

            # Set the main date field
            tgtarray[self.context.tgtcolmap[dest_field]] = source_date

        # Handle regular date fields with linked date-only fields
        elif dest_field in self.context.date_col_data:
            tgtarray[self.context.tgtcolmap[dest_field]] = source_date
            # Set the linked date-only field
            if self.context.date_col_data[dest_field] in self.context.tgtcolmap:
                tgtarray[
                    self.context.tgtcolmap[self.context.date_col_data[dest_field]]
                ] = source_date[:10]

        # Handle simple date fields
        else:
            tgtarray[self.context.tgtcolmap[dest_field]] = source_date

        return True

    def write_record_directly(self, output_record: List[str]) -> bool:
        """Write single record directly to output file with all necessary processing"""
        # Set auto-increment ID
        if self.context.auto_num_col is not None:
            output_record[self.context.tgtcolmap[self.context.auto_num_col]] = str(
                self.context.record_numbers[self.context.tgtfilename]
            )
            self.context.record_numbers[self.context.tgtfilename] += 1

        # Map person ID
        person_id = output_record[self.context.tgtcolmap[self.context.person_id_col]]
        if person_id in self.context.person_lookup:
            output_record[self.context.tgtcolmap[self.context.person_id_col]] = (
                self.context.person_lookup[person_id]
            )

            # Update metrics
            self.context.metrics.increment_with_datacol(
                source_path=self.context.srcfilename,
                target_file=self.context.tgtfilename,
                datacol=self.context.srcfield,
                out_record=output_record,
            )

            # Write directly to output file (files are kept open)
            self.context.file_handles[self.context.tgtfilename].write(
                "\t".join(output_record) + "\n"
            )

            return True
        else:
            # Invalid person ID
            self.context.metrics.increment_key_count(
                source=self.context.srcfilename,
                fieldname="all",
                tablename=self.context.tgtfilename,
                concept_id="all",
                additional="",
                count_type="invalid_person_ids",
            )
            return False


class PersonRecordBuilder(TargetRecordBuilder):
    """Specialized builder for person table records"""

    def __init__(self, context: RecordContext):
        super().__init__(context)
        self.processed_cache: Set[str] = set()

    def build_records(self) -> RecordResult:
        """Build person table records with special merging logic"""
        # Check if person ID mapping exists
        if not self.context.v2_mapping.person_id_mapping:
            return RecordResult(False, 0, self.context.metrics)

        # Create a unique key for this source row
        person_key = f"{self.context.srcfilename}:{self.context.srcdata[self.context.srccolmap[self.context.v2_mapping.person_id_mapping.source_field]]}"

        # Only process if we haven't already processed this person record
        if person_key in self.processed_cache:
            return RecordResult(False, 0, self.context.metrics)

        # Mark this person as processed
        self.processed_cache.add(person_key)

        # Collect all mappings from all fields
        all_concept_mappings, all_original_values = self._collect_all_mappings()

        # If no valid mappings found, return empty
        if not all_concept_mappings and not all_original_values:
            return RecordResult(False, 0, self.context.metrics)

        # Generate combined concept combinations
        concept_combinations = (
            generate_combinations(all_concept_mappings)
            if all_concept_mappings
            else [{}]
        )

        # Create person records for each combination
        record_count = 0
        for concept_combo in concept_combinations:
            record = self._build_single_person_record(
                concept_combo, all_original_values
            )
            if record:
                # Write record directly using the built-in method
                if self.write_record_directly(record):
                    record_count += 1

        return RecordResult(record_count > 0, record_count, self.context.metrics)

    def _collect_all_mappings(self) -> Tuple[Dict[str, List[int]], Dict[str, str]]:
        """Collect all concept mappings and original values from all fields"""
        all_concept_mappings = {}
        all_original_values = {}

        for (
            field_name,
            concept_mapping,
        ) in self.context.v2_mapping.concept_mappings.items():
            if field_name not in self.context.srccolmap:
                continue

            # Check if field has valid value
            source_value = str(self.context.srcdata[self.context.srccolmap[field_name]])
            if not valid_value(source_value):
                continue

            # Get value mapping for this field
            value_mapping = get_value_mapping(concept_mapping, source_value)

            if value_mapping:
                # Add this field's mappings to the combined mappings
                for dest_field, concept_ids in value_mapping.items():
                    all_concept_mappings[dest_field] = concept_ids

            # Collect original value mappings
            if concept_mapping.original_value_fields:
                for dest_field in concept_mapping.original_value_fields:
                    all_original_values[dest_field] = source_value

        return all_concept_mappings, all_original_values

    def _build_single_person_record(
        self, concept_combo: Dict[str, int], all_original_values: Dict[str, str]
    ) -> Optional[List[str]]:
        """Build a single person record"""
        tgtarray = self.create_empty_record()

        # Apply the merged concept combination
        self.apply_concept_mapping(tgtarray, concept_combo)

        # Handle original value fields (direct field copying)
        for dest_field, source_value in all_original_values.items():
            if dest_field in self.context.tgtcolmap:
                tgtarray[self.context.tgtcolmap[dest_field]] = source_value

        # Handle person ID mapping
        self.apply_person_id_mapping(tgtarray)

        # Handle date mappings
        if not self.apply_date_mappings(tgtarray):
            logger.warning("Failed to apply date mappings for person table")
            return None

        return tgtarray


class StandardRecordBuilder(TargetRecordBuilder):
    """Builder for standard (non-person) table records"""

    def build_records(self) -> RecordResult:
        """Build standard table records"""
        # Check if source field has a value
        if not valid_value(
            str(self.context.srcdata[self.context.srccolmap[self.context.srcfield]])
        ):
            self.context.metrics.increment_key_count(
                source=self.context.srcfilename,
                fieldname=self.context.srcfield,
                tablename=self.context.tgtfilename,
                concept_id="all",
                additional="",
                count_type="invalid_source_fields",
            )
            return RecordResult(False, 0, self.context.metrics)

        # Check if we have a concept mapping for this field
        if self.context.srcfield not in self.context.v2_mapping.concept_mappings:
            return RecordResult(False, 0, self.context.metrics)

        concept_mapping = self.context.v2_mapping.concept_mappings[
            self.context.srcfield
        ]
        source_value = str(
            self.context.srcdata[self.context.srccolmap[self.context.srcfield]]
        )

        # Get value mapping (concept mappings or wildcard)
        value_mapping = get_value_mapping(concept_mapping, source_value)

        # Only proceed if we have concept mappings OR original value fields
        if not value_mapping and not concept_mapping.original_value_fields:
            return RecordResult(False, 0, self.context.metrics)

        # Generate all concept combinations
        concept_combinations = generate_combinations(value_mapping)

        # If no concept combinations, don't build records
        if not concept_combinations:
            return RecordResult(False, 0, self.context.metrics)

        # Create records for each concept combination
        record_count = 0
        for concept_combo in concept_combinations:
            record = self._build_single_standard_record(
                concept_combo, concept_mapping, source_value
            )
            if record:
                # Write record directly using the built-in method
                if self.write_record_directly(record):
                    record_count += 1
                else:
                    # If write fails, return failure
                    return RecordResult(False, 0, self.context.metrics)
            else:
                # If any record fails, return failure
                return RecordResult(False, 0, self.context.metrics)

        return RecordResult(record_count > 0, record_count, self.context.metrics)

    def _build_single_standard_record(
        self,
        concept_combo: Dict[str, int],
        concept_mapping: ConceptMapping,
        source_value: str,
    ) -> Optional[List[str]]:
        """Build a single standard record"""
        tgtarray = self.create_empty_record()

        # Apply this specific concept combination
        self.apply_concept_mapping(tgtarray, concept_combo)

        # Handle original value fields (direct field copying)
        if concept_mapping.original_value_fields:
            self.apply_original_value_mappings(
                tgtarray, concept_mapping.original_value_fields, source_value
            )

        # Handle person ID mapping
        self.apply_person_id_mapping(tgtarray)

        # Handle date mappings
        if not self.apply_date_mappings(tgtarray):
            logger.warning(f"Failed to apply date mappings for {self.context.srcfield}")
            return None

        return tgtarray


class RecordBuilderFactory:
    """Factory for creating appropriate record builders"""

    # Class-level cache for person records
    _person_processed_cache: Set[str] = set()

    @classmethod
    def create_builder(cls, context: RecordContext) -> TargetRecordBuilder:
        """Create the appropriate record builder based on table type"""
        if context.tgtfilename == "person":
            builder = PersonRecordBuilder(context)
            # Share the class-level cache across all person builders
            builder.processed_cache = cls._person_processed_cache
            return builder
        else:
            return StandardRecordBuilder(context)

    @classmethod
    def clear_person_cache(cls):
        """Clear the person processed cache (useful for testing or new runs)"""
        cls._person_processed_cache.clear()
