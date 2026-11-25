from abc import ABC, abstractmethod
from typing import Set, Tuple

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

    def create_empty_record(self) -> list[str]:
        """Create an empty target record with proper initialization"""
        tgtarray = [""] * len(self.context.tgtcolmap)

        # Initialize numeric fields to 0
        for req_integer in self.context.notnull_numeric_fields:
            if req_integer in self.context.tgtcolmap:
                tgtarray[self.context.tgtcolmap[req_integer]] = "0"

        return tgtarray

    def apply_concept_mapping(self, tgtarray: list[str], concept_combo: dict[str, int]):
        """Apply a single concept combination to target array"""
        for dest_field, concept_id in concept_combo.items():
            if dest_field in self.context.tgtcolmap:
                tgtarray[self.context.tgtcolmap[dest_field]] = str(concept_id)

    def apply_original_value_mappings(
        self, tgtarray: list[str], original_value_fields: list[str], source_value: str
    ):
        """Apply original value mappings (direct field copying)"""
        for dest_field in original_value_fields:
            if dest_field in self.context.tgtcolmap:
                tgtarray[self.context.tgtcolmap[dest_field]] = source_value

    def apply_person_id_mapping(self, tgtarray: list[str]):
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

    def apply_date_mappings(self, tgtarray: list[str]) -> bool:
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
        self, tgtarray: list[str], dest_field: str, source_date: str
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

    def apply_record_extensions(self, tgtarray: list[str]) -> bool:
        """
        Apply the record-level keys of the v2 block (field_mappings,
        constant_mappings, references). Returns False if a reference can't be
        resolved, in which case the record must not be written.
        See https://carrot.ac.uk/transform/rules_v2_notes_nlp.
        """
        mapping = self.context.v2_mapping
        tgtcolmap = self.context.tgtcolmap
        srccolmap = self.context.srccolmap

        for dest_field, source_field in mapping.field_mappings.items():
            if dest_field in tgtcolmap and source_field in srccolmap:
                tgtarray[tgtcolmap[dest_field]] = self.context.srcdata[
                    srccolmap[source_field]
                ]

        for dest_field, value in mapping.constant_mappings.items():
            if dest_field in tgtcolmap:
                tgtarray[tgtcolmap[dest_field]] = str(value)

        for dest_field, lookup in mapping.value_mappings.items():
            if dest_field not in tgtcolmap or lookup.source_field not in srccolmap:
                continue
            value = self.context.srcdata[srccolmap[lookup.source_field]]
            concept_id = lookup.concept_for(value)
            if concept_id is not None:
                tgtarray[tgtcolmap[dest_field]] = str(concept_id)
            elif value:
                # The field keeps its default: 0 (no matching concept) for
                # required concept fields, empty otherwise
                self.context.metrics.increment_key_count(
                    source=self.context.srcfilename,
                    fieldname=lookup.source_field,
                    tablename=self.context.tgtfilename,
                    concept_id="all",
                    additional="",
                    count_type="unmapped_value",
                )

        for dest_field, ref in mapping.references.items():
            if dest_field not in tgtcolmap:
                continue
            record_id = None
            if ref.source_field in srccolmap:
                key = self.context.srcdata[srccolmap[ref.source_field]]
                record_id = self.context.record_keys.get(ref.table, {}).get(key)
            if record_id is None:
                self.context.metrics.increment_key_count(
                    source=self.context.srcfilename,
                    fieldname=ref.source_field,
                    tablename=self.context.tgtfilename,
                    concept_id="all",
                    additional="",
                    count_type="invalid_references",
                )
                return False
            tgtarray[tgtcolmap[dest_field]] = record_id

        return True

    def derived_from_allows(self) -> bool:
        """
        For blocks with `derived_from`: False if the source row's values exclude
        derived records, or the row produced no record in the table they're
        derived from (e.g. its note_nlp record wasn't written). Skipped records
        are counted, not rejected.
        """
        derived = self.context.v2_mapping.derived_from
        if derived is None:
            return True

        srccolmap = self.context.srccolmap

        def value_of(source_field: str) -> str | None:
            if source_field not in srccolmap:
                return None
            return self.context.srcdata[srccolmap[source_field]]

        if derived.excludes(value_of):
            count_type = "derived_excluded"
        elif not self.context.row_records.get(derived.table):
            count_type = "derived_unlinked"
        else:
            return True

        self.context.metrics.increment_key_count(
            source=self.context.srcfilename,
            fieldname=self.context.srcfield,
            tablename=self.context.tgtfilename,
            concept_id="all",
            additional="",
            count_type=count_type,
        )
        return False

    def _remember_row_record(self, output_record: list[str]) -> str | None:
        """
        Note this record against the current source row, so records derived
        from it can be linked. Returns its id (None if the table has none).
        """
        if self.context.auto_num_col is None:
            return None
        tgtcolmap = self.context.tgtcolmap
        record_id = output_record[tgtcolmap[self.context.auto_num_col]]
        concept_col = (
            f"{self.context.tgtfilename}_concept_id"  # e.g. note_nlp_concept_id
        )
        concept = (
            output_record[tgtcolmap[concept_col]] if concept_col in tgtcolmap else ""
        )
        self.context.row_records.setdefault(self.context.tgtfilename, []).append(
            (record_id, concept)
        )
        return record_id

    def _write_fact_relationships(self, output_record: list[str], record_id: str):
        """
        Link a derived record to the record(s) it came from, in both directions.
        It's linked to the source row's records with a matching concept (e.g. a
        note_nlp record with the same concept as the condition), or to all of
        the row's records in that table if none match.
        """
        derived = self.context.v2_mapping.derived_from
        if derived is None or derived.fact_relationship is None:
            return
        link = derived.fact_relationship
        anchors = self.context.row_records.get(derived.table, [])

        concepts = {
            output_record[index]
            for column, index in self.context.tgtcolmap.items()
            if column.endswith("_concept_id")
            and not column.endswith(("_source_concept_id", "_type_concept_id"))
        } - {"", "0"}
        matched = [anchor for anchor in anchors if anchor[1] in concepts] or anchors

        columns = self.context.omopcdm.get_omop_column_list("fact_relationship")
        for anchor_id, _ in matched:
            for row in (
                {
                    "domain_concept_id_1": link.source_domain_concept_id,
                    "fact_id_1": anchor_id,
                    "domain_concept_id_2": link.domain_concept_id,
                    "fact_id_2": record_id,
                    "relationship_concept_id": link.relationship_concept_id,
                },
                {
                    "domain_concept_id_1": link.domain_concept_id,
                    "fact_id_1": record_id,
                    "domain_concept_id_2": link.source_domain_concept_id,
                    "fact_id_2": anchor_id,
                    "relationship_concept_id": link.reverse_relationship_concept_id,
                },
            ):
                self._emit("fact_relationship", [str(row[c]) for c in columns])
                self.context.link_counts["fact_relationship"] = (
                    self.context.link_counts.get("fact_relationship", 0) + 1
                )

    def _register_record_key(self, output_record: list[str]):
        """Remember this record's generated id under its source key, if it has one"""
        record_key = self.context.v2_mapping.record_key
        if record_key is None or self.context.auto_num_col is None:
            return
        if record_key.source_field not in self.context.srccolmap:
            return
        key = self.context.srcdata[self.context.srccolmap[record_key.source_field]]
        keys = self.context.record_keys.setdefault(self.context.tgtfilename, {})
        if key in keys:
            # Keys must be unique per OMOP table; references use the first record
            logger.warning(
                f"Duplicate record key {key!r} for {self.context.tgtfilename} "
                f"in {self.context.srcfilename}"
            )
            return
        keys[key] = output_record[self.context.tgtcolmap[self.context.auto_num_col]]

    def write_record_directly(self, output_record: list[str]) -> bool:
        """Write single record directly to output file with all necessary processing"""
        # Set auto-increment ID
        if self.context.auto_num_col is not None:
            output_record[self.context.tgtcolmap[self.context.auto_num_col]] = str(
                self.context.record_numbers[self.context.tgtfilename]
            )
            self.context.record_numbers[self.context.tgtfilename] += 1

        # Tables without a person_id column (e.g. note_nlp) need no person lookup
        if (
            self.context.person_id_col is None
            or self.context.person_id_col not in self.context.tgtcolmap
        ):
            self._write(output_record)
            return True

        # Map person ID
        person_id = output_record[self.context.tgtcolmap[self.context.person_id_col]]
        if person_id in self.context.person_lookup:
            output_record[self.context.tgtcolmap[self.context.person_id_col]] = (
                self.context.person_lookup[person_id]
            )
            self._write(output_record)
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

    def _write(self, output_record: list[str]):
        """Record metrics, register the record key and write the record"""
        # Update metrics
        self.context.metrics.increment_with_datacol(
            source_path=self.context.srcfilename,
            target_file=self.context.tgtfilename,
            datacol=self.context.srcfield,
            out_record=output_record,
        )

        self._register_record_key(output_record)
        self._emit(self.context.tgtfilename, output_record)

        record_id = self._remember_row_record(output_record)
        if record_id is not None:
            self._write_fact_relationships(output_record, record_id)

    def _emit(self, table: str, output_record: list[str]):
        """Write a record to a table's output file (files are kept open)"""
        into = self.context.file_handles[table]

        # injection needs the records, the-old-ways expect you to tabbify the record for it
        # ... so ... when we do "out with the old" get this https://github.com/Health-Informatics-UoN/carrot-transform/issues/159
        import carrottransform.tools.outputs as outputs

        if isinstance(into, outputs.OutputTarget.Handle):
            into.write(output_record)
        else:
            into.write("\t".join(output_record) + "\n")


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

    def _collect_all_mappings(self) -> Tuple[dict[str, list[int]], dict[str, str]]:
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
        self, concept_combo: dict[str, int], all_original_values: dict[str, str]
    ) -> list[str] | None:
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

        # Derived records (e.g. conditions from NLP) may be skipped for this row
        if not self.derived_from_allows():
            return RecordResult(True, 0, self.context.metrics)

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
        concept_combo: dict[str, int],
        concept_mapping: ConceptMapping,
        source_value: str,
    ) -> list[str] | None:
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

        # Field copies, constants and references
        if not self.apply_record_extensions(tgtarray):
            return None

        return tgtarray


class RowRecordBuilder(TargetRecordBuilder):
    """
    Builder for blocks without concept mappings that use record-level keys:
    exactly one record per source row (e.g. note).
    """

    def build_records(self) -> RecordResult:
        if not self.derived_from_allows():
            return RecordResult(True, 0, self.context.metrics)

        tgtarray = self.create_empty_record()

        self.apply_person_id_mapping(tgtarray)

        if not self.apply_date_mappings(tgtarray):
            logger.warning(
                f"Failed to apply date mappings for {self.context.tgtfilename}"
            )
            return RecordResult(False, 0, self.context.metrics)

        if not self.apply_record_extensions(tgtarray):
            return RecordResult(False, 0, self.context.metrics)

        if self.write_record_directly(tgtarray):
            return RecordResult(True, 1, self.context.metrics)
        return RecordResult(False, 0, self.context.metrics)


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
        elif context.v2_mapping.is_row_mapping:
            return RowRecordBuilder(context)
        else:
            return StandardRecordBuilder(context)

    @classmethod
    def clear_person_cache(cls):
        """Clear the person processed cache (useful for testing or new runs)"""
        cls._person_processed_cache.clear()
