from dataclasses import dataclass, field


# To prevent circular import, these types should be in a separate file rather than in the types.py
@dataclass
class PersonIdMapping:
    source_field: str
    dest_field: str


@dataclass
class DateMapping:
    source_field: str
    dest_fields: list[str]


@dataclass
class ConceptMapping:
    source_field: str
    value_mappings: dict[
        str, dict[str, list[int]]
    ]  # value -> dest_field -> concept_ids
    original_value_fields: list[str]


@dataclass
class RecordKey:
    """Source column whose value identifies a written record (see `record_key`)"""

    source_field: str


@dataclass
class Reference:
    """Destination field filled with the id of a record in another table"""

    table: str
    source_field: str


@dataclass
class ValueMapping:
    """
    Fills a destination field with the concept a source column's value maps
    to (e.g. note_type "Outpatient note" -> note_class_concept_id 706556).
    """

    source_field: str
    values: dict[str, int]  # source value -> concept id

    def concept_for(self, value: str) -> int | None:
        return self.values.get(value)


@dataclass
class FactRelationshipLink:
    """
    How to link a derived record to the record it was derived from in
    fact_relationship. Each link is written twice, once in each direction, as
    the CDM conventions require.
    """

    domain_concept_id: int  # domain of the derived record (e.g. Condition)
    source_domain_concept_id: int  # domain of the record it came from
    relationship_concept_id: int  # source record -> derived record
    reverse_relationship_concept_id: int  # derived record -> source record


@dataclass
class DerivedFrom:
    """
    Records of this block are derived from records of `table` built from the
    same source row (e.g. a condition from a note_nlp record): they're only
    written if the row produced such a record and no `exclude_values` match.
    """

    table: str
    # source column -> values (case-insensitive) that stop derived records
    exclude_values: dict[str, list[str]] = field(default_factory=dict)
    fact_relationship: FactRelationshipLink | None = None

    def excludes(self, value_of) -> bool:
        """True if any exclusion column of the row holds an excluded value"""
        for source_field, values in self.exclude_values.items():
            value = value_of(source_field)
            if value is None:
                continue
            if value.strip().lower() in {v.strip().lower() for v in values}:
                return True
        return False


@dataclass
class V2TableMapping:
    source_table: str
    person_id_mapping: PersonIdMapping | None
    date_mapping: DateMapping | None
    concept_mappings: dict[str, ConceptMapping]  # source_field -> ConceptMapping
    # Record-level extensions, see https://carrot.ac.uk/transform/rules_v2_notes_nlp
    field_mappings: dict[str, str] = field(default_factory=dict)  # dest -> source
    constant_mappings: dict[str, str | int] = field(default_factory=dict)
    record_key: RecordKey | None = None
    references: dict[str, Reference] = field(default_factory=dict)  # dest -> ref
    derived_from: DerivedFrom | None = None
    value_mappings: dict[str, ValueMapping] = field(default_factory=dict)  # dest ->

    @property
    def has_record_extensions(self) -> bool:
        # derived_from is left out: it only gates records built by other keys
        return bool(
            self.field_mappings
            or self.constant_mappings
            or self.record_key
            or self.references
            or self.value_mappings
        )

    @property
    def is_row_mapping(self) -> bool:
        """
        Blocks with record-level keys but no concept mappings build one record
        per source row. Blocks with neither still build nothing, as before.
        """
        return not self.concept_mappings and self.has_record_extensions
