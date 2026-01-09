from dataclasses import dataclass


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
class V2TableMapping:
    source_table: str
    person_id_mapping: PersonIdMapping | None
    date_mapping: DateMapping | None
    concept_mappings: dict[str, ConceptMapping]  # source_field -> ConceptMapping
