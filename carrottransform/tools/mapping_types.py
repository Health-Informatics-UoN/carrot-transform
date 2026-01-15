from pydantic import BaseModel

# To prevent circular import, these types should be in a separate file rather than in the types.py
class PersonIdMapping(BaseModel):
    source_field: str
    dest_field: str


class DateMapping(BaseModel):
    source_field: str
    dest_fields: list[str]


class ConceptMapping(BaseModel):
    source_field: str
    value_mappings: dict[
        str, dict[str, list[int]]
    ]  # value -> dest_field -> concept_ids
    original_value_fields: list[str]


class V2TableMapping(BaseModel):
    source_table: str
    person_id_mapping: PersonIdMapping | None
    date_mapping: DateMapping | None
    concept_mappings: dict[str, ConceptMapping]  # source_field -> ConceptMapping
