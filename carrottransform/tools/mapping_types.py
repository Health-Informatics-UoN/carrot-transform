from dataclasses import dataclass
from typing import Dict, List, Optional


# To prevent circular import, these types should be in a separate file rather than in the types.py
@dataclass
class PersonIdMapping:
    source_field: str
    dest_field: str


@dataclass
class DateMapping:
    source_field: str
    dest_fields: List[str]


@dataclass
class ConceptMapping:
    source_field: str
    value_mappings: Dict[
        str, Dict[str, List[int]]
    ]  # value -> dest_field -> concept_ids
    original_value_fields: List[str]


@dataclass
class V2TableMapping:
    source_table: str
    person_id_mapping: Optional[PersonIdMapping]
    date_mapping: Optional[DateMapping]
    concept_mappings: Dict[str, ConceptMapping]  # source_field -> ConceptMapping
