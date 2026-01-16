from datetime import datetime
from typing import Literal, Any
from pydantic import BaseModel, Field, model_validator
import json

term_mapping = dict[str, int] | int

class RuleSetMetadata(BaseModel):
    date_created: datetime
    dataset: str
    # why doesn't the metadata have a "v2" flag to make parsing simpler?

class V1CDMField(BaseModel):
    source_table: str
    source_field: str
    term_mapping: term_mapping | None

# To prevent circular import, these types should be in a separate file rather than in the types.py
class PersonIdMapping(BaseModel):
    source_field: str
    dest_field: str


class DateMapping(BaseModel):
    source_field: str
    dest_field: list[str]

class ConceptMapping(BaseModel):
    value_mappings: dict[str, dict[str, list[int]]]  # value -> dest_field -> concept_ids
    original_value: list[str]

    @model_validator(mode='before')
    @classmethod
    def wrap_value_mappings(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        
        original_value = data.pop("original_value", [])
        value_mappings = data
        
        return {
            "value_mappings": value_mappings,
            "original_value": original_value
        }
    

class V2TableMapping(BaseModel):
    person_id_mapping: PersonIdMapping | None = None
    date_mapping: DateMapping | None = None
    concept_mappings: dict[str, ConceptMapping] = Field(default_factory=dict)

class MappingRule(BaseModel):
    """
    Representation of how source fields map to destination fields.
    Both v1 and v2 rulesets end in producing one of these
    """
    source_table: str
    source_field: str
    term_mapping: term_mapping | None
    omop_table: Literal["observation", "measurement", "person", "condition_occurrence"]
    destination_fields: dict[str, list[int]] | None
    value: str | None = None

class MappingIndex(BaseModel):
    """
    Collection of `MappingRule`s
    """
    metadata: RuleSetMetadata | None = None
    mappings: list[MappingRule] = Field(default_factory=list)

    def by_source_table(self, source_table_name: str) -> list[MappingRule]:
        return [m for m in self.mappings if m.source_table == source_table_name]

    def by_omop_table(self, omop_table_name: str) -> list[MappingRule]:
        return [m for m in self.mappings if m.omop_table == omop_table_name]

    def by_source_and_omop(self, source_table_name: str, omop_table_name: str) -> list[MappingRule]:
        return [
                m for m in self.mappings
                if m.source_table == source_table_name and m.omop_table == omop_table_name
                ]
    
    def get_source_tables(self) -> list[str]:
        return list(set(m.source_table for m in self.mappings))

    def get_omop_tables(self) -> list[str]:
        return list(set(m.omop_table for m in self.mappings))


class V2RuleSet(BaseModel):
    metadata: RuleSetMetadata | None
    # an array of values with a destination_table field would be easier to parse
    cdm: dict[Literal["observation", "measurement", "person", "condition_occurrence"], dict[str, V2TableMapping]]
    
    def dump_parsed_rules(self) -> str:
        return json.dumps(self.model_dump()['cdm'])

    def get_dsname_from_rules(self) -> str:
        if self.metadata is None:
            return "Unknown"
        else:
            return self.metadata.dataset

    def get_all_outfile_names(self) -> list[str]:
        return list(self.cdm.keys())

    def get_all_infile_names(self) -> list[str]:
        return list(set(
            [key for mapping in self.cdm.values() for key in mapping]
            ))

    def to_mapping_index(self) -> MappingIndex:
        ...

class V1RuleSet(BaseModel):
    metadata: RuleSetMetadata | None
    cdm: dict[Literal["observation", "measurement", "person", "condition_occurrence"], dict[str, V1CDMField]]

    def dump_parsed_rules(self) -> str:
        return json.dumps(self.cdm)

    def get_dsname_from_rules(self) -> str:
        if self.metadata is None:
            return "Unknown"
        else:
            return self.metadata.dataset

    def to_mapping_index(self) -> MappingIndex:
        ...

RuleSet = V1RuleSet | V2RuleSet
