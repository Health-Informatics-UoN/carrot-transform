from datetime import datetime
from typing import Literal
from pydantic import BaseModel
import json

class RuleSetMetadata(BaseModel):
    date_created: datetime
    dataset: str
    # why doesn't the metadata have a "v2" flag to make parsing simpler?

# To prevent circular import, these types should be in a separate file rather than in the types.py
class PersonIdMapping(BaseModel):
    source_field: str
    dest_field: str

class DateMapping(BaseModel):
    source_field: str
    dest_fields: list[str]

class ConceptMapping(BaseModel):
    source_field: str
    # if I had my druthers, this would be better as {value: somevalue, dest_field: somefield, concept_ids: list[int]} triples
    value_mappings: dict[
        str, dict[str, list[int]]
    ]  # value -> dest_field -> concept_ids
    original_value: list[str]

class V1CDMField(BaseModel):
    source_table: str
    source_field: str
    term_mapping = dict[str, int] | int | None
    

class V2TableMapping(BaseModel):
    person_id_mapping: PersonIdMapping | None
    date_mapping: DateMapping | None
    concept_mappings: dict[str, ConceptMapping]  # source_field -> ConceptMapping

class V2RuleSet(BaseModel):
    metadata: RuleSetMetadata | None
    # an array of values with a destination_table field would be easier to parse
    cdm: dict[Literal["observation", "measurement", "person", "condition_occurrence"], dict[str, V2TableMapping]]
    
    def dump_parsed_rules(self) -> str:
        return json.dumps(self.cdm)

    def get_dsname_from_rules(self) -> str:
        if self.metadata is None:
            return "Unknown"
        else:
            return self.metadata.dataset

    def get_all_outfile_names(self) -> list[str]:
        return list(self.cdm.keys())

    def get_all_infile_names(self) -> list[str]:
        keys = [mapping.keys() for mapping in self.cdm.values()]
        return list(set(*keys))

    def get_infile_data_fields(self, infilename: str) -> dict[str, list[str]]:
        ...

    def get_infile_date_person_id(self, infilename: str) -> tuple[str,str]:
        ...

    def get_person_source_field_info(self, tgtfilename: str) -> tuple[Optional[str], Optional[str]]:
        ...

    def parse_rules_src_to_tgt(self, infilename: str) -> tuple[list[str], dict[Any]]:
        # Aargh what is this
        ...

    def process_rules(self, infilename, outfilename, rules):
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

    def get_all_outfile_names(self) -> list[str]:
        ...

    def get_all_infile_names(self) -> list[str]:
        ...

    def get_infile_data_fields(self, infilename: str) -> dict[str, list[str]]:
        ...

    def get_infile_date_person_id(self, infilename: str) -> tuple[str,str]:
        ...

    def get_person_source_field_info(self, tgtfilename: str) -> tuple[Optional[str], Optional[str]]:
        ...

    def parse_rules_src_to_tgt(self, infilename: str) -> tuple[list[str], dict[Any]]:
        # Aargh what is this
        ...

    def process_rules(self, infilename, outfilename, rules):
        ...
