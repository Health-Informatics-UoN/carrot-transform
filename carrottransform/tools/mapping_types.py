from datetime import datetime
from typing import Callable, Literal, Any, Protocol
from pydantic import BaseModel, Field, model_validator
import json


class RuleSet(Protocol):
    cdm: dict[str, Any]

    def is_v2_format(self) -> bool:
        ...

    def dump_parsed_rules(self) -> str:
        ...

    def get_dsname_from_rules(self) -> str:
        ...

    def get_all_outfile_names(self) -> list[str]:
        ...

    def get_all_infile_names(self) -> list[str]:
        ...

    def get_infile_data_fields(self, infilename: str) -> dict[str, list[str]]:
        ...
    def get_infile_date_person_id(
            self,
            infilename: str,
            datetime_source: Callable,
            person_id_source: Callable,
            ) -> tuple[str, str]:
        ...

    def get_person_source_field_info(self, tgtfilename: str) -> tuple[str | None, str | None]:
        ...

    def parse_rules_src_to_tgt(self, infilename) -> tuple[list, dict]:
        ...


class RuleSetMetadata(BaseModel):
    """Model for the metadata of a ruleset"""
    date_created: datetime | None
    dataset: str
    # why doesn't the metadata have a "v2" flag to make parsing simpler?

class V1CDMField(BaseModel):
    """Model for a CDM field for the V1 schema"""
    source_table: str
    source_field: str
    term_mapping: dict[str, int] | int | None = None

# To prevent circular import, these types should be in a separate file rather than in the types.py
class PersonIdMapping(BaseModel):
    """Model for a person ID mapping in a v2 schema"""
    source_field: str
    dest_field: str


class DateMapping(BaseModel):
    """Model for a date mapping in a v2 schema"""
    source_field: str
    dest_field: list[str]

class ConceptMapping(BaseModel):
    """
    Model for a concept mapping in a v2 schema

    In the V2 schema, concept mappings have dynamic keys, hence the "before" mode model validator
    """
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
    """
    Model for a source table mapping in the V2 schema.

    The concept mappings are a dictionary where the keys are a source field?
    """
    person_id_mapping: PersonIdMapping | None = None
    date_mapping: DateMapping | None = None
    concept_mappings: dict[str, ConceptMapping] = Field(default_factory=dict)

    @property
    def data_fields(self) -> list[str]:
        """
        A list of the source fields for this source table's concept mappings
        This could also be left as a set, but I'm matching what's already here
        """
        return list(self.concept_mappings.keys())

    @property
    def person_field(self) -> str | None:
        """The source field for the source table's person_id_mapping"""
        if self.person_id_mapping:
            return self.person_id_mapping.source_field

    @property
    def date_field(self) -> str | None:
        """The source field for the source table's date_mapping"""
        if self.date_mapping:
            return self.date_mapping.source_field

class V2RuleSet(BaseModel):
    metadata: RuleSetMetadata | None
    cdm: dict[Literal["observation", "measurement", "person", "condition_occurrence"], dict[str, V2TableMapping]]
    
    def is_v2_format(self) -> bool:
        return True

    def dump_parsed_rules(self) -> str:
        """
        Dump the rules as parsed

        Returns
        -------
        str
            json for the parsed rules
        """
        return json.dumps(self.model_dump()['cdm'])

    def get_dsname_from_rules(self) -> str:
        """
        Get the name of the dataset from the rules
        """
        if self.metadata is None:
            return "Unknown"
        else:
            return self.metadata.dataset

    def get_all_outfile_names(self) -> list[str]:
        """
        Returns a list of the OMOP tables mapped to by these rules

        Returns
        -------
        list[str]
            A list of the OMOP table names
        """
        return list(self.cdm.keys())

    def get_all_infile_names(self) -> list[str]:
        """
        List the source tables used for mapping

        Returns
        -------
        list[str]
            A list of the source table names
        """
        return list({key for mapping in self.cdm.values() for key in mapping})

    def get_infile_data_fields(self, infilename: str) -> dict[str, list[str]]:
        """
        Get data fields for a specific input file

        Parameters
        ----------
        infilename: str
            The name of a source table

        Returns
        -------
        dict[str, list[str]]
            A dictionary of a list of fields for each OMOP table
        """
        return {
                omop_table: table_mapping[infilename].data_fields
                for (omop_table, table_mapping)
                in self.cdm.items()
                if table_mapping.get(infilename)
                }

    def get_infile_person_id(self, infilename:str) -> str:
        """Get the person_id source field"""
        for omop_mapping in self.cdm.values():
            if infilename in omop_mapping:
                if omop_mapping[infilename].person_id_mapping:
                    return omop_mapping[infilename].person_field
        return ""

    def get_infile_date_id(self, infilename:str) -> str:
        """Get the datetime source field"""
        for omop_mapping in self.cdm.values():
            if infilename in omop_mapping:
                if omop_mapping[infilename].date_mapping:
                    return omop_mapping[infilename].date_field
        return ""

    def get_infile_date_person_id(
            self,
            infilename: str,
            datetime_source: Callable,
            person_id_source: Callable,
            ) -> tuple[str, str]:
        """Get datetime and person_id source fields"""
        # I don't understand why you want this
        # also do you really want it to default to ""?
        person_id = self.get_infile_person_id(infilename)
        datetime = self.get_infile_date_id(infilename)

        return datetime, person_id

    def get_person_id_source(self, tgtfilename: str) -> str | None:
        """Get the person id source for an omop table"""
        if tgtfilename in self.cdm:
            for mapping in self.cdm[tgtfilename].values():
                if mapping.person_field:
                    return mapping.person_field

    def get_datetime_source(self, tgtfilename: str) -> str | None:
        """Get the person id source for an omop table"""
        if tgtfilename in self.cdm:
            for mapping in self.cdm[tgtfilename].values():
                if mapping.date_field:
                    return mapping.date_field

    def get_person_source_field_info(self, tgtfilename: str) -> tuple[str | None, str | None]:
        birth_datetime_source = self.get_datetime_source(tgtfilename)
        person_id_source = self.get_person_id_source(tgtfilename)

        return birth_datetime_source, person_id_source


    def parse_rules_src_to_tgt(self, infilename) -> tuple[list, dict]:
        raise NotImplementedError



class V1RuleSet(BaseModel):
    metadata: RuleSetMetadata | None
    cdm: dict[
            Literal["observation", "measurement", "person", "condition_occurrence"],
            dict[
                str,
                dict[
                    str, V1CDMField
                    ]
                ]
            ]

    parsed_rules: dict[str, dict[str, Any]] = {}
    outfile_names: dict[str, list[str]] = {}

    def is_v2_format(self) -> bool:
        return False

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
        return list({field.source_table for omop_table in self.cdm.values() for field in omop_table.values()})

    def get_infile_data_fields(self, infilename: str) -> dict[str, list[str]]:
        """
        Get data fields for a specific input file

        Parameters
        ----------
        infilename: str
            The name of a source table

        Returns
        -------
        dict[str, list[str]]
            A dictionary of a list of fields for each OMOP table
        """
        data_fields = {}
        for omop_table, table_mapping in self.cdm.items():
            fields = {rule.source_field for rule in table_mapping.values() if rule.source_table == infilename}
            if len(fields) != 0:
                data_fields[omop_table] = fields
        return data_fields

    def get_infile_field(self, infilename:str, query_field: Callable) -> str:
        for omop_name, table in self.cdm.items():
            query_fields = query_field(omop_name)
            for dest_field, mapping in table.items():
                if dest_field == query_fields and mapping.source_table == infilename:
                    return mapping.source_field
        return ""


    def get_infile_date_person_id(
            self,
            infilename: str,
            datetime_source: Callable,
            person_id_source: Callable,
            ) -> tuple[str, str]:
        datetime_source = self.get_infile_field(infilename, datetime_source)
        person_id_source = self.get_infile_field(infilename, person_id_source)

        return datetime_source, person_id_source

    def _get_birth_datetime_source(self, tgtfilename: str) -> str | None:
        if tgtfilename in self.cdm:
            if "birth_datetime" in self.cdm[tgtfilename].keys():
                return self.cdm[tgtfilename]["birth_datetime"].source_field

    def _get_person_id_source(self, tgtfilename: str):
        if tgtfilename in self.cdm:
            if "person_id" in self.cdm[tgtfilename].keys():
                return self.cdm[tgtfilename]["person_id"].source_field

    def get_person_source_field_info(self, tgtfilename: str) -> tuple[str | None, str | None]:
        birth_datetime_source = self._get_birth_datetime_source(tgtfilename)
        person_id_source = self._get_person_id_source(tgtfilename)

        return birth_datetime_source, person_id_source

    def parse_rules_src_to_tgt(self, infilename) -> tuple[list, dict]:
        """
        Parse rules to produce a map of source to target data for a given input file
        """
        # I'm not touching this nonsense
        ## creates a dict of dicts that has input files as keys, and infile~field~data~target as keys for the underlying keys, which contain a list of dicts of lists
        if infilename in self.outfile_names and infilename in self.parsed_rules:
            return self.outfile_names[infilename], self.parsed_rules[infilename]
        outfilenames = []
        outdata = {}

        for outfilename, rules_set in self.cdm.items():
            for rules in rules_set.values():
                key, data = self.process_rules(infilename, outfilename, rules)
                if key != "":
                    if key not in outdata:
                        outdata[key] = []
                        if key.split("~")[-1] == "person":
                            outdata[key].append(data)

                    if key.split("~")[-1] == "person":
                        # Find matching source field keys and merge their dictionaries
                        for source_field, value in data.items():
                            if source_field in outdata[key][0] and isinstance(
                                outdata[key][0][source_field], dict
                            ):
                                # Merge the dictionaries for this source field
                                outdata[key][0][source_field].update(value)
                            else:
                                # If no matching dict or new source field, just set it
                                outdata[key][0][source_field] = value
                            pass
                    else:
                        outdata[key].append(data)
                    if outfilename not in outfilenames:
                        outfilenames.append(outfilename)

        self.parsed_rules[infilename] = outdata
        self.outfile_names[infilename] = outfilenames
        return outfilenames, outdata

    def process_rules(self, infilename, outfilename, rules):
        """
        Process rules for an infile, outfile combination
        """
        data = {}

        ### used for mapping simple fields that are always mapped (e.g., dob)
        plain_key = ""
        term_value_key = ""  ### used for mapping terms (e.g., gender, race, ethnicity)

        ## iterate through the rules, looking for rules that apply to the input file.
        for outfield, source_info in rules.items():
            # Check if this rule applies to our input file
            if source_info["source_table"] == infilename:
                if "term_mapping" in source_info:
                    if type(source_info["term_mapping"]) is dict:
                        for inputvalue, term in source_info["term_mapping"].items():
                            if outfilename == "person":
                                term_value_key = infilename + "~person"
                                source_field = source_info["source_field"]
                                if source_field not in data:
                                    data[source_field] = {}
                                if str(inputvalue) not in data[source_field]:
                                    try:
                                        data[source_field][str(inputvalue)] = []
                                    except TypeError:
                                        ### need to convert data[source_field] to a dict
                                        ### like this: {'F': ['gender_concept_id~8532', 'gender_source_concept_id~8532', 'gender_source_value']}
                                        temp_data_list = data[source_field].copy()
                                        data[source_field] = {}
                                        data[source_field][str(inputvalue)] = (
                                            temp_data_list
                                        )

                                data[source_field][str(inputvalue)].append(
                                    outfield + "~" + str(term)
                                )
                            else:
                                term_value_key = (
                                    infilename
                                    + "~"
                                    + source_info["source_field"]
                                    + "~"
                                    + str(inputvalue)
                                    + "~"
                                    + outfilename
                                )
                                if source_info["source_field"] not in data:
                                    data[source_info["source_field"]] = []
                                data[source_info["source_field"]].append(
                                    outfield + "~" + str(term)
                                )
                    else:
                        plain_key = (
                            infilename
                            + "~"
                            + source_info["source_field"]
                            + "~"
                            + outfilename
                        )
                        if source_info["source_field"] not in data:
                            data[source_info["source_field"]] = []
                        data[source_info["source_field"]].append(
                            outfield + "~" + str(source_info["term_mapping"])
                        )
                else:
                    if source_info["source_field"] not in data:
                        data[source_info["source_field"]] = []
                    if type(data[source_info["source_field"]]) is dict:
                        data[source_info["source_field"]][str(inputvalue)].append(
                            outfield
                        )
                    else:
                        data[source_info["source_field"]].append(outfield)
        if term_value_key != "":
            return term_value_key, data

        return plain_key, data
