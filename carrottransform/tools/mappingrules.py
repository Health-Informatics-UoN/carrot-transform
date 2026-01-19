import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

import carrottransform.tools as tools
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.mapping_types import (
       V2RuleSet,
       V1RuleSet,
       RuleSet
)
from carrottransform.tools.omopcdm import OmopCDM

logger = logger_setup()

def is_v2_format(rules_data: dict[Any, Any]) -> bool:
     """
     Detect if the rules file is in v2 format by checking for characteristic v2 structures
     """
     # Check if any table has the v2 structure (source_table -> mapping_types)
     for _, table_data in rules_data["cdm"].items():
         if isinstance(table_data, dict):
             for _, value in table_data.items():
                 # v2 format has CSV filenames as keys, with mapping types as values
                 if isinstance(value, dict) and all(
                     mapping_type in value
                     for mapping_type in [
                         "person_id_mapping",
                         "date_mapping",
                         "concept_mappings",
                     ]
                 ):
                     return True
     return False

def parse_mapping_rules(rules_file_path: Path) -> RuleSet:
    """
    Detect format and parse to RulesData 
    """
    rules_data = tools.load_json(rules_file_path)

    # part of me wants to try.. except.. finally... this bad boy
    if is_v2_format(rules_data):
        return V2RuleSet.model_validate(rules_data)
    else:
        return V1RuleSet.model_validate(rules_data)

class MappingRules:
    """
    self.rules_data stores the mapping rules as untransformed json, as each input file is processed rules are reorganised
    as a file-specific dictionary allowing rules to be "looked-up" depending on data content
    """

    def __init__(self, rules_file_path: Path, omopcdm: OmopCDM):
        ## just loads the json directly
        self.rules_data = parse_mapping_rules(rules_file_path)
        self.mapping_rules = self.rules_data.to_mapping_index()
        self.omopcdm = omopcdm

        self.dataset_name = self.rules_data.get_dsname_from_rules()
        
    def dump_parsed_rules(self) -> str:
        return self.mapping_rules.model_dump_json(indent=2)

    # this is all a bit redundant
    def get_dsname_from_rules(self) -> str:
        return self.rules_data.get_dsname_from_rules()

    def get_dataset_name(self):
        return self.dataset_name

    def get_all_outfile_names(self):
        return self.mapping_rules.get_omop_tables()

    def get_all_infile_names(self):
        return self.mapping_rules.get_source_tables()

    def get_infile_data_fields(self, infilename: str):
        rules = self.mapping_rules.by_source_table(infilename)
        return [rule.source_field for rule in rules]

    def get_infile_date_person_id(self, infilename: str) -> tuple[str, str]:
        # but why?
        # and why a tuple of strings
        rules = self.mapping_rules.by_source_table(infilename)
        datetime_source = [rule.source_field for rule in rules if rule.source_field in self.omopcdm.get_omop_datetime_fields(rule.omop_table)][0]
        person_id_source = [rule.source_field for rule in rules if rule.is_person_id_mapping][0]
        return datetime_source, person_id_source

    def get_person_source_field_info(self, tgtfilename: str) -> tuple[str | None, str | None]:
        rules = self.mapping_rules.by_omop_table(tgtfilename)
        birth_datetime_source = [rule.source_field for rule in rules if rule.]


    def _get_person_source_field_info_v2(
        self, tgtfilename: str
    ) -> tuple[Optional[str], Optional[str]]:
        """
        Get person source field info for v2 format,
        from the dest. table "Person" in the rules file.
        """
        birth_datetime_source = None
        person_id_source = None

        if tgtfilename in self.v2_mappings:
            for mapping in self.v2_mappings[tgtfilename].values():
                if mapping.date_mapping:
                    birth_datetime_source = mapping.date_mapping.source_field

                if mapping.person_id_mapping:
                    person_id_source = mapping.person_id_mapping.source_field

                # If we found both, we can break
                if birth_datetime_source and person_id_source:
                    break

        return birth_datetime_source, person_id_source

    def _get_person_source_field_info_v1(
        self, tgtfilename: str
    ) -> tuple[Optional[str], Optional[str]]:
        """Get person source field info for v1 format (legacy method)"""
        birth_datetime_source = None
        person_id_source = None
        if tgtfilename in self.rules_data["cdm"]:
            source_rules_data = self.rules_data["cdm"][tgtfilename]
            ## this loops over all the fields in the person part of the rules, which will lead to overwriting of the source variables and unneccesary looping
            for rule_name, rule_fields in source_rules_data.items():
                if "birth_datetime" in rule_fields:
                    birth_datetime_source = rule_fields["birth_datetime"][
                        "source_field"
                    ]
                if "person_id" in rule_fields:
                    person_id_source = rule_fields["person_id"]["source_field"]

        return birth_datetime_source, person_id_source

    def parse_rules_src_to_tgt(self, infilename):
        """
        Parse rules to produce a map of source to target data for a given input file
        """
        ## creates a dict of dicts that has input files as keys, and infile~field~data~target as keys for the underlying keys, which contain a list of dicts of lists
        if infilename in self.outfile_names and infilename in self.parsed_rules:
            return self.outfile_names[infilename], self.parsed_rules[infilename]
        outfilenames = []
        outdata = {}

        for outfilename, rules_set in self.rules_data["cdm"].items():
            for datatype, rules in rules_set.items():
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
