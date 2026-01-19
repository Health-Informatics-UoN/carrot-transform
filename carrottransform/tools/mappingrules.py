from pathlib import Path
from typing import Any

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
        self.rules_data: RuleSet = parse_mapping_rules(rules_file_path)
        self.omopcdm = omopcdm

        self.dataset_name = self.rules_data.get_dsname_from_rules()

    @property
    def is_v2_format(self) -> bool:
        return self.rules_data.is_v2_format()
        
    def dump_parsed_rules(self) -> str:
        return self.rules_data.dump_parsed_rules()

    # this is all a bit redundant
    def get_dsname_from_rules(self) -> str:
        return self.rules_data.get_dsname_from_rules()

    def get_dataset_name(self):
        return self.dataset_name

    def get_all_outfile_names(self):
        return self.rules_data.get_all_outfile_names()

    def get_all_infile_names(self):
        return self.rules_data.get_all_infile_names()

    def get_infile_data_fields(self, infilename: str):
        return self.rules_data.get_infile_data_fields(infilename)

    def get_infile_date_person_id(self, infilename: str) -> tuple[str, str]:
        return self.rules_data.get_infile_date_person_id(
                infilename,
                datetime_source=self.omopcdm.get_omop_datetime_fields,
                person_id_source=self.omopcdm.get_omop_person_id_field
                )

    def get_person_source_field_info(self, tgtfilename: str) -> tuple[str | None, str | None]:
        return self.rules_data.get_person_source_field_info(tgtfilename)

    def parse_rules_src_to_tgt(self, infilename) -> tuple[list, dict]:
        """
        Parse rules to produce a map of source to target data for a given input file
        """
        try:
            self.rules_data.parse_rules_src_to_tgt(infilename)
        except NotImplementedError:
            logger.error("Expected a v1 ruleset")

