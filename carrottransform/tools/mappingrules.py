import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import carrottransform.tools as tools
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.mapping_types import (
    ConceptMapping,
    DateMapping,
    PersonIdMapping,
    V2TableMapping,
)
from carrottransform.tools.omopcdm import OmopCDM

logger = logger_setup()


class MappingRules:
    """
    self.rules_data stores the mapping rules as untransformed json, as each input file is processed rules are reorganised
    as a file-specific dictionary allowing rules to be "looked-up" depending on data content
    """

    def __init__(self, rulesfilepath: Path, omopcdm: OmopCDM):
        ## just loads the json directly
        self.rules_data = tools.load_json(Path(rulesfilepath))
        self.omopcdm = omopcdm

        # Detect format version and parse accordingly
        self.is_v2_format = self._is_v2_format()
        if self.is_v2_format:
            logger.info("Detected v2.json format, using direct v2 parser...")
            self.v2_mappings = self._parse_v2_format()
        else:
            logger.info("Detected v1.json format, using legacy parser...")

        self.parsed_rules: Dict[str, Dict[str, Any]] = {}
        self.outfile_names: Dict[str, List[str]] = {}

        self.dataset_name = self.get_dsname_from_rules()

    def _is_v2_format(self) -> bool:
        """
        Detect if the rules file is in v2 format by checking for characteristic v2 structures
        """
        # Check if any table has the v2 structure (source_table -> mapping_types)
        for table_name, table_data in self.rules_data["cdm"].items():
            if isinstance(table_data, dict):
                for key, value in table_data.items():
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

    def _parse_v2_format(self) -> Dict[str, Dict[str, V2TableMapping]]:
        """
        Parse v2 format into clean data structures
        Returns: Dict[table_name, Dict[source_table, V2TableMapping]]
        """
        v2_mappings: Dict[str, Dict[str, V2TableMapping]] = {}

        for table_name, table_data in self.rules_data["cdm"].items():
            v2_mappings[table_name] = {}

            for source_table, mappings in table_data.items():
                # Parse person_id_mapping
                person_id_mapping = None
                if "person_id_mapping" in mappings:
                    pid_data = mappings["person_id_mapping"]
                    person_id_mapping = PersonIdMapping(
                        source_field=pid_data["source_field"],
                        dest_field=pid_data["dest_field"],
                    )

                # Parse date_mapping
                date_mapping = None
                if "date_mapping" in mappings:
                    date_data = mappings["date_mapping"]
                    date_mapping = DateMapping(
                        source_field=date_data["source_field"],
                        dest_fields=date_data["dest_field"],
                    )

                # Parse concept_mappings
                concept_mappings = {}
                if "concept_mappings" in mappings:
                    for source_field, field_mappings in mappings[
                        "concept_mappings"
                    ].items():
                        original_value_fields = field_mappings.get("original_value", [])
                        value_mappings = {}

                        for source_value, dest_mappings in field_mappings.items():
                            if source_value != "original_value":
                                value_mappings[source_value] = dest_mappings

                        concept_mappings[source_field] = ConceptMapping(
                            source_field=source_field,
                            value_mappings=value_mappings,
                            original_value_fields=original_value_fields,
                        )

                v2_mappings[table_name][source_table] = V2TableMapping(
                    source_table=source_table,
                    person_id_mapping=person_id_mapping,
                    date_mapping=date_mapping,
                    concept_mappings=concept_mappings,
                )

        return v2_mappings

    def dump_parsed_rules(self):
        return json.dumps(self.parsed_rules, indent=2)

    def get_dsname_from_rules(self):
        dsname = "Unknown"

        if "metadata" in self.rules_data:
            if "dataset" in self.rules_data["metadata"]:
                dsname = self.rules_data["metadata"]["dataset"]

        return dsname

    def get_dataset_name(self):
        return self.dataset_name

    def get_all_outfile_names(self):
        if self.is_v2_format:
            return list(self.v2_mappings.keys())
        else:
            return list(self.rules_data["cdm"])

    def get_all_infile_names(self):
        if self.is_v2_format:
            return self._get_all_infile_names_v2()
        else:
            return self._get_all_infile_names_v1()

    def _get_all_infile_names_v2(self) -> List[str]:
        """Get all input file names from v2 format"""
        file_list = []
        for table_mappings in self.v2_mappings.values():
            for source_table in table_mappings.keys():
                if source_table not in file_list:
                    file_list.append(source_table)
        return file_list

    def _get_all_infile_names_v1(self) -> List[str]:
        """Get all input file names from v1 format (legacy method)"""
        file_list = []
        for outfilename, conditions in self.rules_data["cdm"].items():
            for outfield, source_field in conditions.items():
                for source_field_name, source_data in source_field.items():
                    if "source_table" in source_data:
                        if source_data["source_table"] not in file_list:
                            file_list.append(source_data["source_table"])
        return file_list

    def get_infile_data_fields(self, infilename: str):
        if self.is_v2_format:
            return self._get_infile_data_fields_v2(infilename)
        else:
            return self._get_infile_data_fields_v1(infilename)

    def _get_infile_data_fields_v2(self, infilename: str) -> Dict[str, List[str]]:
        """Get data fields for a specific input file from v2 format"""
        data_fields_lists: Dict[str, List[str]] = {}

        for table_name, table_mappings in self.v2_mappings.items():
            if infilename in table_mappings:
                mapping = table_mappings[infilename]
                data_fields_lists[table_name] = []

                # Add fields from concept mappings
                for source_field in mapping.concept_mappings.keys():
                    if source_field not in data_fields_lists[table_name]:
                        data_fields_lists[table_name].append(source_field)

        return data_fields_lists

    def _get_infile_data_fields_v1(self, infilename: str) -> Dict[str, List[str]]:
        """Get data fields for a specific input file from v1 format (legacy method)"""
        data_fields_lists: Dict[str, List[str]] = {}
        outfilenames, outdata = self.parse_rules_src_to_tgt(infilename)

        for outfilename in outfilenames:
            data_fields_lists[outfilename] = []

        for key, outfield_data in outdata.items():
            keydata = key.split("~")
            outfile = keydata[-1]
            for outfield_elem in outfield_data:
                for infield, outfields in outfield_elem.items():
                    for outfield in outfields:
                        outfielddata = outfield.split("~")
                        if self.omopcdm.is_omop_data_field(outfile, outfielddata[0]):
                            if infield not in data_fields_lists[outfile]:
                                data_fields_lists[outfile].append(infield)

        return data_fields_lists

    def get_infile_date_person_id(self, infilename: str):
        if self.is_v2_format:
            return self._get_infile_date_person_id_v2(infilename)
        else:
            return self._get_infile_date_person_id_v1(infilename)

    # TODO: combine this with _get_person_source_field_info_v2
    def _get_infile_date_person_id_v2(self, infilename: str) -> tuple[str, str]:
        """Get datetime and person_id source fields for v2 format"""
        datetime_source = ""
        person_id_source = ""

        for table_mappings in self.v2_mappings.values():
            if infilename in table_mappings:
                mapping = table_mappings[infilename]

                if mapping.date_mapping:
                    datetime_source = mapping.date_mapping.source_field

                if mapping.person_id_mapping:
                    person_id_source = mapping.person_id_mapping.source_field

                # If we found both, we can break
                if datetime_source and person_id_source:
                    break

        return datetime_source, person_id_source

    def _get_infile_date_person_id_v1(self, infilename: str) -> tuple[str, str]:
        """Get datetime and person_id source fields for v1 format (legacy method)"""
        outfilenames, outdata = self.parse_rules_src_to_tgt(infilename)
        datetime_source = ""
        person_id_source = ""

        for key, outfield_data in outdata.items():
            keydata = key.split("~")
            outfile = keydata[-1]
            for outfield_elem in outfield_data:
                for infield, outfield_list in outfield_elem.items():
                    logger.debug(
                        "{0}, {1}, {2}".format(outfile, infield, str(outfield_list))
                    )
                    for outfield in outfield_list:
                        if outfield.split("~")[
                            0
                        ] in self.omopcdm.get_omop_datetime_fields(outfile):
                            datetime_source = infield
                        if outfield.split("~")[
                            0
                        ] == self.omopcdm.get_omop_person_id_field(outfile):
                            person_id_source = infield

        return datetime_source, person_id_source

    def get_person_source_field_info(self, tgtfilename: str):
        if self.is_v2_format:
            return self._get_person_source_field_info_v2(tgtfilename)
        else:
            return self._get_person_source_field_info_v1(tgtfilename)

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
