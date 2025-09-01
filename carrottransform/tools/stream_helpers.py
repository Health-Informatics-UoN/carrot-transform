from collections import defaultdict
from typing import Any, Dict, Set

from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM


class StreamingLookupCache:
    """Pre-computed lookup tables for efficient streaming processing"""

    def __init__(self, mappingrules: MappingRules, omopcdm: OmopCDM):
        self.mappingrules = mappingrules
        self.omopcdm = omopcdm

        # Pre-compute lookups
        self.input_to_outputs = self._build_input_to_output_lookup()
        self.file_metadata_cache = self._build_file_metadata_cache()
        self.target_metadata_cache = self._build_target_metadata_cache()

    def _build_input_to_output_lookup(self) -> Dict[str, Set[str]]:
        """Build lookup: input_file -> set of output tables it can map to"""
        lookup = defaultdict(set)

        for target_file, source_mappings in self.mappingrules.v2_mappings.items():
            for source_file in source_mappings.keys():
                lookup[source_file].add(target_file)

        return dict(lookup)

    def _build_file_metadata_cache(self) -> Dict[str, Dict[str, Any]]:
        """Pre-compute metadata for each input file"""
        cache = {}

        for input_file in self.mappingrules.get_all_infile_names():
            datetime_source, person_id_source = (
                self.mappingrules.get_infile_date_person_id(input_file)
            )

            data_fields = self.mappingrules.get_infile_data_fields(input_file)

            cache[input_file] = {
                "datetime_source": datetime_source,
                "person_id_source": person_id_source,
                "data_fields": data_fields,
            }

        return cache

    def _build_target_metadata_cache(self) -> Dict[str, Dict[str, Any]]:
        """Pre-compute metadata for each target table"""
        cache = {}

        for target_file in self.mappingrules.get_all_outfile_names():
            auto_num_col = self.omopcdm.get_omop_auto_number_field(target_file)
            person_id_col = self.omopcdm.get_omop_person_id_field(target_file)
            date_col_data = self.omopcdm.get_omop_datetime_linked_fields(target_file)
            date_component_data = self.omopcdm.get_omop_date_field_components(
                target_file
            )
            notnull_numeric_fields = self.omopcdm.get_omop_notnull_numeric_fields(
                target_file
            )

            cache[target_file] = {
                "auto_num_col": auto_num_col,
                "person_id_col": person_id_col,
                "date_col_data": date_col_data,
                "date_component_data": date_component_data,
                "notnull_numeric_fields": notnull_numeric_fields,
            }

        return cache
