import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import carrottransform.tools as tools
from carrottransform.tools.mappingrules import (
    ConceptMapping,
    DateMapping,
    PersonIdMapping,
    V2TableMapping,
)
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.date_helpers import get_datetime_value
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.validation import valid_value

logger = logger_setup()

# Global cache to track processed person records
_person_processed_cache: set[str] = set()

#  TODO: don't output the meaningless records
def get_target_records_v2(
    tgtfilename: str,
    tgtcolmap: Dict[str, int],
    v2_mapping: V2TableMapping,
    srcfield: str,
    srcdata: List[str],
    srccolmap: Dict[str, int],
    srcfilename: str,
    omopcdm: OmopCDM,
    metrics: tools.metrics.Metrics,
) -> Tuple[bool, List[List[str]], tools.metrics.Metrics]:
    """
    Build target records for v2 format - clean implementation

    Handles:
    - Multiple concept IDs: Creates separate records for each concept ID
    - Wildcard (*) mappings: Maps all field values to the same concept
    - Original value mappings: Direct field copying (like v1's condition_source_value)
    """
    build_records = False
    tgtrecords: List[List[str]] = []

    # Get field definitions from OMOP CDM
    date_col_data = omopcdm.get_omop_datetime_linked_fields(tgtfilename)
    date_component_data = omopcdm.get_omop_date_field_components(tgtfilename)
    notnull_numeric_fields = omopcdm.get_omop_notnull_numeric_fields(tgtfilename)

    # Special handling for person table - merge all field mappings
    if tgtfilename == "person":
        # Check if person ID mapping exists
        if not v2_mapping.person_id_mapping:
            return build_records, tgtrecords, metrics
        
        # Create a unique key for this source row
        person_key = f"{srcfilename}:{srcdata[srccolmap[v2_mapping.person_id_mapping.source_field]]}"
        
        # Only process if we haven't already processed this person record
        if person_key in _person_processed_cache:
            return build_records, tgtrecords, metrics
        
        # Mark this person as processed
        _person_processed_cache.add(person_key)
        
        # Collect all concept mappings from ALL fields
        all_concept_mappings = {}
        all_original_values = {}
        
        for field_name, concept_mapping in v2_mapping.concept_mappings.items():
            if field_name not in srccolmap:
                continue
                
            # Check if field has valid value
            source_value = str(srcdata[srccolmap[field_name]])
            if not valid_value(source_value):
                continue
                
            # Get value mapping for this field
            value_mapping = _get_value_mapping(concept_mapping, source_value)
            
            if value_mapping:
                # Add this field's mappings to the combined mappings
                for dest_field, concept_ids in value_mapping.items():
                    all_concept_mappings[dest_field] = concept_ids
            
            # Collect original value mappings
            if concept_mapping.original_value_fields:
                for dest_field in concept_mapping.original_value_fields:
                    all_original_values[dest_field] = source_value
        
        # If no valid mappings found, return empty
        if not all_concept_mappings and not all_original_values:
            return build_records, tgtrecords, metrics

        # Generate combined concept combinations
        concept_combinations = _get_concept_combinations(all_concept_mappings) if all_concept_mappings else [{}]
        
        # Create person records for each combination
        for concept_combo in concept_combinations:
            build_records = True
            
            # Create target record
            tgtarray = [""] * len(tgtcolmap)

            # Initialize numeric fields to 0
            for req_integer in notnull_numeric_fields:
                if req_integer in tgtcolmap:
                    tgtarray[tgtcolmap[req_integer]] = "0"

            # Apply the merged concept combination
            _apply_single_concept_mapping(tgtarray, tgtcolmap, concept_combo)

            # Handle original value fields (direct field copying)
            for dest_field, source_value in all_original_values.items():
                if dest_field in tgtcolmap:
                    tgtarray[tgtcolmap[dest_field]] = source_value

            # Handle person ID mapping
            if v2_mapping.person_id_mapping:
                _apply_person_id_mapping(
                    tgtarray, tgtcolmap, v2_mapping.person_id_mapping, srcdata, srccolmap
                )

            # Handle date mappings
            if v2_mapping.date_mapping:
                success = _apply_date_mappings(
                    tgtarray,
                    tgtcolmap,
                    v2_mapping.date_mapping,
                    srcdata,
                    srccolmap,
                    date_col_data,
                    date_component_data,
                    srcfilename,
                    "person_combined",  # Combined field name for person
                    tgtfilename,
                    metrics,
                )
                if not success:
                    logger.warning(f"Failed to apply date mappings for person table")
                    continue

            tgtrecords.append(tgtarray)
        
        return build_records, tgtrecords, metrics

    # Regular processing for non-person tables
    # Check if source field has a value
    if not valid_value(str(srcdata[srccolmap[srcfield]])):
        metrics.increment_key_count(
            source=srcfilename,
            fieldname=srcfield,
            tablename=tgtfilename,
            concept_id="all",
            additional="",
            count_type="invalid_source_fields",
        )
        return build_records, tgtrecords, metrics

    # Check if we have a concept mapping for this field
    if srcfield not in v2_mapping.concept_mappings:
        return build_records, tgtrecords, metrics

    concept_mapping = v2_mapping.concept_mappings[srcfield]
    source_value = str(srcdata[srccolmap[srcfield]])

    # Get value mapping (concept mappings or wildcard)
    value_mapping = _get_value_mapping(concept_mapping, source_value)
    
    # Only proceed if we have concept mappings OR original value fields
    if not value_mapping and not concept_mapping.original_value_fields:
        return build_records, tgtrecords, metrics

    # Generate all concept combinations
    concept_combinations = _get_concept_combinations(value_mapping)
    
    # If no concept combinations but we have original_value fields, create one record
    if not concept_combinations and concept_mapping.original_value_fields:
        concept_combinations = [{}]  # Empty mapping for original values only

    # Create records for each concept combination
    for concept_combo in concept_combinations:
        build_records = True
        
        # Create target record
        tgtarray = [""] * len(tgtcolmap)

        # Initialize numeric fields to 0
        for req_integer in notnull_numeric_fields:
            if req_integer in tgtcolmap:
                tgtarray[tgtcolmap[req_integer]] = "0"

        # Apply this specific concept combination
        _apply_single_concept_mapping(tgtarray, tgtcolmap, concept_combo)

        # Handle original value fields (direct field copying)
        if concept_mapping.original_value_fields:
            _apply_original_value_mappings(
                tgtarray, tgtcolmap, concept_mapping.original_value_fields, source_value
            )

        # Handle person ID mapping
        if v2_mapping.person_id_mapping:
            _apply_person_id_mapping(
                tgtarray, tgtcolmap, v2_mapping.person_id_mapping, srcdata, srccolmap
            )

        # Handle date mappings
        if v2_mapping.date_mapping:
            success = _apply_date_mappings(
                tgtarray,
                tgtcolmap,
                v2_mapping.date_mapping,
                srcdata,
                srccolmap,
                date_col_data,
                date_component_data,
                srcfilename,
                srcfield,
                tgtfilename,
                metrics,
            )
            if not success:
                logger.warning(f"Failed to apply date mappings for {srcfield}")
                return False, [], metrics
        
        tgtrecords.append(tgtarray)

    return build_records, tgtrecords, metrics


def _get_concept_combinations(value_mapping: Optional[Dict[str, List[int]]]) -> List[Dict[str, int]]:
    """
    Generate all concept combinations for multiple concept IDs
    
    For example, if value_mapping is:
    {
        "observation_concept_id": [35827395, 35825531],
        "observation_source_concept_id": [35827395, 35825531]
    }
    
    This returns:
    [
        {"observation_concept_id": 35827395, "observation_source_concept_id": 35827395},
        {"observation_concept_id": 35825531, "observation_source_concept_id": 35825531}
    ]
    """
    if not value_mapping:
        return []
    
    # Find the maximum number of concept IDs across all fields
    max_concepts = max(len(concept_ids) for concept_ids in value_mapping.values() if concept_ids)
    
    combinations = []
    for i in range(max_concepts):
        combo = {}
        for dest_field, concept_ids in value_mapping.items():
            if concept_ids:
                # Use the concept at index i, or the last one if not enough concepts
                concept_index = min(i, len(concept_ids) - 1)
                combo[dest_field] = concept_ids[concept_index]
        combinations.append(combo)
    
    return combinations


def _apply_single_concept_mapping(
    tgtarray: List[str], tgtcolmap: Dict[str, int], concept_combo: Dict[str, int]
):
    """Apply a single concept combination to target array"""
    for dest_field, concept_id in concept_combo.items():
        if dest_field in tgtcolmap:
            tgtarray[tgtcolmap[dest_field]] = str(concept_id)


def _get_value_mapping(
    concept_mapping: ConceptMapping, source_value: str
) -> Optional[Dict[str, List[int]]]:
    """
    Get value mapping for a source value, handling wildcards

    Priority:
    1. Exact match for source value
    2. Wildcard match (*) - maps all values to same concept
    3. None
    """
    if source_value in concept_mapping.value_mappings:
        return concept_mapping.value_mappings[source_value]
    elif "*" in concept_mapping.value_mappings:
        return concept_mapping.value_mappings["*"]
    return None


def _apply_concept_mappings(
    tgtarray: List[str], tgtcolmap: Dict[str, int], value_mapping: Dict[str, List[int]]
):
    """Apply concept ID mappings to target array"""
    for dest_field, concept_ids in value_mapping.items():
        if dest_field in tgtcolmap:
            # Use the first concept ID if multiple are provided
            concept_id = concept_ids[0] if concept_ids else 0
            tgtarray[tgtcolmap[dest_field]] = str(concept_id)


def _apply_original_value_mappings(
    tgtarray: List[str],
    tgtcolmap: Dict[str, int],
    original_value_fields: List[str],
    source_value: str,
):
    """
    Apply original value mappings (direct field copying)

    This is similar to how v1 handled condition_source_value - just copy the raw value
    from the source field to the destination field without any transformation.
    """
    for dest_field in original_value_fields:
        if dest_field in tgtcolmap:
            tgtarray[tgtcolmap[dest_field]] = source_value


def _apply_person_id_mapping(
    tgtarray: List[str],
    tgtcolmap: Dict[str, int],
    person_id_mapping: PersonIdMapping,
    srcdata: List[str],
    srccolmap: Dict[str, int],
):
    """Apply person ID mapping"""
    if (
        person_id_mapping.dest_field in tgtcolmap
        and person_id_mapping.source_field in srccolmap
    ):
        person_id = srcdata[srccolmap[person_id_mapping.source_field]]
        tgtarray[tgtcolmap[person_id_mapping.dest_field]] = person_id


def _apply_date_mappings(
    tgtarray: List[str],
    tgtcolmap: Dict[str, int],
    date_mapping: DateMapping,
    srcdata: List[str],
    srccolmap: Dict[str, int],
    date_col_data: Dict[str, str],
    date_component_data: Dict[str, Dict[str, str]],
    srcfilename: str,
    srcfield: str,
    tgtfilename: str,
    metrics: tools.metrics.Metrics,
) -> bool:
    """Apply date mappings with proper error handling"""
    if date_mapping.source_field not in srccolmap:
        logger.warning(f"Date mapping source field not found in source data: {date_mapping.source_field}")
        return True

    source_date = srcdata[srccolmap[date_mapping.source_field]]

    for dest_field in date_mapping.dest_fields:
        if dest_field in tgtcolmap:
            # Handle date component fields (birth dates with year/month/day)
            if dest_field in date_component_data:
                # TODO: check the case where the date is not in the format YYYY-MM-DD 00:00:00
                dt = get_datetime_value(source_date.split(" ")[0])
                if dt is None:
                    metrics.increment_key_count(
                        source=srcfilename,
                        fieldname=srcfield,
                        tablename=tgtfilename,
                        concept_id="all",
                        additional="",
                        count_type="invalid_date_fields",
                    )
                    logger.warning(f"Invalid date fields: {srcfield}")
                    return False

                # Set individual date components
                component_info = date_component_data[dest_field]
                if "year" in component_info and component_info["year"] in tgtcolmap:
                    tgtarray[tgtcolmap[component_info["year"]]] = str(dt.year)
                if "month" in component_info and component_info["month"] in tgtcolmap:
                    tgtarray[tgtcolmap[component_info["month"]]] = str(dt.month)
                if "day" in component_info and component_info["day"] in tgtcolmap:
                    tgtarray[tgtcolmap[component_info["day"]]] = str(dt.day)

                # Set the main date field
                tgtarray[tgtcolmap[dest_field]] = source_date

            # Handle regular date fields with linked date-only fields
            elif dest_field in date_col_data:
                tgtarray[tgtcolmap[dest_field]] = source_date
                # Set the linked date-only field
                if date_col_data[dest_field] in tgtcolmap:
                    tgtarray[tgtcolmap[date_col_data[dest_field]]] = source_date[:10]

            # Handle simple date fields
            else:
                tgtarray[tgtcolmap[dest_field]] = source_date

    return True
