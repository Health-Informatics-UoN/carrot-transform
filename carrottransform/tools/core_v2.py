import csv
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import carrottransform.tools as tools
from carrottransform.tools.mappingrules import (
    ConceptMapping,
    DateMapping,
    PersonIdMapping,
    MappingRules,
    V2TableMapping,
)
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.date_helpers import normalise_to8601, get_datetime_value
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.validation import valid_value

logger = logger_setup()


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
    - Wildcard (*) mappings: Maps all field values to the same concept
    - Direct concept mappings: Maps specific values to concept IDs
    - Original value mappings: Direct field copying (like v1's condition_source_value)
    """
    build_records = False
    tgtrecords: List[List[str]] = []

    # Get field definitions from OMOP CDM
    date_col_data = omopcdm.get_omop_datetime_linked_fields(tgtfilename)
    date_component_data = omopcdm.get_omop_date_field_components(tgtfilename)
    notnull_numeric_fields = omopcdm.get_omop_notnull_numeric_fields(tgtfilename)

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

    # Create target record
    tgtarray = [""] * len(tgtcolmap)

    # Initialize numeric fields to 0
    for req_integer in notnull_numeric_fields:
        if req_integer in tgtcolmap:
            tgtarray[tgtcolmap[req_integer]] = "0"

    # Handle concept mappings (specific values or wildcards)
    value_mapping = _get_value_mapping(concept_mapping, source_value)
    if value_mapping:
        build_records = True
        _apply_concept_mappings(tgtarray, tgtcolmap, value_mapping)

    # Handle original value fields (direct field copying)
    if concept_mapping.original_value_fields:
        build_records = True
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
            return False, [], metrics

    if build_records:
        tgtrecords.append(tgtarray)

    return build_records, tgtrecords, metrics


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
        return True

    source_date = srcdata[srccolmap[date_mapping.source_field]]

    for dest_field in date_mapping.dest_fields:
        if dest_field in tgtcolmap:
            # Handle date component fields (birth dates with year/month/day)
            if dest_field in date_component_data:
                dt = get_datetime_value(source_date)
                if dt is None:
                    metrics.increment_key_count(
                        source=srcfilename,
                        fieldname=srcfield,
                        tablename=tgtfilename,
                        concept_id="all",
                        additional="",
                        count_type="invalid_date_fields",
                    )
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


def process_v2_data(
    mappingrules: MappingRules,
    omopcdm: OmopCDM,
    input_dir: Path,
    person_lookup: Dict[str, str],
    record_numbers: Dict[str, int],
    fhd: Dict[str, Any],
    tgtcolmaps: Dict[str, Dict[str, int]],
    metrics: tools.metrics.Metrics,
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Process data using v2 format rules

    Returns:
        Tuple of (outcounts, rejidcounts)
    """
    logger.info("Processing data using v2 format...")

    # Get all input files from rules
    input_files = mappingrules.get_all_infile_names()
    output_files = mappingrules.get_all_outfile_names()

    # Initialize counters
    outcounts = {outfile: 0 for outfile in output_files}
    rejidcounts = {infile: 0 for infile in input_files}

    # Process each input file
    for srcfilename in input_files:
        logger.info(f"Processing input file: {srcfilename}")

        # Open and read the input file
        file_path = input_dir / srcfilename
        if not file_path.exists():
            logger.warning(f"Input file not found: {srcfilename}")
            continue

        with file_path.open(mode="r", encoding="utf-8-sig") as fh:
            csvr = csv.reader(fh)
            csv_column_headers = next(csvr)
            inputcolmap = omopcdm.get_column_map(csv_column_headers)

            # Get date and person ID fields for this file
            datetime_source, person_id_source = mappingrules.get_infile_date_person_id(
                srcfilename
            )

            if not datetime_source or not person_id_source:
                logger.warning(f"Missing date or person ID mapping for {srcfilename}")
                continue

            datetime_col = inputcolmap[datetime_source]
            person_id_col = inputcolmap[person_id_source]

            # Get data fields for this file
            dflist = mappingrules.get_infile_data_fields(srcfilename)

            # Process each row
            for indata in csvr:
                metrics.increment_key_count(
                    source=srcfilename,
                    fieldname="all",
                    tablename="all",
                    concept_id="all",
                    additional="",
                    count_type="input_count",
                )

                # Normalize date
                fulldate = normalise_to8601(indata[datetime_col])
                if fulldate is None:
                    metrics.increment_key_count(
                        source=srcfilename,
                        fieldname="all",
                        tablename="all",
                        concept_id="all",
                        additional="",
                        count_type="input_date_fields",
                    )
                    continue

                indata[datetime_col] = fulldate

                # Process each target table
                for tgtfile in output_files:
                    if (
                        tgtfile in mappingrules.v2_mappings
                        and srcfilename in mappingrules.v2_mappings[tgtfile]
                    ):
                        v2_mapping = mappingrules.v2_mappings[tgtfile][srcfilename]
                        tgtcolmap = tgtcolmaps[tgtfile]
                        auto_num_col = omopcdm.get_omop_auto_number_field(tgtfile)
                        pers_id_col = omopcdm.get_omop_person_id_field(tgtfile)

                        # Get data fields for this target table
                        datacols = dflist.get(tgtfile, [])

                        # Process each data column
                        for datacol in datacols:
                            if datacol not in inputcolmap:
                                continue

                            built_records, outrecords, metrics = get_target_records_v2(
                                tgtfile,
                                tgtcolmap,
                                v2_mapping,
                                datacol,
                                indata,
                                inputcolmap,
                                srcfilename,
                                omopcdm,
                                metrics,
                            )

                            if built_records:
                                for outrecord in outrecords:
                                    # Set auto-increment ID
                                    if auto_num_col is not None:
                                        outrecord[tgtcolmap[auto_num_col]] = str(
                                            record_numbers[tgtfile]
                                        )
                                        record_numbers[tgtfile] += 1

                                    # Map person ID
                                    person_id = outrecord[tgtcolmap[pers_id_col]]
                                    if person_id in person_lookup:
                                        outrecord[tgtcolmap[pers_id_col]] = (
                                            person_lookup[person_id]
                                        )
                                        outcounts[tgtfile] += 1

                                        # Update metrics
                                        metrics.increment_with_datacol(
                                            source_path=srcfilename,
                                            target_file=tgtfile,
                                            datacol=datacol,
                                            out_record=outrecord,
                                        )

                                        # Write to output file
                                        fhd[tgtfile].write("\t".join(outrecord) + "\n")
                                    else:
                                        metrics.increment_key_count(
                                            source=srcfilename,
                                            fieldname="all",
                                            tablename=tgtfile,
                                            concept_id="all",
                                            additional="",
                                            count_type="invalid_person_ids",
                                        )
                                        rejidcounts[srcfilename] += 1

    return outcounts, rejidcounts
