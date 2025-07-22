import csv
from pathlib import Path
from typing import Dict, Tuple, Any

import carrottransform.tools as tools
from carrottransform.tools.mappingrules import (
    MappingRules,
)
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.date_helpers import normalise_to8601
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.core_v2 import get_target_records_v2

logger = logger_setup()

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
            # index of the datetime column in the input file
            datetime_col: int = inputcolmap[datetime_source]

            # Get data fields for this file
            # for example, Demographics.csv: {'observation': ['ethnicity'], 'person': ['sex']}
            dflist = mappingrules.get_infile_data_fields(srcfilename)

            # Process each row
            for indata in csvr:
                # TODO: understand the purpose and impact of this
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
                        # mapping rules for the target table
                        v2_mapping = mappingrules.v2_mappings[tgtfile][srcfilename]
                        # column map for the target table
                        # for example: {'person_id': 0, 'gender_concept_id': 1,...}
                        tgtcolmap = tgtcolmaps[tgtfile]
                        #  identify the auto-number column for the target table
                        auto_num_col = omopcdm.get_omop_auto_number_field(tgtfile)
                        # identify the person id column for the target table
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
                                srcfield=datacol,
                                srcdata=indata,
                                srccolmap=inputcolmap,
                                srcfilename=srcfilename,
                                omopcdm=omopcdm,
                                metrics=metrics,
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