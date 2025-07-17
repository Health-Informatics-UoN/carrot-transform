import csv
import datetime
import importlib.resources
import logging
import re
import sys
from pathlib import Path
from typing import IO, Iterator
import carrottransform.tools as tools
from carrottransform.tools.omopcdm import OmopCDM

logger = logging.getLogger(__name__)


def get_target_records(
    tgtfilename: str,
    tgtcolmap: dict[str, dict[str, int]],
    rulesmap: dict[str, list[dict[str, list[str]]]],
    srcfield: str,
    srcdata: list[str],
    srccolmap: dict[str, int],
    srcfilename: str,
    omopcdm: OmopCDM,
    metrics: tools.metrics.Metrics,
) -> tuple[bool, list[str], tools.metrics.Metrics]:
    """
    build all target records for a given input field
    """
    build_records = False
    tgtrecords = []
    # Get field definitions from OMOP CDM
    date_col_data = omopcdm.get_omop_datetime_linked_fields(tgtfilename)
    date_component_data = omopcdm.get_omop_date_field_components(tgtfilename)
    notnull_numeric_fields = omopcdm.get_omop_notnull_numeric_fields(tgtfilename)

    # Build keys to look up rules
    srckey = f"{srcfilename}~{srcfield}~{tgtfilename}"
    summarykey = srckey + "~all~"

    # Check if source field has a value
    if valid_value(str(srcdata[srccolmap[srcfield]])):
        ## check if either or both of the srckey and summarykey are in the rules
        srcfullkey = (
            srcfilename
            + "~"
            + srcfield
            + "~"
            + str(srcdata[srccolmap[srcfield]])
            + "~"
            + tgtfilename
        )

        dictkeys = []
        # Check if we have rules for either the full key or just the source field
        if tgtfilename == "person":
            build_records = True
            dictkeys.append(srcfilename + "~person")
        elif srcfullkey in rulesmap:
            build_records = True
            dictkeys.append(srcfullkey)
        if srckey in rulesmap:
            build_records = True
            dictkeys.append(srckey)

        if build_records:
            # Process each matching rule
            for dictkey in dictkeys:
                for out_data_elem in rulesmap[dictkey]:
                    valid_data_elem = True
                    ## create empty list to store the data. Populate numerical data elements with 0 instead of empty string.
                    tgtarray = [""] * len(tgtcolmap)
                    # Initialize numeric fields to 0
                    for req_integer in notnull_numeric_fields:
                        tgtarray[tgtcolmap[req_integer]] = "0"

                    # Process each field mapping
                    for infield, outfield_list in out_data_elem.items():
                        if tgtfilename == "person" and isinstance(outfield_list, dict):
                            # Handle term mappings for person records
                            input_value = srcdata[srccolmap[infield]]
                            if str(input_value) in outfield_list:
                                for output_col_data in outfield_list[str(input_value)]:
                                    if "~" in output_col_data:
                                        # Handle mapped values (like gender codes)
                                        outcol, term = output_col_data.split("~")
                                        tgtarray[tgtcolmap[outcol]] = term
                                    else:
                                        # Direct field copy
                                        tgtarray[tgtcolmap[output_col_data]] = srcdata[
                                            srccolmap[infield]
                                        ]
                        else:
                            # Handle direct field copies and non-person records
                            for output_col_data in outfield_list:
                                if "~" in output_col_data:
                                    # Handle mapped values (like gender codes)
                                    outcol, term = output_col_data.split("~")
                                    tgtarray[tgtcolmap[outcol]] = term
                                else:
                                    # Direct field copy
                                    tgtarray[tgtcolmap[output_col_data]] = srcdata[
                                        srccolmap[infield]
                                    ]

                            # get the value. this is out 8061 value that was previously normalised
                            source_date = srcdata[srccolmap[infield]]

                            # Special handling for date fields
                            if output_col_data in date_component_data:
                                # this side of the if/else seems to be fore birthdates which're split up into four fields

                                # parse the date and store it in the old format ... as a way to branch
                                # ... this check might be redudant. the datetime values should be ones that have already been normalised
                                dt = get_datetime_value(source_date.split(" ")[0])
                                if dt is None:
                                    # if (as above) dt isn't going to be None than this branch shouldn't happen
                                    # maybe brithdates can be None?

                                    metrics.increment_key_count(
                                        source=srcfilename,
                                        fieldname=srcfield,
                                        tablename=tgtfilename,
                                        concept_id="all",
                                        additional="",
                                        count_type="invalid_date_fields",
                                    )
                                    valid_data_elem = False
                                else:

                                    year_field = date_component_data[output_col_data][
                                        "year"
                                    ]
                                    month_field = date_component_data[output_col_data][
                                        "month"
                                    ]
                                    day_field = date_component_data[output_col_data][
                                        "day"
                                    ]
                                    tgtarray[tgtcolmap[year_field]] = str(dt.year)
                                    tgtarray[tgtcolmap[month_field]] = str(dt.month)
                                    tgtarray[tgtcolmap[day_field]] = str(dt.day)

                                    tgtarray[tgtcolmap[output_col_data]] = source_date

                            elif (
                                output_col_data in date_col_data
                            ):  # date_col_data for key $K$ is where $only_date(srcdata[K])$ should be copied and is there for all dates

                                # this fork of the if/else seems to be for non-birthdates which're handled differently

                                # copy the full value into this "full value"
                                tgtarray[tgtcolmap[output_col_data]] = source_date

                                # select the first 10 chars which will be YYYY-MM-DD
                                tgtarray[tgtcolmap[date_col_data[output_col_data]]] = (
                                    source_date[:10]
                                )

                    if valid_data_elem:
                        tgtrecords.append(tgtarray)
    else:
        metrics.increment_key_count(
            source=srcfilename,
            fieldname=srcfield,
            tablename=tgtfilename,
            concept_id="all",
            additional="",
            count_type="invalid_source_fields",
        )

    return build_records, tgtrecords, metrics


def valid_value(item):
    """
    Check if an item is non blank (null)
    """
    if item.strip() == "":
        return False
    return True


# DATE TESTING
# ------------
# I started by changing the get_datetime_value to be neater.
# I think it should be handled all as one thing, but I've spent too much time doing this already


def valid_date_value(item):
    """
    Check if a date item is non null and parses as ISO (YYYY-MM-DD), reverse-ISO
    or dd/mm/yyyy or mm/dd/yyyy
    """
    if item.strip() == "":
        return False
    if (
        not valid_iso_date(item)
        and not valid_reverse_iso_date(item)
        and not valid_uk_date(item)
    ):
        logger.warning("Bad date : `{0}`".format(item))
        return False
    return True


def get_datetime_value(item):
    """
    Check if a date item is non-null and parses as ISO (YYYY-MM-DD), reverse-ISO (DD-MM-YYYY),
    or UK format (DD/MM/YYYY).
    Returns a datetime object if successful, None otherwise.
    """
    date_formats = [
        "%Y-%m-%d",  # ISO format (YYYY-MM-DD)
        "%d-%m-%Y",  # Reverse ISO format (DD-MM-YYYY)
        "%d/%m/%Y",  # UK old-style format (DD/MM/YYYY)
    ]

    for date_format in date_formats:
        try:
            return datetime.datetime.strptime(item, date_format)
        except ValueError:
            continue

    # If we get here, none of the formats worked
    return None


def normalise_to8601(item: str) -> str:
    """parses, normalises, and formats a date value using regexes

    could use just one regex but that seems bad.
    """

    if not isinstance(item, str):
        raise Exception("can only normliase a string")

    both = item.split(" ")

    match = re.match(r"(?P<year>\d{4})[-/](?P<month>\d{2})[-/](?P<day>\d{2})", both[0])
    if not match:
        match = re.match(
            r"(?P<day>\d{2})[-/](?P<month>\d{2})[-/](?P<year>\d{4})", both[0]
        )

    if not match:
        raise Exception(f"invalid date format {item=}")

    data = match.groupdict()
    year, month, day = data["year"], data["month"], data["day"]
    value = str(int(year)).zfill(4)
    value += "-"
    value += str(int(month)).zfill(2)
    value += "-"
    value += str(int(day)).zfill(2)
    value += " "

    if 2 == len(both):
        match = re.match(
            r"(?P<hour>\d{2}):(?P<minute>\d{2})(:(?P<second>\d{2})(\.\d{6})?)?", both[1]
        )
        data = match.groupdict()
        hour, minute, second = data["hour"], data["minute"], data["second"]

        # concat the time_suffix
        if hour is not None:
            if minute is None:
                raise Exception(
                    f"unrecognized format seems to have 'hours' but not 'minutes' {item=}"
                )

            value += str(int(hour)).zfill(2)
            value += ":"
            value += str(int(minute)).zfill(2)
            value += ":"
            value += str(int(second if second is not None else "0")).zfill(2)

    if ":" not in value:
        value += "00:00:00"

    return value


def valid_iso_date(item):
    """
    Check if a date item is non null and parses as ISO (YYYY-MM-DD)
    """
    try:
        datetime.datetime.strptime(item, "%Y-%m-%d")
    except ValueError:
        return False

    return True


def valid_reverse_iso_date(item):
    """
    Check if a date item is non null and parses as reverse ISO (DD-MM-YYYY)
    """
    try:
        datetime.datetime.strptime(item, "%d-%m-%Y")
    except ValueError:
        return False

    return True


def valid_uk_date(item):
    """
    Check if a date item is non null and parses as UK format (DD/MM/YYYY)
    """
    try:
        datetime.datetime.strptime(item, "%d/%m/%Y")
    except ValueError:
        return False

    return True


# End of date code


def load_last_used_ids(last_used_ids_file: Path, last_used_ids):
    fh = last_used_ids_file.open(mode="r", encoding="utf-8-sig")
    csvr = csv.reader(fh, delimiter="\t")

    for last_ids_data in csvr:
        last_used_ids[last_ids_data[0]] = int(last_ids_data[1]) + 1

    fh.close()
    return last_used_ids


def load_saved_person_ids(person_file: Path):
    fh = person_file.open(mode="r", encoding="utf-8-sig")
    csvr = csv.reader(fh, delimiter="\t")
    last_int = 1
    person_ids = {}

    next(csvr)
    for persondata in csvr:
        person_ids[persondata[0]] = persondata[1]
        last_int += 1

    fh.close()
    return person_ids, last_int


def load_person_ids(
    saved_person_id_file, person_file, mappingrules, use_input_person_ids, delim=","
):
    person_ids, person_number = get_person_lookup(saved_person_id_file)

    fh = person_file.open(mode="r", encoding="utf-8-sig")
    csvr = csv.reader(fh, delimiter=delim)
    person_columns = {}
    person_col_in_hdr_number = 0
    reject_count = 0

    personhdr = next(csvr)
    logger.info(personhdr)

    # Make a dictionary of column names vs their positions
    for col in personhdr:
        person_columns[col] = person_col_in_hdr_number
        person_col_in_hdr_number += 1

    ## check the mapping rules for person to find where to get the person data) i.e., which column in the person file contains dob, sex
    birth_datetime_source, person_id_source = mappingrules.get_person_source_field_info(
        "person"
    )
    logger.info(
        "Load Person Data {0}, {1}".format(birth_datetime_source, person_id_source)
    )

    ## get the column index of the PersonID from the input file
    person_col = person_columns[person_id_source]

    for persondata in csvr:
        if not valid_value(
            persondata[person_columns[person_id_source]]
        ):  # just checking that the id is not an empty string
            reject_count += 1
            continue
        if not valid_date_value(persondata[person_columns[birth_datetime_source]]):
            reject_count += 1
            continue
        if (
            persondata[person_col] not in person_ids
        ):  # if not already in person_ids dict, add it
            if use_input_person_ids == "N":
                person_ids[persondata[person_col]] = str(
                    person_number
                )  # create a new integer person_id
                person_number += 1
            else:
                person_ids[persondata[person_col]] = str(
                    persondata[person_col]
                )  # use existing person_id
    fh.close()

    return person_ids, reject_count


def check_dir_isvalid(directory: Path, create_if_missing: bool = False) -> None:
    """Check if directory is valid, optionally create it if missing.

    Args:
        directory: Directory path as string or tuple
        create_if_missing: If True, create directory if it doesn't exist
    """

    ## check directory has been set
    if directory is None:
        logger.warning("Directory not provided.")
        sys.exit(1)

    ## if not a directory, create it if requested (including parents. This option is for the output directory only).
    if not directory.is_dir():
        if create_if_missing:
            try:
                ## deliberately not using the exist_ok option, as we want to know whether it was created or not to provide different logger messages.
                directory.mkdir(parents=True)
                logger.info(f"Created directory: {directory}")
            except OSError as e:
                logger.warning(f"Failed to create directory {directory}: {e}")
                sys.exit(1)
        else:
            logger.warning(f"Not a directory, dir {directory}")
            sys.exit(1)


def set_saved_person_id_file(
    saved_person_id_file: Path | None, output_dir: Path
) -> Path:
    """check if there is a saved person id file set in options - if not, check if the file exists and remove it"""

    if saved_person_id_file is None:
        saved_person_id_file = output_dir / "person_ids.tsv"
        if saved_person_id_file.is_dir():
            logger.exception(
                f"the detected saved_person_id_file {saved_person_id_file} is already a dir"
            )
            sys.exit(1)
        if saved_person_id_file.exists():
            saved_person_id_file.unlink()
    else:
        if saved_person_id_file.is_dir():
            logger.exception(
                f"the passed saved_person_id_file {saved_person_id_file} is already a dir"
            )
            sys.exit(1)
    return saved_person_id_file


def check_files_in_rules_exist(
    rules_input_files: list[str], existing_input_files: list[str]
) -> None:
    for infile in existing_input_files:
        if infile not in rules_input_files:
            msg = (
                "WARNING: no mapping rules found for existing input file - {0}".format(
                    infile
                )
            )
            logger.warning(msg)
    for infile in rules_input_files:
        if infile not in existing_input_files:
            msg = "WARNING: no data for mapped input file - {0}".format(infile)
            logger.warning(msg)


def open_file(file_path: Path) -> tuple[IO[str], Iterator[list[str]]] | None:
    """opens a file and does something related to CSVs"""
    try:

        fh = file_path.open(mode="r", encoding="utf-8-sig")
        csvr = csv.reader(fh)
        return fh, csvr
    except IOError as e:
        logger.exception("Unable to open: {0}".format(file_path))
        logger.exception("I/O error({0}): {1}".format(e.errno, e.strerror))
        return None


def set_omop_filenames(
    omop_ddl_file: Path, omop_config_file: Path, omop_version: str
) -> tuple[Path, Path]:
    if (
        (omop_ddl_file is None)
        and (omop_config_file is None)
        and (omop_version is not None)
    ):
        omop_config_file = (
            importlib.resources.files("carrottransform") / "config/omop.json"
        )
        omop_ddl_file_name = "OMOPCDM_postgresql_" + omop_version + "_ddl.sql"
        omop_ddl_file = (
            importlib.resources.files("carrottransform") / "config" / omop_ddl_file_name
        )
    return omop_config_file, omop_ddl_file


def get_person_lookup(saved_person_id_file: Path) -> tuple[dict[str, str], int]:
    # Saved-person-file existence test, reload if found, return last used integer
    if saved_person_id_file.is_file():
        person_lookup, last_used_integer = load_saved_person_ids(saved_person_id_file)
    else:
        person_lookup = {}
        last_used_integer = 1
    return person_lookup, last_used_integer
