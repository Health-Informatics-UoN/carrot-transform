import csv
import sys
from pathlib import Path
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.validation import valid_value, valid_date_value
from carrottransform.tools.mappingrules import MappingRules

logger = logger_setup()


def load_last_used_ids(last_used_ids_file: Path, last_used_ids):
    fh = last_used_ids_file.open(mode="r", encoding="utf-8-sig")
    csvr = csv.reader(fh, delimiter="\t")

    for last_ids_data in csvr:
        last_used_ids[last_ids_data[0]] = int(last_ids_data[1]) + 1

    fh.close()
    return last_used_ids


def load_person_ids(
    saved_person_id_file,
    person_file,
    mappingrules: MappingRules,
    use_input_person_ids,
    delim=",",
):
    person_ids, person_number = _get_person_lookup(saved_person_id_file)

    fh = person_file.open(mode="r", encoding="utf-8-sig")
    csvr = csv.reader(fh, delimiter=delim)
    person_columns = {}
    person_col_in_hdr_number = 0
    reject_count = 0
    # Header row of the person file
    personhdr = next(csvr)
    # TODO: not sure if this is needed
    logger.info("Headers in Person file: %s", personhdr)

    # Make a dictionary of column names vs their positions
    for col in personhdr:
        person_columns[col] = person_col_in_hdr_number
        person_col_in_hdr_number += 1

    ## check the mapping rules for person to find where to get the person data) i.e., which column in the person file contains dob, sex
    birth_datetime_source, person_id_source = mappingrules.get_person_source_field_info(
        "person"
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


# TODO: understand the purpose of this function and simplify it
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


def _get_person_lookup(saved_person_id_file: Path) -> tuple[dict[str, str], int]:
    # Saved-person-file existence test, reload if found, return last used integer
    if saved_person_id_file.is_file():
        person_lookup, last_used_integer = _load_saved_person_ids(saved_person_id_file)
    else:
        person_lookup = {}
        last_used_integer = 1
    return person_lookup, last_used_integer


def _load_saved_person_ids(person_file: Path):
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
