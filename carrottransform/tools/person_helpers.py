import csv
import sys
from pathlib import Path
from typing import Iterator, Optional

from sqlalchemy.engine import Connection
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql.expression import select

from carrottransform.tools.logger import logger_setup
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.validation import valid_date_value, valid_value

logger = logger_setup()


def load_last_used_ids(last_used_ids_file: Path, last_used_ids):
    fh = last_used_ids_file.open(mode="r", encoding="utf-8-sig")
    csvr = csv.reader(fh, delimiter="\t")

    for last_ids_data in csvr:
        last_used_ids[last_ids_data[0]] = int(last_ids_data[1]) + 1

    fh.close()
    return last_used_ids


def load_person_ids_v2(
    saved_person_id_file,
    person_file: Path | None,
    person_table_name: str | None,
    mappingrules: MappingRules,
    use_input_person_ids: str,
    delim=",",
    db_connection: Optional[Connection] = None,
    schema: Optional[str] = None,
):
    person_ids, person_number = _get_person_lookup(saved_person_id_file)

    if db_connection and person_table_name:
        person_table_model = Table(
            person_table_name,
            MetaData(schema=schema),
            autoload_with=db_connection,
        )
        personhdr = person_table_model.columns.keys()
        person_table_data = db_connection.execute(select(person_table_model)).fetchall()
    elif person_file:
        fh = person_file.open(mode="r", encoding="utf-8-sig")
        csvr = csv.reader(fh, delimiter=delim)
        personhdr = next(csvr)
    else:
        raise ValueError("No person file or person table name provided")

    person_columns = {}
    person_col_in_hdr_number = 0
    reject_count = 0

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

    for persondata in person_table_data if db_connection else csvr:
        if not valid_value(
            persondata[person_columns[person_id_source]]
        ):  # just checking that the id is not an empty string
            reject_count += 1
            continue
        if not valid_date_value(str(persondata[person_columns[birth_datetime_source]])):
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
    if not db_connection:
        fh.close()

    return person_ids, reject_count


def load_person_ids(
    saved_person_id_file: Path,
    person_file: Path,
    mappingrules: MappingRules,
    use_input_person_ids: bool,
    delim: str = ",",
):
    """`old` loading method that accepts a Path object pointing to the file along witrh a delimeter.

    this is used to preserve the old API for the current v2 testing
    """
    return read_person_ids(
        saved_person_id_file=saved_person_id_file,
        csvr=csv.reader(
            person_file.open(mode="r", encoding="utf-8-sig"), delimiter=delim
        ),
        mappingrules=mappingrules,
        use_input_person_ids=use_input_person_ids,
    )


def read_person_ids(
    saved_person_id_file: Path,
    csvr: Iterator[list[str]],
    mappingrules: MappingRules,
    use_input_person_ids: bool,
):
    """revised loading method that accepts an itterator eitehr for a file or for a database connection"""

    if not isinstance(use_input_person_ids, bool):
        raise Exception(
            f"use_input_person_ids needs to be bool but it was {type(use_input_person_ids)=}"
        )
    if not isinstance(csvr, Iterator):
        raise Exception(f"csvr needs to be iterable but it was {type(csvr)=}")

    person_ids, person_number = _get_person_lookup(saved_person_id_file)

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
            if not use_input_person_ids:
                person_ids[persondata[person_col]] = str(
                    person_number
                )  # create a new integer person_id
                person_number += 1
            else:
                person_ids[persondata[person_col]] = str(
                    persondata[person_col]
                )  # use existing person_id

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
