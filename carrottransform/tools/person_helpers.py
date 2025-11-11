import csv
import sys
from pathlib import Path
from typing import Iterator, Optional

from sqlalchemy.engine import Connection
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql.expression import select

import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
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
    mappingrules: MappingRules,
    inputs: sources.SourceObject,
    person: str,
    output: outputs.OutputTarget,
):
    # we used to try and load these, but, that's not happening now
    person_ids = {}
    person_number = 1

    saved_person_id_file: Path | None  # self.output_dir / "person_ids.tsv"
    person_file: Path | None = None
    person_table_name: str | None = None
    use_input_person_ids: str = "N"
    delim: str = ","
    db_connection: Optional[Connection] = None
    schema: Optional[str] = None

    #
    # so now ... load all existing persons?
    fh = inputs.open(person)
    csvr = fh  # TODO; rename this
    person_table_column_headers: list[str] = next(csvr)

    reject_count = 0

    # Make a dictionary of column names vs their positions
    person_columns: dict[str, int] = {}
    person_col_in_hdr_number = 0
    for column_headers in person_table_column_headers:
        person_columns[column_headers] = person_col_in_hdr_number
        person_col_in_hdr_number += 1
    person_col_in_hdr_number = None

    ## check the mapping rules for person to find where to get the person data) i.e., which column in the person file contains dob, sex
    birth_datetime_source, person_id_source = mappingrules.get_person_source_field_info(
        "person"
    )

    ## get the column index of the PersonID from the input file
    person_col = person_columns[person_id_source]

    # copy the records
    for person_data_row in csvr:
        person_id = person_data_row[person_col]

        if not valid_value(
            person_id
        ):  # just checking that the id is not an empty string
            reject_count += 1
            continue

        if not valid_date_value(
            str(person_data_row[person_columns[birth_datetime_source]])
        ):
            reject_count += 1
            continue

        if person_id not in person_ids:
            if use_input_person_ids == "N":
                # create a new integer person_id
                person_ids[person_id] = str(person_number)
                person_number += 1
            else:
                # use existing person_id
                person_ids[person_id] = str(person_id)

    return person_ids, reject_count


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
