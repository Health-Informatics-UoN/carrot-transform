import random
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from carrottransform.tools import outputs, sources


@pytest.mark.docker
def test_connection(postgres, tmp_path: Path):
    """
    this test case tests the connection to a database and the ability to read and write to it.



    it does this by:
    - load
        - connecting to a database
        - creating a target and source
        - moving records randomly into the database
    - flip
        - "flipping" the databse connection so we're reading from it
        - re-reading the records back to verify they are identical to the on-disk copy

    """

    test_data: Path = Path(__file__).parent / "test_data/measure_weight_height"
    table_names: list[str] = ["heights", "persons", "weights"]

    ## load

    # connect to a database
    # Create engine and connection
    engine = create_engine(postgres.config.connection)

    # create the target
    outputTarget: outputs.OutputTarget = outputs.sql_output_target(engine)

    # create the source
    source: sources.SourceObject = sources.csv_source_object(test_data, ",")

    # open the three outputs
    targets = []
    for table in table_names:
        iterator = source.open(table)
        header = next(iterator)
        targets.append((outputTarget.start(table, header), iterator))

    # randomly move records
    # i want to be sure that multiple read/write things can be active at once
    while 0 != len(targets):
        # select a random index

        index = random.randint(0, len(targets) - 1)

        # select a rangom one to move
        (target, iterator) = targets[index]

        # get a record to move, or, remove this target if it's already been finished
        try:
            record = next(iterator)
        except StopIteration:
            targets.pop(index)
            target.close()
            continue

        # move the record
        target.write(record)

    ## flip

    # create a source that connects to the database we just filled
    source = sources.sql_source_object(engine)

    # re-read and verify
    for table in table_names:
        # read what was inserted; aggregate it into a big string (makes diffs easier)
        actual = ""
        for line in source.open(table):
            actual += ",".join(line) + "\n"
        actual = actual.strip()

        # read the raw expected data as a big string
        with open(test_data / f"{table}.csv", "r") as file:
            expected = file.read().strip()

        # compare the two values
        assert expected == actual, f"mismatch in {table}"
