import random
from pathlib import Path

import pytest
from sqlalchemy import create_engine

from carrottransform.tools import outputs, sources


@pytest.mark.docker
def test_targetWriter(postgres, tmp_path: Path):
    test_data: Path = Path(__file__).parent / "test_data/measure_weight_height"

    # connect to a database
    # Create engine and connection
    engine = create_engine(postgres.config.connection)

    # create the target
    outputTarget: outputs.OutputTarget = outputs.sql_output_target(engine)

    # create the source
    source: sources.SourceObject = sources.csv_source_object(test_data, ",")

    # open the three outputs
    targets = []
    for table in ["heights", "persons", "weights"]:
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

    # create a source
    source = sources.sql_source_object(engine)

    # re-read and verify
    for table in ["heights", "persons", "weights"]:
        # read what was inserted
        actual = ""
        for line in source.open(table):
            actual += ",".join(line) + "\n"
        actual = actual.strip()

        # read the raw expected
        with open(test_data / f"{table}.csv", "r") as file:
            expected = file.read().strip()

        # compare the two values
        assert expected == actual, f"mismatch in {table}"
