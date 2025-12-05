"""
runs tests for the target writer

# λ uv run pytest tests/test_outputs.py

"""

import logging
import textwrap
from pathlib import Path

import pytest
import sqlalchemy

from carrottransform.tools import outputs, sources

logger = logging.getLogger(__name__)


@pytest.mark.unit
def test_csv_output_target(tmp_path: Path):
    target = outputs.csv_output_target(tmp_path)

    csv = target.start("foo", ["a", "b"])

    csv.write(["1", "2"])
    csv.write(["three", "4.0"])

    csv.close()

    with open(tmp_path / "foo.tsv", "r") as file:
        text = file.read().strip()
        assert (
            text
            == textwrap.dedent("""
            a	b
            1	2
            three	4.0
        """).strip()
        )


@pytest.mark.unit
def test_sqliteTargetWriter(tmp_path: Path):
    heights = Path(__file__).parent / "test_data/measure_weight_height/heights.csv"
    # persons = Path(__file__).parent / "test_data/measure_weight_height/persons.csv"
    # weights = Path(__file__).parent / "test_data/measure_weight_height/weights.csv"

    # connect to a database
    engine: sqlalchemy.engine.Engine = sqlalchemy.create_engine(
        f"sqlite:///{(tmp_path / 'testing.db').absolute()}"
    )

    # create the target
    outputTarget = outputs.sql_output_target(engine)

    source: sources.SourceObject = sources.csv_source_object(
        Path(__file__).parent / "test_data/measure_weight_height/", ","
    )

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
        import random

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
        with open(heights.parent / f"{table}.csv", "r") as file:
            expected = file.read().strip()

        # compare the two values
        assert expected == actual, f"mismatch in {table}"


@pytest.mark.unit
def test_in_and_out_sqlite(tmp_path: Path):
    heights = Path(__file__).parent / "test_data/measure_weight_height/heights.csv"
    # persons = Path(__file__).parent / "test_data/measure_weight_height/persons.csv"
    # weights = Path(__file__).parent / "test_data/measure_weight_height/weights.csv"

    # connect to a database
    engine: sqlalchemy.engine.Engine = sqlalchemy.create_engine(
        f"sqlite:///{(tmp_path / 'testing.db').absolute()}"
    )

    # create a writer
    outputTarget = outputs.sql_output_target(engine)

    source: sources.SourceObject = sources.csv_source_object(
        Path(__file__).parent / "test_data/measure_weight_height/", ","
    )

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
        import random

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
        with open(heights.parent / f"{table}.csv", "r") as file:
            expected = file.read().strip()

        # compare the two values
        assert expected == actual, f"mismatch in {table}"


@pytest.mark.unit
def test_join():
    header = ["a", "b", "c"]

    assert "a\tb\tc\n" == ("\t".join(header) + "\n")
