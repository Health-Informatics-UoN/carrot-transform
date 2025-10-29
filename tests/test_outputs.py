"""
runs tests for the target writer

# λ uv run pytest tests/test_outputs.py

"""

import io
import logging
import textwrap
from pathlib import Path

import boto3
import pytest
import sqlalchemy
from click.testing import CliRunner
from sqlalchemy import Column, MetaData, Table, Text, insert

import carrottransform.tools.sources
import tests.testools as testools
from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs, sources
from tests.click_tools import package_root

logger = logging.getLogger(__name__)


@pytest.mark.unit
def test_csvOutputTarget(tmp_path: Path):
    target = outputs.csvOutputTarget(tmp_path)

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
    persons = Path(__file__).parent / "test_data/measure_weight_height/persons.csv"
    weights = Path(__file__).parent / "test_data/measure_weight_height/weights.csv"

    # connect to a database
    engine: sqlalchemy.engine.Engine = sqlalchemy.create_engine(
        f"sqlite:///{(tmp_path / 'testing.db').absolute()}"
    )

    # create the target
    outputTarget = outputs.sqlOutputTarget(engine)

    source: sources.SourceObject = sources.csvSourceObject(
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
    source = sources.sqlSourceObject(engine)

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
    persons = Path(__file__).parent / "test_data/measure_weight_height/persons.csv"
    weights = Path(__file__).parent / "test_data/measure_weight_height/weights.csv"

    # connect to a database
    engine: sqlalchemy.engine.Engine = sqlalchemy.create_engine(
        f"sqlite:///{(tmp_path / 'testing.db').absolute()}"
    )

    # create a writer
    outputTarget = outputs.sqlOutputTarget(engine)

    source: sources.SourceObject = sources.csvSourceObject(
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
    source = sources.sqlSourceObject(engine)

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


@pytest.mark.s3tests
def test_s3run(tmp_path: Path, caplog):
    caplog.set_level(logging.INFO)

    output = f"s3:{testools.CARROT_TEST_BUCKET}/s3run"

    # this file is the only real parameter
    person_file: Path = (
        package_root.parent / "tests/test_data/observe_smoking/demos.csv"
    )

    # cool; we fine the .json file and use it as rules
    rules1_file: Path | None = None
    for f in person_file.parent.glob("*.json"):
        if f.is_file():
            assert rules1_file is None
            rules1_file = f
    assert rules1_file is not None

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--inputs",
            str(person_file.parent),
            "--rules-file",
            str(rules1_file),
            "--person-file",
            str(person_file),
            "--output",
            output,
            "--omop-ddl-file",
            "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
            "--omop-config-file",
            "@carrot/config/config.json",
        ],
    )

    if result.exception is not None:
        raise (result.exception)

    ##
    # verify / assert
    testools.compare_to_tsvs(
        "observe_smoking", sources.s3SourceObject(output, sep="\t")
    )
