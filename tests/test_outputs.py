"""
runs tests for the target writer

# λ uv run pytest tests/test_outputs.py

"""

import io
import textwrap
from pathlib import Path

import boto3
import pytest
import sqlalchemy
from click.testing import CliRunner
from sqlalchemy import Column, MetaData, Table, Text, insert

from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs, sources
from tests.click_tools import package_root


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

    source = sources.SourceOpener(
        folder=Path(__file__).parent / "test_data/measure_weight_height/"
    )

    # open the three outputs
    targets = []
    for table in ["heights", "persons", "weights"]:
        iterator = source.open(f"{table}.csv")
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
    source = sources.SourceOpener(engine=engine)

    # re-read and verify
    for table in ["heights", "persons", "weights"]:
        # read what was inserted
        actual = ""
        for line in source.open(f"{table}.csv"):
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

    source = sources.SourceOpener(
        folder=Path(__file__).parent / "test_data/measure_weight_height/"
    )

    # open the three outputs
    targets = []
    for table in ["heights", "persons", "weights"]:
        iterator = source.open(f"{table}.csv")
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
    source = sources.SourceOpener(engine=engine)

    # re-read and verify
    for table in ["heights", "persons", "weights"]:
        # read what was inserted
        actual = ""
        for line in source.open(f"{table}.csv"):
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


def rand_hex(length: int = 16) -> str:
    """genearttes a random hex string. used for test data"""
    import random

    out = ""
    src = "0123456789abcdef"

    for i in range(0, length):
        out += src[random.randint(0, len(src) - 1)]

    return out


@pytest.mark.s3tests
def test_listing_a_folder():
    s3tool = outputs.S3Tool(boto3.client("s3"), "carrot-transform-testtt")

    # list contents and generate a name to use
    names_seen = s3tool.scan()
    names_seen.append("")
    test_name = ""
    while test_name in names_seen:
        test_name = "yass.test/test-" + rand_hex() + ".txt"

    # generate some content for the object
    body = "Hello from Python!\n" + rand_hex()

    # create the object
    s3tool.new_stream(test_name)  # start the object
    s3tool.send_chunk(
        test_name, body.encode("utf-8")
    )  # send some data about the object
    s3tool.complete_all()  # close all streams

    # check to see if the object's record is in place
    names_seen = s3tool.scan()
    assert test_name in names_seen

    # read the object's content and check it
    data = names_seen = s3tool.read(test_name)
    assert data == body

    # finally - delete the object
    s3tool.delete(test_name)

    names_seen = s3tool.scan()
    assert test_name not in names_seen


@pytest.mark.s3tests
def test_with_the_writer():
    """
    tests my adapter's ability to write to s3 buckets as if they're files
    """
    s3tool = outputs.S3Tool(boto3.client("s3"), "carrot-transform-testtt")

    outputTarget = outputs.s3OutputTarget(s3tool)

    # list contents and generate a name to use
    names_seen = s3tool.scan()
    names_seen.append("")
    test_name = ""
    while test_name in names_seen:
        test_name = "yass.test/test-" + rand_hex() + ".csv"

    # create the object
    handle = outputTarget.start(test_name, ["a", "b", "id"])

    # send data
    handle.write(["a1", "bb", "12"])
    handle.write(["a2", "bbb", "21"])
    handle.close()

    # check to see if the object's record is in place
    names_seen = s3tool.scan()
    assert test_name in names_seen

    # read the object's content and check it
    data = names_seen = s3tool.read(test_name)
    body = "\t".join(["a", "b", "id"])
    body += "\n"
    body += "\t".join(["a1", "bb", "12"])
    body += "\n"
    body += "\t".join(["a2", "bbb", "21"])
    body += "\n"
    assert data == body

    # finally - delete the object
    s3tool.delete(test_name)

    names_seen = s3tool.scan()
    assert test_name not in names_seen


@pytest.mark.s3tests
def test_s3run(
    tmp_path: Path,
):
    output = "s3:carrot-transform-testtt"

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
            "--input-dir",
            str(person_file.parent),
            "--rules-file",
            str(rules1_file),
            "--person-file",
            str(person_file),
            "--output-dir",
            output,
            "--omop-ddl-file",
            f"{package_root / 'config/OMOPCDM_postgresql_5.3_ddl.sql'}",
            "--omop-config-file",
            f"{package_root / 'config/config.json'}",
        ],
    )

    if result.exception is not None:
        raise (result.exception)

    message = "did that work?"
    message += f"\n\t{result.exit_code=}"

    raise Exception(message)

    # (patient_csv, persons, observations, measurements, conditions, post_check) = (
    #     # patient_csv
    #     "observe_smoking/demos.csv",
    #     # persons
    #     4,
    #     # observations
    #     {
    #         123: {
    #             "2018-01-01": {"3959110": "active"},
    #             "2018-02-01": {"3959110": "active"},
    #             "2018-03-01": {"3957361": "quit"},
    #             "2018-04-01": {"3959110": "active"},
    #             "2018-05-01": {"35821355": "never"},
    #         },
    #         456: {
    #             "2009-01-01": {"35821355": "never"},
    #             "2009-02-01": {"35821355": "never"},
    #             "2009-03-01": {"3957361": "quit"},
    #         },
    #     },
    #     # measurements
    #     None,
    #     # conditions
    #     None,
    #     None,  # no extra post-test checks
    # )

    # from tests import test_integration

    # s3tool = outputs.S3Tool(boto3.client("s3"), "carrot-transform-testtt")
    # outputTarget = outputs.s3OutputTarget(s3tool)

    # test_integration.test_fixture(
    #     # output = 's3://carrot-transform-testtt',
    #     engine=True,
    #     pass__input__as_arg=True,
    #     pass__rules_file__as_arg=True,
    #     pass__person_file__as_arg=True,
    #     pass__output_dir__as_arg=True,
    #     pass__omop_ddl_file__as_arg=True,
    #     pass__omop_config_file__as_arg=True,
    #     tmp_path=tmp_path,
    #     patient_csv=patient_csv,
    #     persons=persons,
    #     observations=observations,
    #     measurements=measurements,
    #     conditions=conditions,
    #     post_check=post_check,
    # )
