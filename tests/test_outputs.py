"""
runs tests for the target writer

# λ uv run pytest tests/test_outputs.py

"""

import io
import logging
import textwrap
from pathlib import Path
import carrottransform.tools.sources

import boto3
import pytest
import sqlalchemy
from click.testing import CliRunner
from sqlalchemy import Column, MetaData, Table, Text, insert

from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs, sources
from tests.click_tools import package_root
import tests.testools as testools
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




@pytest.mark.s3tests
def test_listing_a_folder():
    s3tool = outputs.S3Tool(boto3.client("s3"), "carrot-transform-testtt")

    # list contents and generate a name to use
    names_seen = s3tool.scan()
    names_seen.append("")
    test_name = ""
    while test_name in names_seen:
        test_name = "yass.test/test-" + testools.rand_hex() + ".txt"

    # generate some content for the object
    body = "Hello from Python!\n" + testools.rand_hex()

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
        test_name = "yass.test/test-" + testools.rand_hex() + ".csv"

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
def test_s3read():
    # compare_to_tsv(
    #     'observe_smoking',
    #     None # ... so far ...
    # )

    # open the saved .tsv file
    so_expected = sources.csvSourceObject(
        package_root.parent / "tests/test_data/observe_smoking/", sep="\t"
    )

    # open the computed s3 file
    # https://signin.aws.amazon.com/signin?redirect_uri=https%3A%2F%2Feu-west-2.console.aws.amazon.com%2Fs3%2Fbuckets%2Fcarrot-transform-testtt%3FbucketType%3Dgeneral%26hashArgs%3D%2523%26isauthcode%3Dtrue%26oauthStart%3D1760127248281%26region%3Deu-west-2%26state%3DhashArgsFromTB_eu-west-2_046fc5458fe464a1%26tab%3Dobjects&client_id=arn%3Aaws%3Asignin%3A%3A%3Aconsole%2Fs3tb&forceMobileApp=0&code_challenge=M8oQyzNBHlAsVLKU1kuH9lJj16_QN7mmL81UXB7cVo8&code_challenge_method=SHA-256
    so_actual = sources.s3SourceObject("carrot-transform-testtt", sep="\t")

    import itertools

    for e, a in itertools.zip_longest(
        so_expected.open("observation"), so_actual.open("observation")
    ):
        assert e is not None
        assert a is not None

        assert e == a

    testools.compare_to_tsvs(
        "observe_smoking", sources.s3SourceObject("carrot-transform-testtt", sep="\t")
    )


@pytest.mark.s3tests
def test_s3run(
    tmp_path: Path,
    caplog,
):
    caplog.set_level(logging.INFO)

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
        "observe_smoking", sources.s3SourceObject("carrot-transform-testtt", sep="\t")
    )
