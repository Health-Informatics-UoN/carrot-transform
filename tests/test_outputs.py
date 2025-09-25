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
from sqlalchemy import Column, MetaData, Table, Text, insert

from carrottransform.tools import outputs, sources


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


"""

                                # write the line to the file
                                fhd[tgtfile].write("\t".join(outrecord) + "\n")


        ## Initialise output files (adding them to a dict), output a header for each
        ## these aren't being closed deliberately
        for tgtfile in output_files:
            fhd[tgtfile] = (
                (output_dir / tgtfile).with_suffix(".tsv").open(mode=write_mode)
            )
            if write_mode == "w":
                outhdr = omopcdm.get_omop_column_list(tgtfile)
                fhd[tgtfile].write("\t".join(outhdr) + "\n")
            ## maps all omop columns for each file into a dict containing the column name and the index
            ## so tgtcolmaps is a dict of dicts.
            tgtcolmaps[tgtfile] = omopcdm.get_omop_column_map(tgtfile)
    """
