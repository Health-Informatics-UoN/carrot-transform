import logging
from itertools import product
from pathlib import Path
from typing import Iterable

import boto3
import pytest
import sqlalchemy
from click.testing import CliRunner

from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs, sources
from carrottransform.tools.args import PathArg

#
logger = logging.getLogger(__name__)

# Get the package root directory
project_root: Path = Path(__file__).parent.parent
package_root: Path = project_root / "carrottransform"

# this reffers to an s3 bucket tied to the system-level credentials
# we're going to move ot MinIO at some point
CARROT_TEST_BUCKET = "carrot-transform-test"

#### ==========================================================================
## unit test cases - test the test functions


@pytest.mark.unit
def test_compare(caplog) -> None:
    """test the validator"""

    caplog.set_level(logging.INFO)

    path: Path = project_root / "tests/test_data/observe_smoking"

    compare_to_tsvs("observe_smoking", sources.csv_source_object(path, sep="\t"))


#### ==========================================================================
## verification functions


def compare_to_tsvs(subpath: str, actual: sources.SourceObject) -> None:
    """generate a source for the named subpath and compare all .tsv to the passed so

    open each .tsv in the tests subpath and compare it to the open'ed from the named SO.

    if the SO is missing a tsv? fail!
    if the SO has different rows? fail!
    if the SO has extra/too few rows? fail!
    if the SO has .tsv files we don't ... pass ...

    """

    test: Path
    if subpath.startswith("@carrot"):
        test = PathArg.convert(subpath, None, None)
    else:
        test = project_root / "tests/test_data" / subpath

    # find all
    items = [
        item.name[:-4]
        for item in test.glob("*.tsv")
        if "summary_mapstream.tsv" != item.name
    ]

    assert "person" in items, "person.tsv verification data missing from the test case"

    # we can't guarantee this - but - we still need it to verify the other tables
    assert "person_ids" in items, (
        "person_id.tsv verification data missing from the test case"
    )
    items.remove("person_ids")

    # open the saved .tsv file
    expect = sources.csv_source_object(test, sep="\t")

    compare_two_sources(expect=expect, actual=actual, items=items, simple_check=False)
    # it matches!
    logger.info(f"all match in {subpath=}")


def compare_two_sources(
    expect: sources.SourceObject,
    actual: sources.SourceObject,
    items: Iterable[str],
    simple_check: bool = True,
) -> None:
    """compares the named entries from two SourceObject instances. does not enforce order. has optional `simple_check` to control wether the presonIds are mapped back and if the row ids are ignored"""

    expected_persons = {} if simple_check else person_id_mapping(expect)
    actual_persons = {} if simple_check else person_id_mapping(actual)

    for name in items:
        # only check certain tables
        if not simple_check:
            assert name in [
                "measurement",
                "person",
                "observation",
                "condition_occurrence",
            ], f"not allowing {name=}"

        expect_iter = expect.open(name)
        actual_iter = actual.open(name)

        ex_head = next(expect_iter)
        ac_head = next(actual_iter)

        assert ex_head == ac_head, f"mismatch in {name=}\n\t{ex_head=}\n\t{ac_head=}"

        # check that the column name is person_id before we start swapping values
        if not simple_check:
            assert f"{name}_id" == ex_head[0], (
                f"expected {name}_id as first column but was {ex_head[0]}"
            )
            if name != "person":
                assert "person_id" == ex_head[1], (
                    f"expected person_id as second column but was {ex_head[1]}"
                )

        def values(data, persons):
            rows = []
            for row in data:
                if not simple_check:
                    # remove the row's id
                    # > we con't care about the speicific id value in this column, but, it changes when the records come out of the db in a random order
                    if name != "person":
                        row = row[1:]

                    # change the person_id back
                    # > trino (and likley larger systems) don't preserve insertion/extraction order of rows; so we can't rely on the anon ids popping out the same as they went in
                    row[0] = persons[row[0]]

                rows.append(row)
            rows.sort(key=lambda row: str(row))
            return rows

        expect_values = values(expect_iter, expected_persons)
        actual_values = values(actual_iter, actual_persons)

        if expect_values == actual_values:
            continue

        expect_msg = "\texpect:"
        n = 0
        for each in expect_values:
            expect_msg += f"\n\t{n}\t{each}"
            n += 1
        actual_msg = "\tactual:"
        n = 0
        for each in actual_values:
            actual_msg += f"\n\t{n}\t{each}"
            n += 1

        assert expect_values == actual_values, (
            f"mismatch with item {name}\n{ex_head}\n{expect_msg}\n{actual_msg}"
        )


def person_id_mapping(source: sources.SourceObject) -> dict[str, str]:
    """reads back a `person_ids` and determines how to "unmap" anonymisation"""
    persons: dict[str, str] = {}
    first: bool = True
    for row in source.open("person_ids"):
        if first:
            first = False
            row = list(map(lambda col: col.upper(), row))
            assert row == ["SOURCE_SUBJECT", "TARGET_SUBJECT"], (
                f"wrong row in person_id {row=}"
            )
        else:
            persons[row[1]] = row[0]
    assert not first
    return persons


#### ==========================================================================
## test case functions


def keyed_variations(**kv):
    """given some things passed as k=[v1,v2], yields all permutations"""
    keys = kv.keys()
    for values in product(*kv.values()):
        yield dict(zip(keys, values))


#### ==========================================================================
## utility functions


def variations(keys):
    """
    computes key -> bool dicts where all, none, or only one value is true or false, then, maps those dicts to simple lists
    """

    def permutations(keys):
        count = len(keys)
        assert count > 0
        if count == 1:
            yield {keys[0]: True}
            yield {keys[0]: False}
        else:
            key = keys[0]
            for permutation in permutations(keys[1:]):
                permutation = permutation.copy()
                permutation[key] = True
                yield permutation
                permutation = permutation.copy()
                permutation[key] = False
                yield permutation

    for permutation in permutations(list(keys)):
        values = list(permutation.values())
        true_count = values.count(True)
        false_count = values.count(False)

        if (true_count in [0, 1]) or (false_count in [0, 1]):
            output = []
            for key in permutation:
                if permutation[key]:
                    output += [key]
            yield output


def permutations(**name_to_list):
    """given a map of lists; yield all permutations of the contents"""

    def loop(listing):
        count = len(listing)

        if count == 0:
            return

        head_key, head_items = listing[0]

        if count == 1:
            for value in head_items:
                yield {head_key: value}

            return

        for tail in loop(listing[1:]):
            for v in head_items:
                tail[head_key] = v
                yield tail.copy()

    for item in loop(list(map(lambda k: (k, name_to_list[k]), name_to_list.keys()))):
        yield item


def zip_loop(*arguments: list[dict]):
    # convert them all to lists so that they're "stable"
    args_as_lists = list(list(arg) for arg in arguments)

    # find the longest length
    max_length = max(len(arg_list) for arg_list in args_as_lists)

    def loop(arg_list):
        """loops through an arg_list forever"""
        while True:
            for item in arg_list:
                yield item

    # turn them all into forever loops
    args_loops = [loop(arg) for arg in args_as_lists]

    # now build "rows" from each. keep going until we've built "count" rows and hit everything in each arg_list
    count = 0
    while count < max_length:
        count += 1

        # start an empty row
        row: dict = {}

        # each of those inputs will contribute some {k:v} so we union them togehter
        for arg_loop in args_loops:
            row = row | next(arg_loop).copy()

        # yield this row before we continue
        yield row


def copy_across(ot: outputs.OutputTarget, so: sources.SourceObject | Path, names=None):
    assert isinstance(so, Path) == (names is None)
    if isinstance(so, Path):
        names = [file.name[:-4] for file in so.glob("*.csv")]
        so = sources.csv_source_object(path=so, sep=",")
    assert isinstance(so, sources.SourceObject)

    # copy all named ones across
    for name in names:
        input = so.open(name)
        output = None

        for r in input:
            if output is None:
                output = ot.start(name, r)
            else:
                output.write(r)
        # output.close()
        # input.close()

    #
    so.close()
    ot.close()


def run_v1(
    inputs: str,
    person: str,
    mapper: str,
    output: str,
):
    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--inputs",
            inputs,
            "--rules-file",
            mapper,
            "--person",
            person,
            "--output",
            output,
            "--omop-ddl-file",
            "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
        ],
    )

    if result.exception is not None:
        print(result.exception)
        raise (result.exception)

    assert 0 == result.exit_code


def rand_hex(length: int = 16) -> str:
    """genearttes a random hex string. used for test data"""
    import random

    out = ""
    src = "0123456789abcdef"

    for i in range(0, length):
        out += src[random.randint(0, len(src) - 1)]

    return out


test_data = Path(__file__).parent / "test_data"


class CarrotTestCase:
    """defines an integration test case in terms of the person file, and the optional mapper rules"""

    def __init__(self, person_name: str, mapper: str = "", suffix=""):
        self._suffix = suffix
        self._person_name = person_name

        self._folder = (test_data / person_name).parent

        # find the rules mapping
        if mapper == "":
            for json in self._folder.glob("*.json"):
                assert "" == mapper
                mapper = str(json).replace("\\", "/")
        assert "" != mapper
        self._mapper = mapper

        assert 1 == person_name.count("/")
        [label, person] = person_name.split("/")
        self._label = label
        assert person.endswith(".csv")
        self._person = person[:-4]

    def load_sqlite(self, tmp_path: Path):
        assert tmp_path.is_dir()

        # create an SQLite database and copy the contents into it
        sqlite3 = tmp_path / f"{self._label}.sqlite3"
        copy_across(
            ot=outputs.sql_output_target(
                sqlalchemy.create_engine(f"sqlite:///{sqlite3.absolute()}")
            ),
            so=self._folder,
        )
        return f"sqlite:///{sqlite3.absolute()}"

    def compare_to_tsvs(self, source, suffix=""):
        compare_to_tsvs(self._label + self._suffix, source)


##
# build the env and arg parameters
def passed_as(pass_as, *args):
    args = list(args)

    env = {}
    i = 0  # index in the args list

    while i < len(args):
        # parameters should all be of the form "--name" with "value" afterwards in the array
        parameter_key = args[i][2:]

        if parameter_key not in pass_as:
            i += 2
            continue

        # convert the key
        parameter_key = parameter_key.upper().replace("-", "_")

        # get the value
        parameter_value = args[i + 1]

        # save it to the evn vars
        env[parameter_key] = parameter_value

        # demove the key and value from teh list
        args = args[:i] + args[(i + 2) :]

    return (env, args)


def delete_s3_folder(coordinate):
    """
    Delete a folder and all its contents from an S3 bucket.

    Args:
        bucket (str): Name of the S3 bucket
        folder (str): Folder path to delete (e.g., 'my-folder/' or 'prefix/subfolder/')
    """

    [bucket, folder] = outputs.s3_bucket_folder(coordinate)

    client = boto3.client("s3")

    # Ensure the folder path ends with a slash
    if not folder.endswith("/"):
        folder = folder + "/"

    # List all objects in the folder
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=folder)

    # Collect all objects to delete
    objects_to_delete = []
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                objects_to_delete.append({"Key": obj["Key"]})

    if not objects_to_delete:
        logger.info(f"No objects found in folder '{folder}'")
        return

    # Delete all objects in batches of 1000 (S3 API limit)
    for i in range(0, len(objects_to_delete), 1000):
        batch = objects_to_delete[i : i + 1000]
        response = client.delete_objects(Bucket=bucket, Delete={"Objects": batch})

        # Check for errors in deletion
        if "Errors" in response and response["Errors"]:
            logger.info(f"Errors deleting some objects: {response['Errors']}")

    logger.info(f"Successfully deleted folder '{folder}' and its contents")
