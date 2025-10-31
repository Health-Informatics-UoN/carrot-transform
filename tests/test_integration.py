"""
these are various integration tests for carroti using pytest and the inbuild click tools

"""

import logging
import re
from pathlib import Path

import boto3
import pytest
import sqlalchemy
from click.testing import CliRunner

import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
import tests.click_tools as click_tools
import tests.csvrow as csvrow
import tests.testools as testools
from carrottransform.cli.subcommands.run import mapstream

logger = logging.getLogger(__name__)
test_data = Path(__file__).parent / "test_data"


class V1TestCase:
    """defines a V1 integration test case in terms of the person file"""

    def __init__(self, person_name: str):
        self._person_name = "measure_weight_height/persons.csv"

        self._folder = (test_data / person_name).parent

        # find the rules mapping
        mapper = ""
        for json in self._folder.glob("*.json"):
            assert "" == mapper
            mapper = str(json).replace("\\", "/")
        assert "" != mapper
        self._mapper = mapper

        assert 1 == person_name.count("/")
        [label, person] = person_name.split("/")
        self._label = label
        self._person = person

    def load_sqlite(self, tmp_path: Path):
        assert tmp_path.is_dir()

        # create an SQLite database and copy the contents into it
        sqlite3 = tmp_path / f"{self._label}.sqlite3"
        testools.copy_across(
            ot=outputs.sqlOutputTarget(
                sqlalchemy.create_engine(f"sqlite:///{sqlite3.absolute()}")
            ),
            so=self._folder,
        )
        return f"sqlite:///{sqlite3.absolute()}"

    def compare_to_tsvs(self, source):
        testools.compare_to_tsvs(self._label, source)


@pytest.mark.unit  # it's an integration test ... but i need/want one that i can check quickly
def test_sql_read(tmp_path: Path):
    """
    this test checks to be sure that two measurements (width and height) don't "collide" or interfere with eachother
    """

    # this is the paramter
    testing_person_file = "measure_weight_height/persons.csv"

    test_case = V1TestCase(testing_person_file)

    # run the test sourcing that SQLite database but writing to disk
    testools.run_v1(
        inputs=test_case.load_sqlite(tmp_path),
        person=test_case._person,
        mapper=test_case._mapper,
        output=str((tmp_path / "out").absolute()),
    )

    # cool; now verify that the on-disk results are good
    test_case.compare_to_tsvs(sources.csvSourceObject(tmp_path / "out", sep="\t"))


v1TestCases = list(
    map(
        V1TestCase,
        [
            "integration_test1/src_PERSON.csv",
            "floats/src_PERSON.csv",
            "duplications/src_PERSON.csv",
            "mapping_person/demos.csv",
            "observe_smoking/demos.csv",
            "measure_weight_height/persons.csv",
            "condition/persons.csv",
        ],
    )
)

pass__arg_names = [
    "inputs",
    "rules-file",
    "person-file",
    "output",
    "omop-ddl-file",
    "omop-config-file",
]


connection_types = ["csv", "sqlite"]
connection_types_w_s3 = connection_types + [f"s3:{testools.CARROT_TEST_BUCKET}"]


def generate_cases(with_s3: bool):
    types = connection_types_w_s3 if with_s3 else connection_types

    perts = testools.permutations(
        input_from=types, test_case=v1TestCases, output_to=types
    )
    varts = map(lambda v: {"pass_as": v}, testools.variations(pass__arg_names))

    return [
        (case["output_to"], case["test_case"], case["input_from"], case["pass_as"])
        for case in testools.repeating_unions(perts, varts)
    ]


@pytest.mark.parametrize(
    "output_to, test_case, input_from, pass_as", generate_cases(True)
)
@pytest.mark.s3tests
def test_function_w_s3(
    request, tmp_path: Path, output_to, test_case, input_from, pass_as
):
    """dumb wrapper to make the s3 tests run as well as the integration tests"""
    body_of_test(request, tmp_path, output_to, test_case, input_from, pass_as)


@pytest.mark.parametrize(
    "output_to, test_case, input_from, pass_as", generate_cases(False)
)
@pytest.mark.integration
def test_function(request, tmp_path: Path, output_to, test_case, input_from, pass_as):
    body_of_test(request, tmp_path, output_to, test_case, input_from, pass_as)


def body_of_test(request, tmp_path: Path, output_to, test_case, input_from, pass_as):
    """the main integration test. uses a given test case using given input/output techniques and then compares it to known results"""

    # generat a semi-random slug/name to group test data under
    # the files we read/write to s3 will appear in this folder
    import re

    slug = (
        re.sub(r"[^a-zA-Z0-9]+", "_", request.node.name).strip("_")
        + "__"
        + testools.rand_hex()
    )

    logger.info(f"test path is {tmp_path=}\n\t{slug=}")

    def delete_s3_folder(coordinate):
        """
        Delete a folder and all its contents from an S3 bucket.

        Args:
            bucket (str): Name of the S3 bucket
            folder (str): Folder path to delete (e.g., 'my-folder/' or 'prefix/subfolder/')
        """

        [bucket, folder] = outputs.s3BucketFolder(coordinate)

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

    # set the input
    inputs: None | str = None
    if "sqlite" == input_from:
        inputs = test_case.load_sqlite(tmp_path)
    if "csv" == input_from:
        inputs = str(test_case._folder).replace("\\", "/")
    if input_from.startswith("s3:"):
        # create a random s3 subfolder
        inputs = input_from + "/" + slug + "/input"

        # set a task to delete the subfolder on exit
        request.addfinalizer(lambda: delete_s3_folder(inputs))

        # copy data into the thing
        outputTarget = outputs.s3OutputTarget(inputs)
        testools.copy_across(ot=outputTarget, so=test_case._folder, names=None)
    assert inputs is not None, f"couldn't use {input_from=}"  # check inputs as set

    # set the output
    output: None | str = None
    if "csv" == output_to:
        output = str((tmp_path / "out").absolute())
    if "sqlite" == output_to:
        output = f"sqlite:///{(tmp_path / 'output.sqlite3').absolute()}"

    if output_to.startswith("s3:"):
        # create a random s3 subfolder
        output = output_to + "/" + slug + "/output"

        # set a task to delete the subfolder on exit
        request.addfinalizer(lambda: delete_s3_folder(output))

    assert output is not None, f"couldn't use {output_to=}"  # check output was set

    ##
    # build the env and arg parameters
    def passed_as(*args):
        args = list(args)

        env = {}
        i = 0

        while i < len(args):
            k = args[i][2:]

            if not (k in pass_as):
                i += 2
                continue

            # convert eh key
            k = k.upper().replace("-", "_")

            # get the value
            v = args[i + 1]

            # save it to the evn vars
            env[k] = v

            # demove the key and value from teh list
            args = args[:i] + args[(i + 2) :]

        return (env, args)

    env, args = passed_as(
        "--inputs",
        inputs,
        "--rules-file",
        test_case._mapper,
        "--person-file",
        test_case._person,
        "--output",
        output,
        "--omop-ddl-file",
        "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
        "--omop-config-file",
        "@carrot/config/config.json",
    )

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(mapstream, args=args, env=env)

    if result.exception is not None:
        print(result.exception)
        raise (result.exception)

    assert 0 == result.exit_code

    # get the results so we can compare them to the expectations
    results = None
    if "csv" == output_to:
        results = sources.csvSourceObject(tmp_path / "out", sep="\t")
    if "sqlite" == output_to:
        results = sources.sqlSourceObject(sqlalchemy.create_engine(output))
    if output_to.startswith("s3:"):
        results = sources.s3SourceObject(output, sep="\t")

    assert results is not None  # check output was set

    # verify that the results are good
    test_case.compare_to_tsvs(results)


@pytest.mark.integration
def test_mireda_key_error(tmp_path: Path, caplog):
    """this is the original buggy version that should trigger the key error"""

    # capture all
    caplog.set_level(logging.DEBUG)

    person_file = (
        Path(__file__).parent
        / "test_data/mireda_key_error/demographics_mother_gold.csv"
    )

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--inputs",
            str(person_file.parent),
            "--rules-file",
            str(person_file.parent / "original_rules.json"),
            "--person-file",
            person_file.name,
            "--output",
            str(tmp_path),
            "--omop-ddl-file",
            "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
            "--omop-config-file",
            "@carrot/config/config.json",
        ],
    )

    assert result.exit_code == -1

    message = caplog.text.splitlines(keepends=False)[-1]

    assert message.strip().endswith(
        "Person properties were mapped from (['demographics_child_gold.csv', 'infant_data_gold.csv']) but can only come from the person file person_file.name='demographics_mother_gold.csv'"
    )

    assert "-1" == str(result.exception)
