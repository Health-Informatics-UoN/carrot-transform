"""
these are various integration tests for carroti using pytest and the inbuild click tools

"""

import logging
import re
from enum import Enum
from pathlib import Path

import boto3
import pytest
import sqlalchemy
from click.testing import CliRunner

import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
import tests.csvrow as csvrow
import tests.testools as testools
from carrottransform.cli.subcommands.run import mapstream
from tests.conftest import TrinoSchema

logger = logging.getLogger(__name__)
test_data = Path(__file__).parent / "test_data"

V1TestCase = testools.CarrotTestCase


@pytest.mark.unit  # it's an integration test ... but i need/want one that i can check quickly
def test_sql_read(tmp_path: Path):
    """
    this test checks to be sure that two measurements (width and height) don't "collide" or interfere with eachother
    """

    # this is the paramter
    testing_person_file = "measure_weight_height/persons.csv"

    test_case = V1TestCase(testing_person_file)

    # run the test sourcing that SQLite database but writing to disk
    input_db = test_case.load_sqlite(tmp_path)
    output_to = tmp_path / "out"
    testools.run_v1(
        inputs=input_db,
        person=test_case._person,
        mapper=test_case._mapper,
        output=str(output_to),
    )

    # cool; now verify that the on-disk results are good
    actual = sources.csv_source_object(output_to, sep="\t")
    test_case.compare_to_tsvs(actual)


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
) + [
    testools.CarrotTestCase(
        "only_m/patients.csv", str(test_data / "only_m/v1-rules.json"), "/v1-out"
    ),
]

pass__arg_names = [
    "inputs",
    "rules-file",
    "person",
    "output",
    "omop-ddl-file",
]


class Connection(Enum):
    CSV = "csv"
    SQLITE = "sqlite"
    TRINO = "trino"
    S3 = "s3"


def generate_tests(types: list[Connection], needs: None | list[Connection]):
    parameters = testools.permutations(
        input_from=types,
        test_case=list(
            map(
                testools.CarrotTestCase,
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
        ),
        output_to=types,
    )

    pass_vars_as = list(
        map(lambda v: {"pass_as": v}, testools.variations(pass__arg_names))
    )

    def valid(p):
        # if "needs" was defined we "need" one of the entries in it
        has = (needs is None) or (p["output_to"] in needs) or (p["input_from"] in needs)

        # to make testing easy; don't read and write to the same thing
        dif = p["output_to"] != p["input_from"]

        return has and dif

    return [
        (case["output_to"], case["test_case"], case["input_from"], case["pass_as"])
        for case in testools.zip_loop(
            list(p for p in parameters if valid(p)), pass_vars_as
        )
    ]


@pytest.mark.parametrize(
    "output_to, test_case, input_from, pass_as",
    generate_tests(
        [Connection.CSV, Connection.SQLITE, Connection.S3],
        [Connection.S3],
    ),
)
@pytest.mark.s3tests
def test_function_w_s3(
    request, tmp_path: Path, output_to, test_case, input_from, pass_as
):
    """dumb wrapper to make the s3 tests run as well as the integration tests"""
    body_of_test(request, tmp_path, output_to, test_case, input_from, pass_as)

    # generat a semi-random slug/name to group test data under
    # the files we read/write to s3 will appear in this folder


@pytest.mark.parametrize(
    "output_to, test_case, input_from, pass_as",
    generate_tests(
        [Connection.CSV, Connection.SQLITE, Connection.TRINO], [Connection.TRINO]
    ),
)
@pytest.mark.docker
def test_function_trino(
    request, tmp_path: Path, output_to, test_case, input_from, pass_as, trino
):
    """dumb wrapper to make the s3 tests run as well as the integration tests"""
    body_of_test(
        request,
        tmp_path,
        output_to,
        test_case,
        input_from,
        pass_as,
        trino=trino,
    )


@pytest.mark.parametrize(
    "output_to, test_case, input_from, pass_as",
    generate_tests([Connection.CSV, Connection.SQLITE], None),
)
@pytest.mark.integration
def test_function(request, tmp_path: Path, output_to, test_case, input_from, pass_as):
    body_of_test(request, tmp_path, output_to, test_case, input_from, pass_as)


def body_of_test(
    request,
    tmp_path: Path,
    output_to: Connection,
    test_case,
    input_from: Connection,
    pass_as,
    trino: TrinoSchema | None = None,
):
    """the main integration test. uses a given test case using given input/output techniques and then compares it to known results"""

    # generat a semi-random slug/name to group test data under
    # the files we read/write to s3 will appear in this folder

    slug = (
        re.sub(r"[^a-zA-Z0-9]+", "_", request.node.name).strip("_")
        + "__"
        + testools.rand_hex()
    )

    logger.info(f"test path is {str(tmp_path)=}\n\t{slug=}")

    # set the input
    inputs: None | str = None
    source_engine: None | sqlalchemy.engine.Engine = (
        # we need to retain a connection to a trino instance when the catalouge is in memory.
        # if we don't then the catalouge is deleted
        None
    )

    if input_from == Connection.CSV:
        inputs = str(test_case._folder).replace("\\", "/")

    elif input_from == Connection.SQLITE:
        inputs = test_case.load_sqlite(tmp_path)

    elif input_from == Connection.S3:
        # create a random s3 subfolder
        inputs = f"s3:{testools.CARROT_TEST_BUCKET}/{slug}/input"

        # set a task to delete the subfolder on exit
        request.addfinalizer(lambda: testools.delete_s3_folder(inputs))

        # copy data into the thing
        outputTarget = outputs.s3_output_target(inputs)
        testools.copy_across(ot=outputs.s3_output_target(inputs), so=test_case._folder, names=None)

    elif input_from == Connection.TRINO:
        assert trino is not None
        inputs = trino.connection
        source_engine = sqlalchemy.create_engine(inputs)
        outputTarget = outputs.sql_output_target(sqlalchemy.create_engine(inputs))
        testools.copy_across(ot=outputs.sql_output_target(sqlalchemy.create_engine(inputs)), so=test_case._folder, names=None)

    assert inputs is not None, f"couldn't use {input_from=}"  # check inputs as set

    # set the output
    output: None | str = None
    output_engine: None | sqlalchemy.engine.Engine = (
        # we need to retain a connection to a trino instance when the catalouge is in memory.
        # if we don't then the catalouge is deleted
        None
    )

    if output_to == Connection.CSV:
        output = str((tmp_path / "out").absolute())

    elif output_to == Connection.SQLITE:
        output = f"sqlite:///{(tmp_path / 'output.sqlite3').absolute()}"

    elif output_to == Connection.S3:
        # create a random s3 subfolder
        output = f"s3:{testools.CARROT_TEST_BUCKET}/{slug}/output"

        # set a task to delete the subfolder on exit
        request.addfinalizer(lambda: testools.delete_s3_folder(output))

    elif output_to == Connection.TRINO:
        assert trino is not None
        output = trino.connection
        output_engine = sqlalchemy.create_engine(output)

    assert output is not None, f"couldn't use {output_to=}"  # check output was set

    env, args = testools.passed_as(
        pass_as,
        "--inputs",
        inputs,
        "--rules-file",
        test_case._mapper,
        "--person",
        test_case._person,
        "--output",
        output,
        "--omop-ddl-file",
        "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
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
    if output_to == Connection.CSV:
        results = sources.csv_source_object(tmp_path / "out", sep="\t")

    elif (output_to == Connection.SQLITE) or (output_to == Connection.TRINO):
        results = sources.sql_source_object(sqlalchemy.create_engine(output))

    elif output_to == Connection.S3:
        results = sources.s3_source_object(output, sep="\t")

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
            "--person",
            "demographics_mother_gold",  # person_file.name,
            "--output",
            str(tmp_path),
            "--omop-ddl-file",
            "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
        ],
    )

    assert result.exit_code == -1

    message = caplog.text.splitlines(keepends=False)[-1]

    assert message.strip().endswith(
        "Person properties were mapped from (['demographics_child_gold.csv', 'infant_data_gold.csv']) but can only come from the person file person='demographics_mother_gold'"
    )

    assert "-1" == str(result.exception)
