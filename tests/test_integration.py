"""
these are various integration tests for carroti using pytest and the inbuild click tools

"""

import logging
import re
from enum import Enum
from pathlib import Path

import pytest
import sqlalchemy
from click.testing import CliRunner

import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
import tests.conftest as conftest
import tests.testools as testools
from carrottransform.cli.subcommands.run import launch_v2, mapstream
from tests.conftest import TrinoSchema

logger = logging.getLogger(__name__)
test_data = Path(__file__).parent / "test_data"


class Connection(Enum):
    CSV = "csv"
    SQLITE = "sqlite"
    MINIO = "minio"
    POSTGRES = "postgresql"
    TRINO = "trino"


@pytest.mark.unit  # it's an integration test ... but i need/want one that i can check quickly
def test_sql_read(tmp_path: Path):
    """
    this test checks to be sure that two measurements (width and height) don't "collide" or interfere with eachother
    """

    # this is the paramter
    testing_person_file = "measure_weight_height/persons.csv"

    test_case = testools.CarrotTestCase(testing_person_file, entry=mapstream)

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


#### ==========================================================================
## Test case generation - generates a mixutre of test cases with diffenret example data combinations, connection combination, and ways to pass arguments

test_cases = list(
    map(
        lambda person: testools.CarrotTestCase(person, mapstream),
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

test_cases += [
    testools.CarrotTestCase(
        "integration_test1/src_PERSON.csv",
        entry=launch_v2,
        mapper=str(Path(__file__).parent / "test_V2/rules-v2.json"),
        suffix="/v2-out",
    ),
    testools.CarrotTestCase(
        "only_m/patients.csv",
        entry=mapstream,
        mapper=str(test_data / "only_m/v1-rules.json"),
        suffix="/v1-out",
    ),
]

pass__arg_names = [
    "inputs",
    "rules-file",
    "person",
    "output",
    "omop-ddl-file",
]

# the common "easy" connections
connection_types = [Connection.CSV, Connection.SQLITE]


def generate_cases(needs: list[Connection] | None = None):
    """generate a lot of permutations of tests.

    @param types - the types of connection to read and write from/into
    @param needs - if set, connections need to involve these

    """

    # the common "easy" connections that don't require a fixture
    types = [Connection.CSV, Connection.SQLITE]

    # add all needed to types
    if needs is not None:
        for n in needs:
            if n not in types:
                types.append(n)

    # variations of the connections and test case data
    parameters = testools.permutations(
        input_from=types, test_case=test_cases, output_to=types
    )

    # variations of wether to pass things asn CLI or environment variables
    varts = list(map(lambda v: {"pass_as": v}, testools.variations(pass__arg_names)))

    # filter function - check if a config satsifies the need
    def valid(p):
        # if "needs" was defined we "need" one of the entries in it
        has = (needs is None) or (p["output_to"] in needs) or (p["input_from"] in needs)

        # to make testing easy; don't read and write to the same thing
        different = p["output_to"] != p["input_from"]

        return has and different

    return [
        (case["output_to"], case["test_case"], case["input_from"], case["pass_as"])
        for case in testools.zip_loop(list(p for p in parameters if valid(p)), varts)
    ]


#### ==========================================================================
## test front ends; these all have different permutations of the fixtures so that they can pass them all to the final test function


@pytest.mark.parametrize("output_to, test_case, input_from, pass_as", generate_cases())
@pytest.mark.integration
def test_function(
    request,
    tmp_path: Path,
    output_to: Connection,
    test_case,
    input_from: Connection,
    pass_as,
):
    """performs the basic tests"""
    body_of_test(request, tmp_path, output_to, test_case, input_from, pass_as)


@pytest.mark.parametrize(
    "output_to, test_case, input_from, pass_as",
    generate_cases(needs=[Connection.TRINO]),
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
    generate_cases(needs=[Connection.MINIO]),
)
@pytest.mark.docker
def test_function_minio(
    request,
    tmp_path: Path,
    output_to: Connection,
    test_case,
    input_from: Connection,
    pass_as,
    minio,
):
    """performs tests with minio included"""
    body_of_test(
        request, tmp_path, output_to, test_case, input_from, pass_as, minio=minio
    )


@pytest.mark.parametrize(
    "output_to, test_case, input_from, pass_as",
    generate_cases(needs=[Connection.POSTGRES]),
)
@pytest.mark.docker
def test_function_postgresql(
    request, tmp_path: Path, output_to, test_case, input_from, pass_as, postgres
):
    """performs tests with postgres included"""
    body_of_test(
        request, tmp_path, output_to, test_case, input_from, pass_as, postgres=postgres
    )


#### ==========================================================================
## This is the main test function


def body_of_test(
    request,
    tmp_path: Path,
    output_to: Connection,
    test_case,
    input_from: Connection,
    pass_as,
    minio: conftest.MinIOBucket | None = None,
    postgres: conftest.PostgreSQLContainer | None = None,
    trino: TrinoSchema | None = None,
):
    """the main integration test. uses a given test case using given input/output techniques and then compares it to known results"""

    logger.setLevel(logging.DEBUG)

    # generat a semi-random slug/name to group test data under
    # the files we read/write to s3 will appear in this folder
    slug = (
        re.sub(r"[^a-zA-Z0-9]+", "_", request.node.name).strip("_")
        + "__"
        + testools.rand_hex()
    )

    logger.info(f"test path is {str(tmp_path)=}\n\t{slug=}")

    # set the input
    inputs: str | None = None
    source_engine: sqlalchemy.engine.Engine | None = None
    match input_from:
        case Connection.SQLITE:
            inputs = test_case.load_sqlite(tmp_path)

        case Connection.CSV:
            inputs = str(test_case._folder).replace("\\", "/")

        case Connection.MINIO:
            assert minio is not None
            # create the connection string
            inputs = minio.connection

            # copy data into the thing
            outputTarget = outputs.minio_output_target(inputs)
            testools.copy_across(ot=outputTarget, so=test_case._folder, names=None)

        case Connection.TRINO:
            assert trino is not None
            inputs = trino.connection
            # "hold the door" - need this connection to remain active to avoid the trino instance being deleted
            source_engine = sqlalchemy.create_engine(inputs)
            testools.copy_across(
                ot=outputs.sql_output_target(sqlalchemy.create_engine(inputs)),
                so=test_case._folder,
                names=None,
            )

        case Connection.POSTGRES:
            assert postgres is not None
            inputs = postgres.config.connection
            outputTarget = outputs.sql_output_target(sqlalchemy.create_engine(inputs))
            testools.copy_across(ot=outputTarget, so=test_case._folder, names=None)

    assert inputs is not None, f"couldn't use {input_from=}"  # check inputs as set

    # set the output
    output: str | None = None
    output_engine: sqlalchemy.engine.Engine | None = None
    match output_to:
        case Connection.SQLITE:
            output = f"sqlite:///{(tmp_path / 'output.sqlite3').absolute()}"
        case Connection.CSV:
            output = str((tmp_path / "out").absolute())
        case Connection.MINIO:
            assert minio is not None
            # just connect to it
            output = minio.connection
        case Connection.POSTGRES:
            assert postgres is not None
            output = postgres.config.connection
        case Connection.TRINO:
            assert trino is not None
            output = trino.connection
            # "hold the door" - need this connection to remain active to avoid the trino instance being deleted
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
    result = runner.invoke(test_case._entry, args=args, env=env)

    if result.exception is not None:
        print(result.exception)
        raise (result.exception)

    assert 0 == result.exit_code

    # get the results so we can compare them to the expectations
    results = None
    match output_to:
        case Connection.CSV:
            results = sources.csv_source_object(tmp_path / "out", sep="\t")
        case Connection.MINIO:
            results = sources.minio_source_object(output, sep="\t")
        case Connection.POSTGRES | Connection.TRINO | Connection.SQLITE:
            results = sources.sql_source_object(output)

    assert results is not None  # check output was set

    # verify that the results are good
    try:
        test_case.compare_to_tsvs(results)
    except:
        logger.error(f"{tmp_path=}")
        raise

    # dispose of these ... really *just* so we're doing something with them and the type checker is happy
    if source_engine is not None:
        source_engine.dispose()
    if output_engine is not None:
        output_engine.dispose()
