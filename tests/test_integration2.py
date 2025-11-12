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

import carrottransform.cli.subcommands.run_v2 as run_v2
import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
import tests.click_tools as click_tools
import tests.csvrow as csvrow
import tests.testools as testools

logger = logging.getLogger(__name__)
test_data = Path(__file__).parent / "test_data"


v2TestCases = [
    testools.CarrotTestCase(
        "integration_test1/src_PERSON.csv",
        mapper=str(test_data.parent / "test_V2/rules-v2.json"),
    )
]


@pytest.mark.parametrize("test_case", v2TestCases)
@pytest.mark.integration
def test_v2(request, tmp_path: Path, test_case: testools.CarrotTestCase):
    # future test case parameters
    output_to = "csv"  # f"s3:{testools.CARROT_TEST_BUCKET}"
    input_from = "csv"
    main_entry = run_v2.folder
    test_suffix = "/v2-out"

    test_case = test_case

    # generat a semi-random slug/name to group test data under
    # the files we read/write to s3 will appear in this folder
    import re

    slug = (
        re.sub(r"[^a-zA-Z0-9]+", "_", request.node.name).strip("_")
        + "__"
        + testools.rand_hex()
    )

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
        request.addfinalizer(lambda: testools.delete_s3_folder(inputs))

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
        request.addfinalizer(lambda: testools.delete_s3_folder(output))

    assert output is not None, f"couldn't use {output_to=}"  # check output was set

    env, args = testools.passed_as(
        [],
        "--inputs",
        inputs,
        "--rules-file",
        test_case._mapper,
        "--person",
        test_case._person,
        "--output",
        output,
    )

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(main_entry, args=args, env=env)

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
    test_case.compare_to_tsvs(results, test_suffix)
