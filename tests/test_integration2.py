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
import carrottransform.cli.subcommands.run_v2 as run_v2

logger = logging.getLogger(__name__)
test_data = Path(__file__).parent / "test_data"


v2TestCases = [
    testools.CarrotTestCase(
        "integration_test1/src_PERSON.csv", mapper= str(test_data.parent / "test_V2/rules-v2.json")
    )
]



@pytest.mark.parametrize("test_case", v2TestCases)
@pytest.mark.integration
def test_v2(tmp_path:Path, test_case: testools.CarrotTestCase):

    output = tmp_path / "out"

    inputs = str(test_case._folder).replace("\\", "/")

    env, args = testools.passed_as([],
        "--inputs",
        inputs,
        "--rules-file",
        test_case._mapper,
        "--person",
        test_case._person,
        "--output",
        output,
        # "--omop-ddl-file",
        # "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
    )

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(run_v2.folder, args=args, env=env)

    if result.exception is not None:
        print(result.exception)
        raise (result.exception)

    assert 0 == result.exit_code

    raise Exception(f"{test_case}")
