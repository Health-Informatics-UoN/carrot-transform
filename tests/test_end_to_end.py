"""
tests the complete system in a few ways. needs better verification of the outputs
"""

import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from carrottransform.cli.subcommands.run import mapstream
from tests.testools import package_root


@pytest.mark.integration
def test_run_the_command_line():
    """simple test/check to see if the project can "run" - which is good for checking things like imports"""

    import os

    # we only care about the return value, and, subprocess kept failing
    assert 0 == os.system("uv run carrot-transform run mapstream --help")


@pytest.mark.unit
def test_no_args():
    runner = CliRunner()
    result = runner.invoke(mapstream, [])

    ##
    # check click results
    assert 2 == result.exit_code

    assert [
        "Usage: mapstream [OPTIONS]",
        "Try 'mapstream --help' for help.",
        "",
        "Error: Missing option '--person'.",  # this seems to change a lot
        "",
    ] == result.output.split("\n")


@pytest.mark.unit
def test_with_auto_person(tmp_path: Path):
    """
    do a full test without explicitly noting a person file - should detect and error
    """

    ##
    # setup test environment(ish) in the folder

    # rules from carrot mapper
    rules_src = package_root / "examples/test/rules/rules_14June2021.json"
    rules = tmp_path / "rules.json"
    shutil.copy2(rules_src, rules)

    # the source files
    for src in [
        "covid19_antibody.csv",
        "Covid19_test.csv",
        "Demographics.csv",
        "Symptoms.csv",
        "vaccine.csv",
    ]:
        shutil.copy2(package_root / "examples/test/inputs" / src, tmp_path / src)

    # output dir needs to be pre-created
    output = tmp_path / "out"
    output.mkdir()

    # ddl and config files (copied here rather than using embedded one ... for now?)
    ddl = tmp_path / "ddl.sql"
    omop = tmp_path / "config.json"
    shutil.copy2(package_root / "config/config.json", omop)
    shutil.copy2(package_root / "config/OMOPCDM_postgresql_5.3_ddl.sql", ddl)

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--input-dir",
            f"{tmp_path}",
            "--rules-file",
            f"{rules}",
            "--output-dir",
            f"{output}",
            "--omop-ddl-file",
            f"{tmp_path / 'ddl.sql'}",
            "--omop-config-file",
            f"{tmp_path / 'config.json'}",
        ],
    )

    ##
    # check click results

    assert None is not result.exception
    assert "2" == str(result.exception)
    assert 2 == result.exit_code
