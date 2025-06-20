"""
regression tests
"""

from carrottransform.cli.subcommands.run import *
import pytest
from unittest.mock import patch
import importlib.resources
import logging

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream




@pytest.mark.unit
def test_mireda_fake(tmp_path: Path):
    """
    test the mireda data - but - i've renamed all the date columns to have the same name
    """


    # Get the package root directory
    assets = importlib.resources.files("tests")
    
    input_dir = assets / 'regression/mireda/fake'
    input_rules = input_dir / 'rules.json'
    input_person = input_dir / 'demographics_mother_gold.csv'

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--input-dir",
            f"{input_dir}",
            "--rules-file",
            f"{input_rules}",
            "--person-file",
            f"{input_person}",

            "--output-dir",
            f"{tmp_path}",

            "--omop-version", "5.3"
        ],
    )

    if None is not result.exception:
        print(result.output)
        raise result.exception

    assert 0 == result.exit_code

@pytest.mark.unit
def test_mireda(tmp_path: Path):


    # Get the package root directory
    assets = importlib.resources.files("tests")
    
    input_dir = assets / 'regression/mireda'
    input_rules = input_dir / 'rules.json'
    input_person = input_dir / 'demographics_mother_gold.csv'

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--input-dir",
            f"{input_dir}",
            "--rules-file",
            f"{input_rules}",
            "--person-file",
            f"{input_person}",

            "--output-dir",
            f"{tmp_path}",

            "--omop-version", "5.3"
        ],
    )

    if None is not result.exception:
        print(result.output)
        raise result.exception

    assert 0 == result.exit_code