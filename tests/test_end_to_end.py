"""
tests the complete system in a few ways. needs better verification of the outputs
"""

import pytest
import importlib.resources
import logging

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream


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
        "Error: Missing option '--rules-file'.",
        "",
    ] == result.output.split("\n")


@pytest.mark.unit
def test_with_two_dirs(tmp_path: Path, caplog):
    ##
    # setup test environment(ish) in the folder

    # capture all
    caplog.set_level(logging.DEBUG)

    # create teh two input directories
    input1 = tmp_path / "in1"
    input2 = tmp_path / "in2"
    input1.mkdir()
    input2.mkdir()

    # Get the package root directory
    package_root = importlib.resources.files("carrottransform")
    package_root = (
        package_root if isinstance(package_root, Path) else Path(str(package_root))
    )

    # rules from carrot mapper
    rules_src = package_root / "examples/test/rules/rules_14June2021.json"
    rules = tmp_path / "rules.json"
    shutil.copy2(rules_src, rules)

    # the source files
    # ... i'm not renaming these since i'm not sure what would happen if i did
    for src in [
        "covid19_antibody.csv",
        "Covid19_test.csv",
    ]:
        shutil.copy2(Path("carrottransform/examples/test/inputs") / src, input1 / src)
    for src in [
        "Demographics.csv",
        "Symptoms.csv",
        "vaccine.csv",
    ]:
        shutil.copy2(Path("carrottransform/examples/test/inputs") / src, input2 / src)
    person = input2 / "Demographics.csv"

    # output dir needs to be pre-created
    output = tmp_path / "out"
    output.mkdir()

    # ddl and config files (copied here rather than using embedded one ... for now?)
    ddl = tmp_path / "ddl.sql"
    omop = tmp_path / "config.json"
    shutil.copy2("carrottransform/config/config.json", omop)
    shutil.copy2("carrottransform/config/OMOPCDM_postgresql_5.3_ddl.sql", ddl)

    ##
    # run click
    result = CliRunner().invoke(
        mapstream,
        [
            "--input-dir",  ## the first input dir is ignored in favour of the last
            f"{input1}",
            "--rules-file",
            f"{rules}",
            "--person-file",
            f"{person}",
            "--output-dir",
            f"{output}",
            "--omop-ddl-file",
            f"{tmp_path / 'ddl.sql'}",
            "--omop-config-file",
            f"{tmp_path / 'config.json'}",
            "--input-dir",
            f"{input2}",
        ],
    )

    ##
    # click has caught exceptions
    if result.exception is None:
        raise Exception("that test should have failed")

    # check the exception type and parameter
    import carrottransform.tools.sources as sources

    assert isinstance(result.exception, sources.SourceFileNotFoundException)
    exception: sources.SourceFileNotFoundException = result.exception
    assert exception._name == "covid19_antibody.csv"


@pytest.mark.unit
def test_with_one_csv_missing(tmp_path: Path, caplog):
    ##
    # setup test environment(ish) in the folder

    ###
    # arrange
    ###

    # capture all
    caplog.set_level(logging.DEBUG)

    # Get the package root directory
    package_root = importlib.resources.files("carrottransform")
    package_root = (
        package_root if isinstance(package_root, Path) else Path(str(package_root))
    )

    # rules from carrot mapper
    rules_src = package_root / "examples/test/rules/rules_14June2021.json"
    rules = tmp_path / "rules.json"
    shutil.copy2(rules_src, rules)

    # the source files
    # ... i'm not renaming these since i'm not sure what would happen if i did
    for src in [
        "covid19_antibody.csv",
        "Covid19_test.csv",
        "Demographics.csv",
        # "Symptoms.csv", # just skip this file
        "vaccine.csv",
    ]:
        shutil.copy2(package_root / "examples/test/inputs" / src, tmp_path / src)
    person = tmp_path / "Demographics.csv"

    # output dir needs to be pre-created
    output = tmp_path / "out"
    output.mkdir()

    # ddl and config files (copied here rather than using embedded one ... for now?)
    ddl = tmp_path / "ddl.sql"
    omop = tmp_path / "config.json"
    shutil.copy2(package_root / "config/config.json", omop)
    shutil.copy2(package_root / "config/OMOPCDM_postgresql_5.3_ddl.sql", ddl)

    ###
    # act
    ###

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
            "--person-file",
            f"{person}",
            "--output-dir",
            f"{output}",
            "--omop-ddl-file",
            f"{tmp_path / 'ddl.sql'}",
            "--omop-config-file",
            f"{tmp_path / 'config.json'}",
        ],
    )

    ###
    # assert
    ###

    ##
    # check click results
    if result.exception is None:
        raise Exception("this test should have failed")

    import carrottransform.tools.sources as sources

    if not isinstance(result.exception, sources.SourceFileNotFoundException):
        # i can't get the catch of "csvr = source.open(srcfilename)" to work
        raise Exception(f"{result.exception=}")

    exception: sources.SourceFileNotFoundException = result.exception
    assert exception._name == "Symptoms.csv"


@pytest.mark.unit
def test_with_auto_person(tmp_path: Path):
    """
    do a full test without explicitly noting a person file - should detect and error
    """

    ##
    # setup test environment(ish) in the folder

    # Get the package root directory
    package_root = importlib.resources.files("carrottransform")
    package_root = (
        package_root if isinstance(package_root, Path) else Path(str(package_root))
    )

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
