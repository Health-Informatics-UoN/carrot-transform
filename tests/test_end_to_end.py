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
def test_with_example(tmp_path: Path, caplog):
    ##
    # setup test environment(ish) in the folder

    # capture all
    caplog.set_level(logging.DEBUG)

    # Get the package root directory
    package_root = Path(importlib.resources.files('carrottransform'))

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
        "Symptoms.csv",
        "vaccine.csv",
    ]:
        shutil.copy2(package_root / "examples/test/inputs" / src, tmp_path / src)
    person = tmp_path / "Demographics.csv"

    # output dir needs to be pre-created
    output = tmp_path / "out"
    output.mkdir()

    # ddl and config files (copied here rather than using embedded one ... for now?)
    ddl = tmp_path / "ddl.sql"
    omop = tmp_path / "omop.json"
    shutil.copy2(package_root / "config/omop.json", omop)
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
            "--person-file",
            f"{person}",
            "--output-dir",
            f"{output}",
            "--omop-ddl-file",
            f"{tmp_path / 'ddl.sql'}",
            "--omop-config-file",
            f"{tmp_path / 'omop.json'}",
        ],
    )

    ##
    # check click results
    if None != result.exception:
        raise result.exception

    if 0 != result.exit_code:  # if it wasn't a pass ... echo output
        m = f"result.exit_code = {result.exit_code}"

        for l in result.output.split("\n"):
            m += "\n> " + l

        raise Exception(m)

    assert 0 == result.exit_code

    ##
    # check log messages
    for message in [
        "Loaded mapping rules from: ",
        "['PersonID', 'sex', 'date_of_birth', 'ethnicity']",
        "Load Person Data date_of_birth, PersonID",
        "person_id stats: total loaded 1000, reject count 0",
        "WARNING: no mapping rules found for existing input file - Covid19_test.csv",
        "WARNING: no mapping rules found for existing input file - vaccine.csv",
        "['Demographics.csv', 'Symptoms.csv', 'covid19_antibody.csv']",
        "observation, ethnicity, ['observation_concept_id~35825508', 'observation_source_concept_id~35825508', 'observation_source_value']",
        "observation, date_of_birth, ['observation_datetime']",
        "observation, PersonID, ['person_id']",
        "observation, ethnicity, ['observation_concept_id~35825531', 'observation_source_concept_id~35825531', 'observation_source_value']",
        "observation, date_of_birth, ['observation_datetime']",
        "observation, PersonID, ['person_id']",
        "observation, ethnicity, ['observation_concept_id~35826241', 'observation_source_concept_id~35826241', 'observation_source_value']",
        "observation, date_of_birth, ['observation_datetime']",
        "observation, PersonID, ['person_id']",
        "observation, ethnicity, ['observation_concept_id~35827394', 'observation_source_concept_id~35827394', 'observation_source_value']",
        "observation, date_of_birth, ['observation_datetime']",
        "observation, PersonID, ['person_id']",
        "observation, ethnicity, ['observation_concept_id~35825567', 'observation_source_concept_id~35825567', 'observation_source_value']",
        "observation, date_of_birth, ['observation_datetime']",
        "observation, PersonID, ['person_id']",
        "observation, ethnicity, ['observation_concept_id~35827395', 'observation_source_concept_id~35827395', 'observation_source_value']",
        "observation, date_of_birth, ['observation_datetime']",
        "observation, PersonID, ['person_id']",
        "person, date_of_birth, ['birth_datetime']",
        "person, PersonID, ['person_id']",
        "person, date_of_birth, ['birth_datetime']",
        "person, sex, {'F': ['gender_concept_id~8532', 'gender_source_concept_id~8532', 'gender_source_value'], 'M': ['gender_concept_id~8507', 'gender_source_concept_id~8507', 'gender_source_value']}",
        "person, PersonID, ['person_id']",
        "Processing input: Demographics.csv",
        "INPUT file data : Demographics.csv: input count 1000, time since start",
        "TARGET: observation: output count 900",
        "TARGET: person: output count 1000",
        "condition_occurrence, symptom1, ['condition_concept_id~254761', 'condition_source_concept_id~254761', 'condition_source_value']",
        "condition_occurrence, visit_date, ['condition_end_datetime', 'condition_start_datetime']",
        "condition_occurrence, PersonID, ['person_id']",
        "Processing input: Symptoms.csv",
        "INPUT file data : Symptoms.csv: input count 800, time since start",
        "TARGET: condition_occurrence: output count 400",
        "measurement, IgG, ['value_as_number', 'measurement_source_value', 'measurement_concept_id~37398191', 'measurement_source_concept_id~37398191']",
        "measurement, date, ['measurement_datetime']",
        "measurement, PersonID, ['person_id']",
        "Processing input: covid19_antibody.csv",
        "INPUT file data : covid19_antibody.csv: input count 1000, time since start",
        "TARGET: measurement: output count 1000",
    ]:
        assert message in caplog.text

    ##
    # check outputs in env
