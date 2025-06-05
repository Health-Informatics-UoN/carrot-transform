from carrottransform.cli.subcommands.run import *
import pytest
from unittest.mock import patch
import importlib.resources
import logging

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream
import csvrow
import re

@pytest.mark.unit
def test_dateimes_in_persons(tmp_path: Path, caplog):
    ##
    # setup test environment(ish) in the folder

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
    # check the person.tsv created by the above steps
    people = list(csvrow.csv_rows(tmp_path / 'out/person.tsv', '\t'))
    assert 0 != len(people)
    for person in people:

        ##
        # concat the birtdatetime
        concat_birthdate = str(person.year_of_birth)
        concat_birthdate+= "-"
        concat_birthdate+= str(person.month_of_birth).rjust(2, "0")
        concat_birthdate+= "-"
        concat_birthdate+= str(person.day_of_birth).rjust(2, "0")

        assert person.birth_datetime.startswith(concat_birthdate), f"{person.birth_datetime=} shoudl start with {concat_birthdate=}"
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", person.birth_datetime
        ), f"{person.birth_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS`"
