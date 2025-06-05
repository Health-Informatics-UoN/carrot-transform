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

def click_transform(tmp_path: Path, limit: int = -1):
    """sets up the/a test environment and runs the transform thing with it.

    """


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
        with open(package_root / "examples/test/inputs" / src) as s, open(tmp_path / src, 'w') as o:
            for s in (s.readlines() if limit < 0 else s.readlines()[:(limit+1)]):
                o.write(s)
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
