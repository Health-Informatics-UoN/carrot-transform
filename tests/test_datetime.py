import re
from pathlib import Path

import pytest
import sqlalchemy
from click.testing import CliRunner
from sqlalchemy import Column, MetaData, Table, Text

import carrottransform.cli.subcommands.run as run
import tests.click_tools as click_tools
import tests.csvrow as csvrow
import tests.testools as testools
from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs, sources
from tests.click_tools import package_root, project_root
from tests.testools import package_root, project_root


@pytest.mark.unit
def test_datetime_in_measurement_csv(tmp_path: Path):
    testools.run_v1(
        # set the inputs directory
        inputs=str(package_root / "examples/test/inputs/"),
        # set the person file name
        person="Demographics",  # str(package_root / "examples/test/inputs/" / "Demographics.csv"),
        # set the path to the rules file
        mapper=str(package_root / "examples/test/rules/rules_14June2021.json"),
        # set/up the output directory
        output=str(tmp_path / "out"),
    )

    testools.compare_to_tsvs(
        "@carrot/examples/test/output",
        sources.csv_source_object(tmp_path / "out", "\t"),
    )


@pytest.mark.unit
def test_datetime_in_measurement_sqlite(tmp_path: Path):
    # set the sqlite dir
    sqlite_file: Path = tmp_path / "sql.db"
    sqlite_string = f"sqlite:///{sqlite_file}"

    # copy our example to the sqluite database
    testools.copy_across(
        outputs.sql_output_target(connection=sqlalchemy.create_engine(sqlite_string)),
        package_root / "examples/test/inputs/",
    )

    testools.run_v1(
        # set the inputs directory
        inputs=sqlite_string,
        # set the person file name
        person="Demographics",  # str("Demographics.csv"),
        # set the path to the rules file
        mapper=str(package_root / "examples/test/rules/rules_14June2021.json"),
        # set/up the output directory
        output=str(tmp_path / "out"),
    )

    testools.compare_to_tsvs(
        "@carrot/examples/test/output",
        sources.csv_source_object(tmp_path / "out", "\t"),
    )
