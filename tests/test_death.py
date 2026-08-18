"""
Regression test for the "death" table config fix.

config.json used to declare an auto-number field ("death_id") for the death
table, but the OMOP death table has no id column at all. That caused a
KeyError at runtime as soon as any death record was written. See
carrottransform/config/config.json's auto_number_field section.
"""

from pathlib import Path

import pytest
from click.testing import CliRunner

from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools.omopcdm import OmopCDM

test_data = Path(__file__).parent / "test_data" / "death"
package_root = Path(__file__).parent.parent / "carrottransform"


@pytest.mark.unit
def test_death_table_has_no_auto_number_field():
    """the death table has no id column in the OMOP DDL, so config.json must not
    declare an auto_number_field for it - that mismatch is what caused the crash"""

    omopcdm = OmopCDM(
        omopddl=package_root / "config/OMOPCDM_postgresql_5.3_ddl.sql",
        omopcfg=package_root / "config/config.json",
    )

    assert omopcdm.get_omop_auto_number_field("death") is None


@pytest.mark.unit
def test_death_records_are_written(tmp_path: Path):
    """full run through mapstream with a rules file that maps to the death table -
    this used to raise a KeyError while writing the first death record"""

    output = tmp_path / "out"
    output.mkdir()

    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--inputs",
            str(test_data),
            "--rules-file",
            str(test_data / "rules.json"),
            "--person",
            "patients",
            "--output",
            str(output),
            "--omop-ddl-file",
            "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
            "--omop-config-file",
            "@carrot/config/config.json",
        ],
    )

    if result.exception is not None:
        raise result.exception
    assert 0 == result.exit_code

    death_tsv = output / "death.tsv"
    assert death_tsv.exists()

    with death_tsv.open() as fh:
        lines = [line.rstrip("\n").split("\t") for line in fh]

    header, *rows = lines

    # no death_id column - the death table doesn't have one
    assert "death_id" not in header

    assert header.index("person_id") >= 0
    assert header.index("death_date") >= 0
    assert header.index("death_datetime") >= 0

    assert 2 == len(rows)
    rows_by_person = {row[header.index("person_id")]: row for row in rows}

    assert "2020-05-04 00:00:00" == rows_by_person["1"][header.index("death_datetime")]
    assert "2021-11-30 00:00:00" == rows_by_person["2"][header.index("death_datetime")]
