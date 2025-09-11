import datetime
import logging
import re
from pathlib import Path

import pytest

import carrottransform.tools.date_helpers as date_helpers
import carrottransform.tools.validation as validation
import tests.click_tools as click_tools
import tests.csvrow as csvrow


@pytest.mark.unit
@pytest.mark.parametrize(
    "blank",
    [
        "",
        " ",
        "\n",
        "\t",
        "  ",
        "\n ",
        " \t",
    ],
)
def test_validation_blank(blank: str) -> None:
    assert not (validation.valid_date_value(blank))


@pytest.mark.unit
@pytest.mark.parametrize(
    "item",
    [
        "today",
        " tomorrw",
        "24 january, 1895",
    ],
)
def test_validation_junk(caplog, item: str) -> None:
    caplog.set_level(logging.WARNING)
    valid = validation.valid_date_value(item)
    assert not valid
    records = list(caplog.records)
    assert 1 == len(records)
    assert f"{item} is not a valid/supported date format" == records[0].msg


@pytest.mark.unit
def test_reverse_iso():
    valid = validation._valid_reverse_iso_date("11-09-2025")
    assert valid


@pytest.mark.unit
def test_uk_time():
    valid = validation._valid_uk_date("11/09/2025")
    assert valid
