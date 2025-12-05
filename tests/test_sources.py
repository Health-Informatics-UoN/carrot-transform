"""
runs some tests on the source reader thing
"""

from pathlib import Path

import pytest
import sqlalchemy

import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
from carrottransform.tools import outputs, sources
from tests import testools


@pytest.mark.unit
def test_basic_csv():
    """opens a csv connection, reads a file, checks we got the correct data"""

    folder = Path(__file__).parent / "test_data/measure_weight_height/"

    source = sources.csv_source_object(folder, ",")

    iterator = source.open("heights")

    # first entry should be the header
    assert next(iterator) == ["pid", "date", "value"]

    # check each row
    assert next(iterator) == ["21", "2021-12-02", "123"]
    assert next(iterator) == ["21", "2021-12-01", "122"]
    assert next(iterator) == ["21", "2021-12-03", "12"]
    assert next(iterator) == ["81", "2022-12-02", "23"]
    assert next(iterator) == ["81", "2021-03-01", "92"]
    assert next(iterator) == ["91", "2021-02-03", "72"]

    # check the iterator is exhausted
    with pytest.raises(StopIteration):
        next(iterator)


@pytest.mark.unit
def test_basic_sqlite():
    """opens a sql connection, loads data from a file, checks the correct data comes back out"""

    folder = Path(__file__).parent / "test_data/measure_weight_height/"
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    height = "heights.csv"

    source = sources.sql_source_object(engine)

    # load a table with data
    testools.copy_across(
        outputs.sql_output_target(engine),
        sources.csv_source_object(folder, ","),
        ["heights"],
    )

    ###
    # act

    # read that table back
    iterator = source.open("heights")

    ###
    # assert

    # first entry should be the header
    assert next(iterator) == ["pid", "date", "value"]

    # check each row
    assert next(iterator) == ["21", "2021-12-02", "123"]
    assert next(iterator) == ["21", "2021-12-01", "122"]
    assert next(iterator) == ["21", "2021-12-03", "12"]
    assert next(iterator) == ["81", "2022-12-02", "23"]
    assert next(iterator) == ["81", "2021-03-01", "92"]
    assert next(iterator) == ["91", "2021-02-03", "72"]

    # check the iterator is exhausted
    with pytest.raises(StopIteration):
        next(iterator)
