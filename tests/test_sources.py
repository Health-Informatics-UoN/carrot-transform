"""
runs some tests on the source reader thing
"""

from pathlib import Path

import pytest
import sqlalchemy

import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
import tests.testools as testools


@pytest.mark.unit
def test_basic_csv():
    """opens a csv connection, reads a file, checks we got the correct data"""

    folder = Path(__file__).parent / "test_data/measure_weight_height/"

    source = sources.SourceOpener(folder=folder)

    iterator = source.open("heights.csv")

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

    ###
    # arrange
    folder = Path(__file__).parent / "test_data/measure_weight_height/"
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    height = "heights.csv"

    # load a table with data
    testools.copy_across(
        # an output tied to that sql engine,
        outputs.sqlOutputTarget(engine),
        # the path to the files,
        sources.csvSourceObject(folder, ","),
        # we only want the heights file
        [height[:-4]],
    )

    ###
    # act

    # read that table back
    source = sources.sqlSourceObject(engine)
    iterator = source.open(height[:-4])

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
