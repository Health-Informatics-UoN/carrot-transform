"""
runs some tests on the source reader thing
"""

from pathlib import Path

import pytest
import sqlalchemy

import carrottransform.tools.sources as sources
import tests.click_tools as click_tools


@pytest.mark.unit
def test_basic_csv():
    """opens a csv connection, reads a file, checks we got the correct data"""

    folder = Path(__file__).parent / "test_data/measure_weight_height/"

    source = sources.csvSourceObject(folder, ",")

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

    source = sources.sqlSourceObject(engine)

    # load a table with data
    click_tools.load_test_database_table(engine, folder / "heights.csv")

    # read that table back
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




test_data: Path = Path(__file__).parent / 'test_data'

@pytest.mark.unit
def test_csv_truncating_empty():
    """csv files may use trailing commas. if the last column has no column name; this checks to be sure we're handling that"""

    folder = test_data / "integration_test1/"

    source = sources.csvSourceObject(folder, ",")

    iterator = source.open("src_PERSON")

    head = next(iterator)

    assert'' != head[-1], f"{head=}"
    # first entry should be the header
    assert head == ["person_id", "gender_source_value", "birth_datetime"]

    # check each row
    assert next(iterator) == ["321", "male", "1950-10-31"]
    assert next(iterator) == ["789345", "female", "1981-11-19"]
    assert next(iterator) == ["6789", "femail", "1985-03-01"]
    assert next(iterator) == ["289", "", "1989-07-23"]

    # check the iterator is exhausted
    with pytest.raises(StopIteration):
        next(iterator)

