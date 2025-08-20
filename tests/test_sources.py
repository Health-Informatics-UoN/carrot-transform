"""
runs some tests on the source reader thing
"""

from pathlib import Path

import click_tools
import pytest
import sqlalchemy

import carrottransform.tools.sources as sources


@pytest.mark.unit
def test_too_many_parameters(tmp_path: Path):
    try:
        sources.SourceOpener(
            folder=tmp_path, engine=sqlalchemy.create_engine("sqlite:///:memory:")
        )
        assert False, "this should have failed"
    except RuntimeError as e:
        assert str(e) == "SourceOpener cannot have both a folder and an engine"


@pytest.mark.unit
def test_too_few_parameters(tmp_path: Path):
    try:
        sources.SourceOpener(folder=None, engine=None)
        assert False, "this should have failed"
    except RuntimeError as e:
        assert str(e) == "SourceOpener needs either an engine or a folder"


@pytest.mark.unit
def test_bad_table_name(tmp_path: Path):
    source = sources.SourceOpener(engine=sqlalchemy.create_engine("sqlite:///:memory:"))
    try:
        source.open("a_table")
        assert False, "this should have failed"
    except sources.SourceNotFoundException as e:
        assert e._message == "source names must end with .csv but was name='a_table'"
        assert e._source == source
        assert e._name == "a_table"


@pytest.mark.unit
def test_mssing_csv(tmp_path: Path):
    source = sources.SourceOpener(folder=tmp_path)
    try:
        source.open("a_table.csv")
        assert False, "this should have failed"
    except sources.SourceFileNotFoundException as e:
        assert e._path == tmp_path / "a_table.csv"
        assert e._name == "a_table.csv"


@pytest.mark.unit
def test_name_not_a_path(tmp_path: Path):
    source = sources.SourceOpener(folder=tmp_path)
    try:
        source.open("sub/a_table.csv")
        assert False, "this should have failed"
    except sources.SourceNotFoundException as e:
        assert (
            e._message
            == "source names must name a file not a path but was name='sub/a_table.csv'"
        )
        assert e._source == source
        assert e._name == "sub/a_table.csv"


@pytest.mark.unit
def test_name_not_a_path_sql(tmp_path: Path):
    source = sources.SourceOpener(engine=sqlalchemy.create_engine("sqlite:///:memory:"))
    try:
        source.open("sub/a_table.csv")
        assert False, "this should have failed"
    except sources.SourceNotFoundException as e:
        assert (
            e._message
            == "source names must name a file not a path but was name='sub/a_table.csv'"
        )
        assert e._source == source
        assert e._name == "sub/a_table.csv"


@pytest.mark.unit
def test_blank_csv(tmp_path: Path):
    open(tmp_path / "a_table.csv", "w").close()
    source = sources.SourceOpener(folder=tmp_path)
    try:
        source.open("a_table.csv")
        assert False, "this should have failed"
    except sources.SourceNotFoundException as e:
        assert e._message == "csv file is empty name='a_table.csv'"
        assert e._source == source
        assert e._name == "a_table.csv"


@pytest.mark.unit
def test_missing_table():
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    source = sources.SourceOpener(engine=engine)
    try:
        source.open("a_table.csv")
        assert False, "this should have failed"
    except sources.SourceNotFoundException as e:
        assert (
            e._message
            == "failed to find first row because e=InvalidRequestError('Could not reflect: requested table(s) not available in Engine(sqlite:///:memory:): (a_table)')"
        )
        assert e._source == source
        assert e._name == "a_table.csv"


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

    folder = Path(__file__).parent / "test_data/measure_weight_height/"
    engine = sqlalchemy.create_engine("sqlite:///:memory:")

    source = sources.SourceOpener(engine=engine)

    # load a table with data
    click_tools.load_test_database_table(engine, folder / "heights.csv")

    # read that table back
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
