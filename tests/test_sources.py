"""
runs some tests on the source reader thing
"""

from pathlib import Path

import pytest
import sqlalchemy

import carrottransform.tools.sources as sources
import tests.click_tools as click_tools


@pytest.mark.unit
def test_no_sources():
    with pytest.raises(RuntimeError) as runtimeError:
        sources.SourceOpener()
    assert (
        "SourceOpener needs either an engine or a folder" == runtimeError.value.args[0]
    )


@pytest.mark.unit
def test_both_sources(tmp_path):
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    with pytest.raises(RuntimeError) as runtimeError:
        sources.SourceOpener(folder=tmp_path, engine=engine)
    assert (
        "SourceOpener cannot have both a folder and an engine"
        == runtimeError.value.args[0]
    )


@pytest.mark.unit
def test_bad_name(tmp_path):
    with pytest.raises(RuntimeError) as runtimeError:
        sources.SourceOpener(folder=tmp_path).open("fred")
    assert (
        "source names must end with .csv but was name='fred'"
        == runtimeError.value.args[0]
    )


@pytest.mark.unit
def test_bad_name2(tmp_path):
    with pytest.raises(RuntimeError) as runtimeError:
        sources.SourceOpener(folder=tmp_path).open("fred.sql")
    assert (
        "source names must end with .csv but was name='fred.sql'"
        == runtimeError.value.args[0]
    )


@pytest.mark.unit
def test_bad_name3(tmp_path):
    name = "fr/ed.csv"
    with pytest.raises(RuntimeError) as runtimeError:
        sources.SourceOpener(folder=tmp_path).open(name)
    assert (
        f"source names must name a file not a path but was {name=}"
        == runtimeError.value.args[0]
    )


@pytest.mark.unit
def test_bad_name4(tmp_path):
    name = "fr\\ed.csv"
    with pytest.raises(RuntimeError) as runtimeError:
        sources.SourceOpener(folder=tmp_path).open(name)
    assert (
        f"source names must name a file not a path but was {name=}"
        == runtimeError.value.args[0]
    )


@pytest.mark.unit
def test_table_not_found(tmp_path):
    name = "fred.csv"
    with pytest.raises(sources.SourceFileNotFoundException) as runtimeError:
        sources.SourceOpener(folder=tmp_path).open(name)

    assert name == runtimeError.value._name
    assert runtimeError.value.args[0].startswith("Source file 'fred.csv' not found by")


@pytest.mark.unit
def test_table_blank(tmp_path):
    name = "fred.csv"

    open(tmp_path / name, "w").close()

    with pytest.raises(StopIteration):
        reader = sources.SourceOpener(folder=tmp_path).open(name)

        print(next(reader))


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
