from pathlib import Path

import pytest

from carrottransform.tools.args import (
    NoPersonMappings,
    ObjectQueryError,
    ObjectStructureError,
    OnlyOnePersonInputAllowed,
    PersonRulesWrong,
    RulesFileNotFound,
    WrongInputException,
    object_query,
    person_rules_check,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            OnlyOnePersonInputAllowed(
                person_file=("empty-person-file.csv"),
                rules_file=Path("tests/test_data/args/reads-from-other-tables.json"),
                inputs={
                    "demos_m.csv",
                    "demos_f.csv",
                },
            ),
            id="reads from other tables",
        ),
        pytest.param(
            NoPersonMappings(
                person_file=("empty-person-file.csv"),
                rules_file=Path("tests/test_data/args/no-person-rules.json"),
            ),
            id="test when no person mappings are defined",
        ),
        pytest.param(
            OnlyOnePersonInputAllowed(
                person_file="demographics_mother_gold.csv",
                rules_file=Path("tests/test_data/mireda_key_error/original_rules.json"),
                inputs={
                    "infant_data_gold.csv",
                    "demographics_child_gold.csv",
                },
            ),
            id="test the mireda rules file",
        ),
        # TODO it'd be good to test;
        # TODO - test reading from only a single wrong person file
        # TODO - do a valid test here. one that passes.
    ],
)
def test_person_rules_throws(exception):
    # arrange
    caught = False

    # act
    try:
        person_rules_check(
            person_file_name=exception._person_file,
            rules_file=exception._rules_file,
        )
    except OnlyOnePersonInputAllowed as e:
        assert not caught
        caught = e
    except NoPersonMappings as e:
        assert not caught
        caught = e

    # assert
    if exception is None:
        assert not caught
    else:
        assert caught

        assert isinstance(caught, type(exception)), (
            f"{type(caught)=} != {type(exception)=}"
        )

        assert exception._person_file == caught._person_file
        assert exception._rules_file == caught._rules_file

        if isinstance(caught, OnlyOnePersonInputAllowed):
            assert exception._inputs == caught._inputs


@pytest.mark.unit
def test_person_rules_throws_WrongInputException():
    """this is a test to trigger the WrongInputException"""
    person_file = "demographics_mother_gold.csv"
    rules_file = Path("tests/test_data/wrong-person-table-rules.json")
    source_table = "src_PERSON.csv"

    # arrange
    caught: None | WrongInputException = None

    # act
    try:
        person_rules_check(
            person_file_name=person_file,
            rules_file=rules_file,
        )
    except WrongInputException as e:
        assert caught is None
        caught = e

    # assert
    assert caught is not None

    assert caught._rules_file == rules_file
    assert caught._person_file == person_file
    assert caught._source_table == source_table


@pytest.mark.unit
def test_person_rules_check__not_found(tmp_path: Path):
    fred_file: Path = tmp_path / "fred.json"

    try:
        person_rules_check("dave.csv", fred_file)
        raise Exception("that should have failed")
    except RulesFileNotFound as e:
        assert fred_file == e._rules_file


@pytest.mark.unit
def test_person_rules_check__bad_form(tmp_path: Path):
    fred_file: Path = tmp_path / "fred.json"

    open(fred_file, "w").write(
        """
        {
            "cdm": {
                "person": "fred"
            }
        }
        """
    )

    try:
        person_rules_check("dave.csv", fred_file)
        raise Exception("that should have failed")
    except PersonRulesWrong as e:
        assert fred_file == e._rules_file


@pytest.mark.unit
def test_person_rules_check__no_person(tmp_path: Path):
    fred_file: Path = tmp_path / "fred.json"

    open(fred_file, "w").write(
        """
        {
            "cdm": {
                "persons": "the name is wrong here"
            }
        }
        """
    )

    try:
        person_rules_check("dave.csv", fred_file)
        raise Exception("that should have failed")
    except NoPersonMappings as e:
        assert fred_file == e._rules_file


@pytest.mark.unit
def test_person_rules_check__empty_person(tmp_path: Path):
    fred_file: Path = tmp_path / "fred.json"

    open(fred_file, "w").write(
        """
        {
            "cdm": {
                "person": {}
            }
        }
        """
    )

    try:
        person_rules_check("dave.csv", fred_file)
        raise Exception("that should have failed")
    except NoPersonMappings as e:
        assert fred_file == e._rules_file


@pytest.mark.unit
def test_object_query_error():
    """tests the object_query() throws an error when it starts with /"""

    data = {"foo": 9, "bar": {"value": 12}}

    error: None | ObjectQueryError = None
    try:
        object_query(data, "/bar/value")
        raise Exception("that should have thrown an exception")
    except ObjectQueryError as e:
        error = e

    assert error is not None

    assert (
        str(error)
        == "Invalid path format: '/bar/value' (must not start with '/' and not end with '/')"
    )


@pytest.mark.unit
def test_object_structure_error():
    """tests the object_query() throws an error when trying to read a string as a dict"""

    data = {"foo": 9, "bar": {"value": 12}}

    error: None | ObjectStructureError = None
    try:
        object_query(data, "foo/value")
        raise Exception("that should have thrown an exception")
    except ObjectStructureError as e:
        error = e

    assert error is not None

    assert str(error) == "Cannot descend into non-dict value at key 'foo'"


@pytest.mark.unit
def test_invalid_paths():
    """i can't find a way to make an invalid Path(: str) so i'm using `:int`"""

    bad_path = 7

    from carrottransform.tools.args import PathArgumentType

    class E(Exception):
        def __init__(self, message):
            super().__init__(message)
            self._message = message

    class M(PathArgumentType):
        def fail(self, message, param, ctx):
            raise E(message)

    try:
        M().convert(bad_path, "p", "c")

        raise Exception("that should have failed")
    except E as e:
        assert e._message.startswith("Invalid path: 7 (")


@pytest.mark.unit
def test_invalid_connection():
    """use a junk connection string"""

    from carrottransform.tools.args import AlchemyConnectionArgumentType

    class E(Exception):
        def __init__(self, message):
            super().__init__(message)
            self._message = message

    class M(AlchemyConnectionArgumentType):
        def fail(self, message, param, ctx):
            raise E(message)

    try:
        M().convert("this.wont.work", "p", "c")

        raise Exception("that should have failed")
    except E as e:
        assert (
            "invalid connection string: this.wont.work (Could not parse SQLAlchemy URL from given URL string)"
            == e._message
        )
