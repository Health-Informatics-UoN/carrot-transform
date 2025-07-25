import pytest

from pathlib import Path
from carrottransform.tools.args import (
    person_rules_check,
    OnlyOnePersonInputAllowed,
    NoPersonMappings,
)


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            OnlyOnePersonInputAllowed(
                person_file=Path("tests/test_data/args/empty-person-file.csv"),
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
                person_file=Path("tests/test_data/args/empty-person-file.csv"),
                rules_file=Path("tests/test_data/args/no-person-rules.json"),
            ),
            id="test when no person mappings are defined",
        ),
        pytest.param(
            OnlyOnePersonInputAllowed(
                person_file=Path(
                    "tests/test_data/mireda_key_error/demographics_mother_gold.csv"
                ),
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
            person_file=exception._person_file,
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
