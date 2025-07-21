import pytest

from pathlib import Path
import carrottransform
from carrottransform.tools.args import *


@pytest.mark.unit
def test_on_the_example_rules():
    rules = (
        Path(carrottransform.__file__).parent
        / "examples/test/rules/rules_14June2021.json"
    )

    people = (
        Path(carrottransform.__file__).parent / "examples/test/rules/Demographics.csv"
    )

    assert rules.is_file()

    assert people == auto_person_in_rules(rules)


@pytest.mark.unit
def test_with_bad_rules():
    rules = (
        Path(carrottransform.__file__).parent.parent
        / "tests/test_data/broken_rules.json"
    )

    assert rules.is_file()
    with pytest.raises(MultipleTablesError) as exc_info:
        auto_person_in_rules(rules)

    assert "5Demographics.csv" in exc_info.value.source_tables
    assert "aDemographics.csv" in exc_info.value.source_tables
    assert 2 == len(exc_info.value.source_tables)
