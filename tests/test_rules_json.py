import pytest

from pathlib import Path
import carrottransform
import json


def find_person_in_rules(rules: Path) -> str:
    """scan a rules file to see where it's getting its `PersonID` from
    """

    # mark we haven't found one
    source_table = ""

    # grab the data
    data = json.load(rules.open())
    assert data is not None
    data = data["cdm"]
    assert data is not None
    data = data["person"]
    assert data is not None
    for g in data.keys():
        assert data[g] is not None
        assert data[g]["person_id"] is not None
        assert data[g]["person_id"]["source_field"] is not None
        assert data[g]["person_id"]["source_table"] is not None

        # grab the source table - check that the person_id is "good"
        assert "PersonID" == data[g]["person_id"]["source_field"]
        g_source_table = data[g]["person_id"]["source_table"]

        # check that we didn't get an unsable one
        assert g_source_table is not None
        assert "" != g_source_table

        # possibly record the source_table OR ensure it
        if "" == source_table:
            source_table = g_source_table

        # assert that the source table matches
        assert g_source_table == source_table

    # assert that we got a "real" soruce table AND return it
    assert "" != source_table
    return source_table


@pytest.mark.unit
def test_on_the_example_rules():
    rules = (
        Path(carrottransform.__file__).parent
        / "examples/test/rules/rules_14June2021.json"
    )

    assert rules.is_file()

    assert "Demographics.csv" == find_person_in_rules(rules)
