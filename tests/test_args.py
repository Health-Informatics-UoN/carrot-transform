import pytest

from pathlib import Path
import carrottransform
from carrottransform.tools.args import person_rules_check, OnlyOnePersonInputAllowed,NoPersonMappings
from carrottransform.tools.args import person_rules_check

# def auto_person_in_rules(rules: Path) -> Path:
#     """scan a rules file to see where it's getting its `PersonID` from"""

#     # for better error reporting, record all the sourcetables
#     source_tables = set()

#     # grab the data
#     data = json.load(rules.open())

#     # query the objects for the items
#     for _, person in object_query(data, "cdm/person").items():
#         # check if the source field is correct
#         if "PersonID" != object_query(person, "person_id/source_field"):
#             raise SourceFieldError()

#         source_tables.add(object_query(person, "person_id/source_table"))

#     # check result
#     if len(source_tables) == 1:
#         return rules.parent / next(iter(source_tables))

#     # raise an error
#     multipleTablesError = MultipleTablesError()
#     multipleTablesError.source_tables = sorted(source_tables)
#     raise multipleTablesError


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(
            OnlyOnePersonInputAllowed(
                person_file = Path("tests/test_data/args/empty-person-file.csv"),
                rules_file = Path("tests/test_data/args/reads-from-other-tables.json"),
                inputs = {
                    "demos_m.csv",
                    "demos_f.csv",
                }
            ),
            id="reads from other tables"
        ),
        pytest.param(
            NoPersonMappings(
                person_file = Path("tests/test_data/args/empty-person-file.csv"),
                rules_file = Path("tests/test_data/args/no-person-rules.json")
            ),
            id="test when no person mappings are defined"
        ),


        #
        # TODO; test reading from only a single wrong person file
        #


        
        pytest.param(
            OnlyOnePersonInputAllowed(
                person_file = Path("tests/test_data/mireda_key_error/demographics_mother_gold.csv"),
                rules_file = Path("tests/test_data/mireda_key_error/original_rules.json"),
                inputs = {
                    "infant_data_gold.csv",
                    "demographics_child_gold.csv",
                }
            ),
            id="test the mireda rules file"
        ),

        
        #
        # TODO; do a valid test here. one that passes.
        #
    ],
)
def test_person_rules_throws(exception):

    # arrange
    caught = False

    # act
    try:
        person_rules_check(
            person_file = exception._person_file,
            rules_file = exception._rules_file,
        )
    except OnlyOnePersonInputAllowed as e:
        assert caught == False
        caught = e
    except NoPersonMappings as e:
        assert caught == False
        caught = e

    # assert
    if exception is None:
        assert caught == False
    else:
        
        assert caught != False
        
        
        
        assert isinstance(caught, type(exception)), f"{type(caught)=} != {type(exception)=}"

        assert exception._person_file == caught._person_file    
        assert exception._rules_file == caught._rules_file

        if isinstance(caught, OnlyOnePersonInputAllowed):
            assert exception._inputs == caught._inputs
