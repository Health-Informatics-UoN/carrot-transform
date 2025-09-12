"""
functions to handle args
"""

from pathlib import Path

import click
from sqlalchemy import create_engine

import carrottransform.tools.file_helpers as helpers
from carrottransform.tools.mappingrules import MappingRules

# need this for substition. this should be the folder iwth an "examples/" sub" folder
carrot: Path = Path(__file__).parent.parent
# assert (mine/'examples/').is_dir()
# assert (mine/'cli/').is_dir()
# assert (mine/'config/').is_dir()


def object_query(data: dict[str, dict | str], path: str) -> dict | str:
    """
    Navigate a nested dictionary using a `/`-delimited path string.

    Args:
        data: The dictionary to traverse.
        path: The object path, e.g., "/foo/bar".

    Returns:
        The value at the given path.

    Raises:
        ObjectQueryError: If the path format is invalid or the key is missing.
    """

    if path.startswith("/") or path.endswith("/"):
        raise ObjectQueryError(
            f"Invalid path format: {path!r} (must not start with '/' and not end with '/')"
        )

    current_key, _, remaining_path = path.partition("/")

    if current_key not in data:
        raise ObjectStructureError(f"Key {current_key!r} not found in object")

    value = data[current_key]
    if not remaining_path:
        return value

    if not isinstance(value, dict):
        raise ObjectStructureError(
            f"Cannot descend into non-dict value at key {current_key!r}"
        )

    return object_query(value, remaining_path)


class OnlyOnePersonInputAllowed(Exception):
    """Raised when they try to use more than one person file in the mapping"""

    def __init__(self, rules_file: Path, person_file: str, inputs: set[str]):
        self._rules_file = rules_file
        self._person_file = person_file
        self._inputs = inputs


class NoPersonMappings(Exception):
    """Raised when they try to use more than one person file in the mapping"""

    def __init__(self, rules_file: Path, person_file: str):
        self._rules_file = rules_file
        self._person_file = person_file


class WrongInputException(Exception):
    """Raised when they try to read from the wrong table - and only the wrong table"""

    def __init__(self, rules_file: Path, person_file: str, source_table: str):
        self._rules_file = rules_file
        self._person_file = person_file
        self._source_table = source_table


class PathArgumentType(click.ParamType):
    """implements a "Path" type that click can pass to our program ... rather than checking the value ourselves"""

    name = "filepath"

    def convert(self, value: str, param, ctx) -> Path:
        try:
            # switch to posix separators
            value = value.replace("\\", "/")

            prefix: str = "@carrot/"
            if value.startswith(prefix):
                return carrot / value[len(prefix) :]
            else:
                return Path(value)
        except Exception as e:
            self.fail(f"Invalid path: {value} ({e})", param, ctx)


class AlchemyConnectionArgumentType(click.ParamType):
    """implements an SQLAlchemy connection type that can be checkd and passed to our function by click"""

    name = "sqlalchemy connection string"

    def convert(self, value, param, ctx):
        try:
            return create_engine(value)
        except Exception as e:
            self.fail(f"invalid connection string: {value} ({e})", param, ctx)


# create singletons for these argument types
PathArg = PathArgumentType()
AlchemyConnectionArg = AlchemyConnectionArgumentType()


class ObjectQueryError(Exception):
    """Raised when the object path format is invalid."""


class ObjectStructureError(Exception):
    """Raised when the object path format points to inaccessible elements."""


def person_rules_check_v2(
    person_file: Path | None, person_table: str | None, mappingrules: MappingRules
) -> None:
    """check that the person rules file is correct."""
    person_file_name = None
    if person_file:
        if not person_file.is_file():
            raise Exception("Person file not found.")
        person_file_name = person_file.name

    person_rules = object_query(mappingrules.rules_data, "cdm/person")
    if not person_rules:
        raise Exception("Mapping rules to Person table not found")
    if len(person_rules) > 1:
        raise Exception(
            f"""The source table for the OMOP table Person can be only one, which is the person file: {person_file_name}. However, there are multiple source tables {list(person_rules.keys())} for the Person table in the mapping rules."""
        )
    if (
        len(person_rules) == 1
        and person_table
        and person_table != list(person_rules.keys())[0].split(".")[0]
    ):
        raise Exception(
            f"""The source table for the OMOP table Person should be the person table {person_table}, but the current source table for Person is {list(person_rules.keys())[0].split(".")[0]}."""
        )
    if (
        len(person_rules) == 1
        and person_file_name
        and (person_file_name not in person_rules)
    ):
        raise Exception(
            f"""The source table for the OMOP table Person should be the person file {person_file_name}, but the current source table for Person is {list(person_rules.keys())[0]}."""
        )


def person_rules_check(person_file_name: str, rules_file: Path) -> None:
    """check that the person rules file is correct.

    Parameters:
            person_file: str - the text name of the person-file we're allowed and required to read from
            rules_file: Path - the real path to the rules file

    we need all person/patient records to come from one file - the person file. this includes the gender mapping. this should/must also be the person_file parameter.

    requiring this fixes these three issues;
        - https://github.com/Health-Informatics-UoN/carrot-transform/issues/72
        - https://github.com/Health-Informatics-UoN/carrot-transform/issues/76
        - https://github.com/Health-Informatics-UoN/carrot-transform/issues/78

    ... this does reopen the possibility of auto-detecting the person file from the rules file
    """

    assert isinstance(person_file_name, str)

    # check the rules file is real
    if not rules_file.is_file():
        raise Exception(f"person file not found: {rules_file=}")

    # load the rules file
    with open(rules_file) as file:
        import json

        rules_json = json.load(file)

    # to allow prettier error reporting - we collect all names that were used
    seen_inputs: set[str] = set()
    try:
        person_rules = object_query(rules_json, "cdm/person")
        if not isinstance(person_rules, dict):
            raise RuntimeError("the person section is not in the expected format")

        for rule_name, person in person_rules.items():
            found_a_rule = True
            for col in person:
                source_table: str = person[col]["source_table"]
                seen_inputs.add(source_table)
    except ObjectStructureError as e:
        if "Key 'person' not found in object" == str(e):
            raise NoPersonMappings(rules_file, person_file_name)
        else:
            raise e

    # for theoretical cases when there is a `"people":{}` entry that's empty
    # ... i don't think that carrot-mapper would emit it, but, i think that it would be valid JSON
    if not found_a_rule:
        raise NoPersonMappings(rules_file, person_file_name)

    # detect too many input files
    if 1 < len(seen_inputs):
        raise OnlyOnePersonInputAllowed(rules_file, person_file_name, seen_inputs)

    # check if the seen file is correct
    seen_table: str = list(seen_inputs)[0]

    if person_file_name != seen_table:
        raise WrongInputException(rules_file, person_file_name, seen_table)
