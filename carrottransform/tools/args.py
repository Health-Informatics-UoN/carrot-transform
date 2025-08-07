"""
functions to handle args
"""

from pathlib import Path


class OnlyOnePersonInputAllowed(Exception):
    """Raised when they try to use more than one person file in the mapping"""

    def __init__(self, rules_file: Path, person_file: Path, inputs: set[str]):
        self._rules_file = rules_file
        self._person_file = person_file
        self._inputs = inputs


class NoPersonMappings(Exception):
    """Raised when they try to use more than one person file in the mapping"""

    def __init__(self, rules_file: Path, person_file: Path):
        self._rules_file = rules_file
        self._person_file = person_file


class WrongInputException(Exception):
    """Raised when they try to read fromt he wrong table - and only the wrong table"""

    def __init__(self, rules_file: Path, person_file: Path, source_table: str):
        self._rules_file = rules_file
        self._person_file = person_file
        self._source_table = source_table


class ObjectQueryError(Exception):
    """Raised when the object path format is invalid."""


class ObjectStructureError(Exception):
    """Raised when the object path format points to inaccessible elements."""


def person_rules_check(person_file: Path, rules_file: Path) -> None:
    """check that the person rules file is correct.

    we need all person/patient records to come from one file - the person file. this includes the gender mapping. this should/must also be the person_file parameter.

    ... this does reopen the possibility of auto-detecting the person file from the rules file
    """

    # check the args are real files
    if not person_file.is_file():
        raise Exception(f"person file not found: {person_file=}")
    if not rules_file.is_file():
        raise Exception(f"person file not found: {rules_file=}")

    # load the rules file
    with open(rules_file) as file:
        import json

        rules_json = json.load(file)

    # loop through the rules for person rules with wrong_inputs
    seen_inputs: set[str] = set()
    try:
        for rule_name, person in object_query(rules_json, "cdm/person").items():
            found_a_rule = True
            for col in person:
                source_table: str = person[col]["source_table"]
                seen_inputs.add(source_table)
    except ObjectStructureError as e:
        if "Key 'person' not found in object" == str(e):
            raise NoPersonMappings(rules_file, person_file)
        else:
            raise e

    # for theoretical cases when there is a `"people":{}` entry that's empty
    # ... i don't think that carrot-mapper would emit it, but, i think that it would be valid JSON
    if not found_a_rule:
        raise NoPersonMappings(rules_file, person_file)

    # detect too many input files
    if 1 < len(seen_inputs):
        raise OnlyOnePersonInputAllowed(rules_file, person_file, seen_inputs)

    # check if the seen file is correct
    seen_table: str = list(seen_inputs)[0]

    if person_file.name != seen_table:
        raise WrongInputException(rules_file, person_file, seen_table)


def object_query(data: dict[str, dict | str], path: str):
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
    if not current_key:
        raise ObjectQueryError(f"Invalid path: blank key at start in {path!r}")

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
