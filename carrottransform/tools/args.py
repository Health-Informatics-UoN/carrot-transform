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



class ObjectQueryError(Exception):
    """Raised when the object path format is invalid."""


class ObjectStructureError(Exception):
    """Raised when the object path format points to inaccessible elements."""

def person_rules_check(person_file: Path, rules_file: Path) -> None:
    """check that the person rules file is correct"""

    # load the rules file
    with open(rules_file) as file:
        import json
        rules_json = json.load(file)

    raise Exception("??? loop through the rules for person rules")
    raise Exception("??? - mark we found a person rule")
    raise Exception("??? - loop through the person rules and only allow one person file")

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
