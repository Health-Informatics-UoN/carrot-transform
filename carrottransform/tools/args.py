"""
functions to handle args
"""

import json

from pathlib import Path




class SourceFieldError(Exception):
    """Raised when the rules.json does't set person_id/source_field to PersonID."""


class MultipleTablesError(Exception):
    """Raised when there are multiple .csv files in the the person_id/source_table values."""


def auto_person_in_rules(rules: Path) -> Path:
    """scan a rules file to see where it's getting its `PersonID` from"""

    # for better error reporting, record all the sourcetables
    source_tables = set()

    # grab the data
    data = json.load(rules.open())

    # query the objects for the items
    for _, person in object_query(data, "cdm/person").items():

        # check if the source field is correct
        if "PersonID" != object_query(person, "person_id/source_field"):
            raise SourceFieldError()

        source_tables.add(object_query(person, "person_id/source_table"))

    # check result
    if len(source_tables) == 1:
        return rules.parent / next(iter(source_tables))

    # raise an error
    multipleTablesError = MultipleTablesError()
    multipleTablesError.source_tables = sorted(source_tables)
    raise multipleTablesError


class ObjectQueryError(Exception):
    """Raised when the object path format is invalid."""


class ObjectStructureError(Exception):
    """Raised when the object path format points to inaccessible elements."""


def object_query(data: dict, path: str) -> any:
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
