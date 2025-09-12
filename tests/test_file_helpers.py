import importlib.resources as resources
import os
from pathlib import Path
from unittest.mock import patch

import pytest

import carrottransform.tools.args as args


@pytest.mark.unit
@pytest.mark.parametrize(
    "input, expected",
    [
        ("@carrot/config/test.json", args.carrot / "config/test.json"),
        ("/normal/path/file.txt", Path("/normal/path/file.txt")),
        ("relative/path/file.csv", Path("relative/path/file.csv")),
        (
            "@carrot\\config\\test.json",
            args.carrot / "config/test.json",
        ),  # Windows backslash
        (
            "@carrot/config\\test.json",
            args.carrot / "config/test.json",
        ),  # Mixed slashes
        (
            "@carrot\\config/test.json",
            args.carrot / "config/test.json",
        ),  # Mixed slashes
    ],
)
def test_resolve_paths(input: str, expected: Path):
    """
    this used to test the resolve_paths() function but now ... it does the same tests to the path-arg object.

    the multiple tests have been rewritten to be parameters for this one test case

    """

    actual: Path = args.PathArg.convert(input, None, None)

    assert actual == expected
