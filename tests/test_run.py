import importlib
import logging
import os
from pathlib import Path
from unittest.mock import patch

import pytest

import carrottransform.cli.subcommands.run as run
import carrottransform.tools.sources as sources
from carrottransform.tools.person_helpers import _get_person_lookup

logger = logging.getLogger(__name__)


@pytest.mark.unit
def test_valid_directory(tmp_path: Path):
    """Test with a valid directory path"""

    run.check_dir_isvalid(tmp_path)  # Should not raise any exception


@pytest.mark.unit
def test_invalid_directory(tmp_path: Path):
    """Test with a non-existent directory"""

    non_existent_dir = tmp_path / "non_existent"
    with pytest.raises(SystemExit) as exc_info:
        run.check_dir_isvalid(non_existent_dir)
    assert exc_info.value.code == 1





### check_files_in_rules_exist(rules_input_files, existing_input_files):
@pytest.mark.unit
def test_matching_files(caplog):
    """Test when all files match between rules and existing files"""


@pytest.mark.unit
def test_successful_file_open(tmp_path: Path):
    """Test successful opening of a valid file"""
    # Create a test file
    test_file = "test.csv"
    file_content = "header1,header2\nvalue1,value2"
    file_path = tmp_path / test_file

    with file_path.open("w", encoding="utf-8") as f:
        f.write(file_content)

    source = sources.csvSourceObject(file_path.parent, ",")
    csv_reader = source.open(
        file_path.name[:-4]
    )  # need to remove the .csv fron the name

    assert csv_reader is not None

    # Verify we can read the content
    rows = list(csv_reader)
    assert rows[0] == ["header1", "header2"]
    assert rows[1] == ["value1", "value2"]


@pytest.mark.unit
def test_nonexistent_file(tmp_path: Path):
    """Test attempting to open a non-existent file"""

    src = tmp_path / "nonexistent.csv"

    source = sources.csvSourceObject(tmp_path, ",")
    try:
        result = source.open("nonexistent")

        raise Exception(f"the test shouldn't get this far {result=}")
    except sources.SourceTableNotFound as sourceTableNotFound:
        assert sourceTableNotFound._name == "nonexistent"


@pytest.mark.unit
def test_directory_not_found(caplog):
    """Test attempting to open a file in a non-existent directory"""

    with caplog.at_level(logging.ERROR):
        folder = Path("/nonexistent/directory")
        try:
            source = sources.csvSourceObject(folder, ",")
            raise Exception("the test shouldn't get this far")
            result = source.open("test.csv")
            assert result is None, "the result should be None"
        except sources.SourceNotFound as sourceNotFound:
            assert sourceNotFound._path == folder


@pytest.mark.unit
def test_utf8_with_bom(tmp_path: Path):
    """Test opening a UTF-8 file with BOM"""
    test_file = "utf8bom.csv"
    # Create UTF-8 with BOM content
    content = "header1,header2\nvalue1,value2"
    file_path = tmp_path / test_file

    # Write with UTF-8-BOM encoding
    with file_path.open("wb") as f:
        f.write(b"\xef\xbb\xbf")  # UTF-8 BOM
        f.write(content.encode("utf-8"))

    source = sources.csvSourceObject(file_path.parent, ",")
    csv_reader = source.open(
        file_path.name[:-4]
    )  # need to remove the .csv fron the name

    assert csv_reader is not None

    rows = list(csv_reader)
    assert rows[0] == ["header1", "header2"]  # Should not have BOM in content
    assert rows[1] == ["value1", "value2"]


### test_get_person_lookup(saved_person_id_file):
@pytest.mark.unit
def test_new_person_lookup(tmp_path: Path):
    """Test when no saved person ID file exists"""
    nonexistent_file = tmp_path / "nonexistent.tsv"

    person_lookup, last_used_integer = _get_person_lookup(nonexistent_file)

    assert person_lookup == {}
    assert last_used_integer == 1


@pytest.mark.unit
@patch("carrottransform.tools.person_helpers._load_saved_person_ids")
def test_existing_person_lookup(mock_load, tmp_path: Path):
    """Test when saved person ID file exists"""
    existing_file = tmp_path / "existing.tsv"
    mock_load.return_value = ({"person1": 1, "person2": 2}, 2)

    existing_file.touch()

    person_lookup, last_used_integer = _get_person_lookup(existing_file)

    assert person_lookup == {"person1": 1, "person2": 2}
    assert last_used_integer == 2
