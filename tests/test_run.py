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


@pytest.mark.unit
def test_explicit_file_path(tmp_path: Path):
    """Test when a specific file path is provided"""

    explicit_path = tmp_path / "file.tsv"
    result = run.set_saved_person_id_file(explicit_path, tmp_path)
    assert result == explicit_path


@pytest.mark.unit
def test_default_file_creation(tmp_path: Path):
    """Test when no file is specified (None case)"""

    output_dir = tmp_path
    result = run.set_saved_person_id_file(None, output_dir)
    expected_path = output_dir / "person_ids.tsv"
    assert result == expected_path


@pytest.mark.unit
def test_existing_file_removal(tmp_path: Path):
    """Test that existing file is removed when None is passed"""

    output_dir = tmp_path
    existing_file = output_dir / "person_ids.tsv"

    # Create a dummy file
    with existing_file.open("w") as f:
        f.write("test")

    assert os.path.exists(existing_file)  # Verify file exists

    result = run.set_saved_person_id_file(None, output_dir)
    assert result == existing_file  # Check returned path
    assert not os.path.exists(existing_file)  # Verify file was removed


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

    source = sources.SourceOpener(folder=file_path.parent)
    csv_reader = source.open(file_path.name)

    assert csv_reader is not None

    # Verify we can read the content
    rows = list(csv_reader)
    assert rows[0] == ["header1", "header2"]
    assert rows[1] == ["value1", "value2"]


@pytest.mark.unit
def test_nonexistent_file(tmp_path: Path):
    """Test attempting to open a non-existent file"""

    src = tmp_path / "nonexistent.csv"
    print(src.is_file())

    source = sources.SourceOpener(folder=tmp_path)
    try:
        result = source.open("nonexistent.csv")

        raise Exception(f"the test shouldn't get this far {result=}")
    except sources.SourceFileNotFoundException as sourceException:
        assert sourceException._name == "nonexistent.csv"
        assert sourceException._path == (tmp_path / "nonexistent.csv")


@pytest.mark.unit
def test_directory_not_found(caplog):
    """Test attempting to open a file in a non-existent directory"""

    with caplog.at_level(logging.ERROR):
        folder = Path("/nonexistent/directory")
        try:
            source = sources.SourceOpener(folder=folder)
            raise Exception("the test shouldn't get this far")
            result = source.open("test.csv")
            assert result is None, "the result should be None"
        except sources.SourceFolderMissingException as sourceFolderMissingException:
            assert sourceFolderMissingException._source._folder == folder


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

    source = sources.SourceOpener(folder=file_path.parent)
    csv_reader = source.open(file_path.name)

    assert csv_reader is not None

    rows = list(csv_reader)
    assert rows[0] == ["header1", "header2"]  # Should not have BOM in content
    assert rows[1] == ["value1", "value2"]


### test_set_omop_filenames(omop_ddl_file, omop_config_file, omop_version):
@pytest.mark.unit
def test_explicit_filenames():
    """Test when both filenames are explicitly provided"""
    version = "5.4"

    ddl_file = Path("/path/to/ddl.sql")
    config_file = Path("/path/to/config.json")

    result_config, result_ddl = run.set_omop_filenames(ddl_file, config_file, version)

    assert result_config == config_file
    assert result_ddl == ddl_file


@pytest.mark.unit
def test_auto_filenames_from_version():
    """Test when version is provided but no files"""
    version = "5.4"

    expected_base = importlib.resources.files("carrottransform")
    assert isinstance(expected_base, Path), "this test assumes it's a Path()"

    expected_config = expected_base / "config/config.json"
    expected_ddl = expected_base / f"config/OMOPCDM_postgresql_{version}_ddl.sql"

    result_config, result_ddl = run.set_omop_filenames(None, None, version)

    assert result_config == expected_config
    assert result_ddl == expected_ddl


@pytest.mark.unit
def test_no_changes_when_partial_files():
    """Test when some files are provided but not all"""
    ddl_file = "/path/to/ddl.sql"
    version = "5.4"

    # Test with only DDL file
    result_config, result_ddl = run.set_omop_filenames(ddl_file, None, version)
    assert result_config is None
    assert result_ddl == ddl_file

    # Test with only config file
    config_file = "/path/to/config.json"
    result_config, result_ddl = run.set_omop_filenames(None, config_file, version)
    assert result_config == config_file
    assert result_ddl is None


@pytest.mark.unit
def test_no_version():
    """Test when no version is provided"""
    ddl_file = None
    config_file = None
    version = None

    result_config, result_ddl = run.set_omop_filenames(ddl_file, config_file, version)

    assert result_config is None
    assert result_ddl is None


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
