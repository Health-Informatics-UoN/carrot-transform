from carrottransform.cli.subcommands.run import *
import pytest
from unittest.mock import patch

### check_dir_isvalid(directory)
@pytest.mark.unit
def test_valid_directory(tmp_path):
    """Test with a valid directory path"""
    check_dir_isvalid(str(tmp_path))  # Should not raise any exception
@pytest.mark.unit
def test_directory_as_tuple(tmp_path):
    """Test with a directory path wrapped in a tuple"""
    check_dir_isvalid((str(tmp_path),))  # Should not raise any exception
@pytest.mark.unit
def test_invalid_directory(tmp_path):
    """Test with a non-existent directory"""
    non_existent_dir = os.path.join(str(tmp_path), "non_existent")
    with pytest.raises(SystemExit) as exc_info:
        check_dir_isvalid(non_existent_dir)
    assert exc_info.value.code == 1


## set_saved_person_id_file(saved_person_id_file, output_dir):
@pytest.mark.unit
def test_explicit_file_path(tmp_path):
    """Test when a specific file path is provided"""
    explicit_path = os.path.join(tmp_path, "file.tsv")
    result = set_saved_person_id_file(explicit_path, str(tmp_path))
    assert result == explicit_path

@pytest.mark.unit
def test_default_file_creation(tmp_path):
    """Test when no file is specified (None case)"""
    output_dir = str(tmp_path)
    result = set_saved_person_id_file(None, output_dir)
    expected_path = os.path.join(output_dir, "person_ids.tsv")
    assert result == expected_path

@pytest.mark.unit
def test_existing_file_removal(tmp_path):
    """Test that existing file is removed when None is passed"""
    output_dir = str(tmp_path)
    existing_file = os.path.join(output_dir, "person_ids.tsv")

    # Create a dummy file
    with open(existing_file, 'w') as f:
        f.write("test")

    assert os.path.exists(existing_file)  # Verify file exists

    result = set_saved_person_id_file(None, output_dir)
    assert result == existing_file  # Check returned path
    assert not os.path.exists(existing_file)  # Verify file was removed

### check_files_in_rules_exist(rules_input_files, existing_input_files):
@pytest.mark.unit
def test_matching_files(capsys):
    """Test when all files match between rules and existing files"""
    rules_files = ["file1.txt", "file2.txt"]
    existing_files = ["file1.txt", "file2.txt"]

    check_files_in_rules_exist(rules_files, existing_files)
    captured = capsys.readouterr()
    assert captured.out == ""  # No warnings should be printed

@pytest.mark.unit
def test_extra_existing_file(capsys):
    """Test when there's an existing file not in rules"""
    rules_files = ["file1.txt"]
    existing_files = ["file1.txt", "extra.txt"]

    check_files_in_rules_exist(rules_files, existing_files)
    captured = capsys.readouterr()
    assert "WARNING: no mapping rules found for existing input file - extra.txt" in captured.out

@pytest.mark.unit
def test_extra_rules_file(capsys):
    """Test when there's a rules file with no existing data"""
    rules_files = ["file1.txt", "missing.txt"]
    existing_files = ["file1.txt"]

    check_files_in_rules_exist(rules_files, existing_files)
    captured = capsys.readouterr()
    assert "WARNING: no data for mapped input file - missing.txt" in captured.out

@pytest.mark.unit
def test_multiple_mismatches(capsys):
    """Test when there are multiple mismatches in both directions"""
    rules_files = ["file1.txt", "missing1.txt", "missing2.txt"]
    existing_files = ["file1.txt", "extra1.txt", "extra2.txt"]

    check_files_in_rules_exist(rules_files, existing_files)
    captured = capsys.readouterr()
    output = captured.out

    assert "WARNING: no mapping rules found for existing input file - extra1.txt" in output
    assert "WARNING: no mapping rules found for existing input file - extra2.txt" in output
    assert "WARNING: no data for mapped input file - missing1.txt" in output
    assert "WARNING: no data for mapped input file - missing2.txt" in output

### open_file(directory, filename):
@pytest.mark.unit
def test_successful_file_open(tmp_path):
    """Test successful opening of a valid file"""
    # Create a test file
    test_file = "test.csv"
    file_content = "header1,header2\nvalue1,value2"
    file_path = tmp_path / test_file

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(file_content)

    file_handle, csv_reader = open_file(str(tmp_path), test_file)

    try:
        assert file_handle is not None
        # Verify we can read the content
        rows = list(csv_reader)
        assert rows[0] == ["header1", "header2"]
        assert rows[1] == ["value1", "value2"]
    finally:
        if file_handle:
            file_handle.close()

@pytest.mark.unit
def test_nonexistent_file(tmp_path, capsys):
    """Test attempting to open a non-existent file"""
    result = open_file(str(tmp_path), "nonexistent.csv")

    assert result is None
    captured = capsys.readouterr()
    assert "Unable to open:" in captured.out
    assert "nonexistent.csv" in captured.out

@pytest.mark.unit
def test_directory_not_found(capsys):
    """Test attempting to open a file in a non-existent directory"""
    result = open_file("/nonexistent/directory", "test.csv")

    assert result is None
    captured = capsys.readouterr()
    assert "Unable to open:" in captured.out
    assert "No such file or directory" in captured.out


@pytest.mark.unit
def test_utf8_with_bom(tmp_path):
    """Test opening a UTF-8 file with BOM"""
    test_file = "utf8bom.csv"
    # Create UTF-8 with BOM content
    content = "header1,header2\nvalue1,value2"
    file_path = tmp_path / test_file

    # Write with UTF-8-BOM encoding
    with open(file_path, "wb") as f:
        f.write(b'\xef\xbb\xbf')  # UTF-8 BOM
        f.write(content.encode('utf-8'))

    file_handle, csv_reader = open_file(str(tmp_path), test_file)

    try:
        assert file_handle is not None
        rows = list(csv_reader)
        assert rows[0] == ["header1", "header2"]  # Should not have BOM in content
        assert rows[1] == ["value1", "value2"]
    finally:
        if file_handle:
            file_handle.close()


### test_set_omop_filenames(omop_ddl_file, omop_config_file, omop_version):
@pytest.mark.unit
def test_explicit_filenames():
    """Test when both filenames are explicitly provided"""
    ddl_file = "/path/to/ddl.sql"
    config_file = "/path/to/config.json"
    version = "5.4"

    result_config, result_ddl = set_omop_filenames(ddl_file, config_file, version)

    assert result_config == config_file
    assert result_ddl == ddl_file


@pytest.mark.unit
def test_auto_filenames_from_version():
    """Test when version is provided but no files"""
    version = "5.4"
    expected_base = str(importlib.resources.files('carrottransform'))
    expected_config = f"{expected_base}/config/omop.json"
    expected_ddl = f"{expected_base}/config/OMOPCDM_postgresql_{version}_ddl.sql"

    result_config, result_ddl = set_omop_filenames(None, None, version)

    assert result_config == expected_config
    assert result_ddl == expected_ddl


@pytest.mark.unit
def test_no_changes_when_partial_files():
    """Test when some files are provided but not all"""
    ddl_file = "/path/to/ddl.sql"
    version = "5.4"

    # Test with only DDL file
    result_config, result_ddl = set_omop_filenames(ddl_file, None, version)
    assert result_config is None
    assert result_ddl == ddl_file

    # Test with only config file
    config_file = "/path/to/config.json"
    result_config, result_ddl = set_omop_filenames(None, config_file, version)
    assert result_config == config_file
    assert result_ddl is None


@pytest.mark.unit
def test_no_version():
    """Test when no version is provided"""
    ddl_file = None
    config_file = None
    version = None

    result_config, result_ddl = set_omop_filenames(ddl_file, config_file, version)

    assert result_config is None
    assert result_ddl is None


### test_get_person_lookup(saved_person_id_file):
@pytest.mark.unit
def test_new_person_lookup(tmp_path):
    """Test when no saved person ID file exists"""
    nonexistent_file = tmp_path / "nonexistent.tsv"

    person_lookup, last_used_integer = get_person_lookup(nonexistent_file)

    assert person_lookup == {}
    assert last_used_integer == 1


@pytest.mark.unit
@patch("carrottransform.cli.subcommands.run.load_saved_person_ids")
def test_existing_person_lookup(mock_load, tmp_path):
    """Test when saved person ID file exists"""
    existing_file = tmp_path / "existing.tsv"
    mock_load.return_value = ({"person1": 1, "person2": 2}, 2)

    existing_file.touch()

    person_lookup, last_used_integer = get_person_lookup(existing_file)

    assert person_lookup == {"person1": 1, "person2": 2}
    assert last_used_integer == 2