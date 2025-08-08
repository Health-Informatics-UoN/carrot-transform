import json
from pathlib import Path
from typing import Tuple
from unittest.mock import Mock, patch

import pytest

from carrottransform.tools.orchestrator import V2ProcessingOrchestrator, StreamProcessor
from carrottransform.tools.types import ProcessingContext
from carrottransform.tools.stream_helpers import StreamingLookupCache
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.metrics import Metrics
from carrottransform.tools.file_helpers import OutputFileManager


def _write_test_omop_files(tmp_path: Path) -> Tuple[Path, Path]:
    ddl = tmp_path / "ddl.sql"
    cfg = tmp_path / "omop.json"
    ddl.write_text((Path(__file__).parent / "test_data" / "test_ddl.sql").read_text())
    cfg.write_text(
        (Path(__file__).parent / "test_data" / "test_config.json").read_text()
    )
    return ddl, cfg


def _write_minimal_v2_rules(
    tmp_path: Path, person_filename: str, observe_filename: str
) -> Path:
    rules = {
        "metadata": {"dataset": "TestDS"},
        "cdm": {
            "person": {
                person_filename: {
                    "person_id_mapping": {
                        "source_field": "person_id",
                        "dest_field": "person_id",
                    },
                    "date_mapping": {
                        "source_field": "birth_date",
                        "dest_field": ["birth_datetime"],
                    },
                    "concept_mappings": {},
                }
            },
            "observation": {
                observe_filename: {
                    "person_id_mapping": {
                        "source_field": "person_id",
                        "dest_field": "person_id",
                    },
                    "date_mapping": {
                        "source_field": "date",
                        "dest_field": ["observation_datetime"],
                    },
                    "concept_mappings": {
                        "status": {
                            "Y": {
                                "observation_concept_id": [9001],
                                "observation_source_concept_id": [9001],
                            },
                            "original_value": ["value_as_string"],
                        }
                    },
                }
            },
        },
    }

    rules_path = tmp_path / "rules.json"
    rules_path.write_text(json.dumps(rules, indent=2))
    return rules_path


def _write_input_files(tmp_path: Path) -> Tuple[Path, Path, Path]:
    input_dir = tmp_path / "inputs"
    output_dir = tmp_path / "outputs"
    input_dir.mkdir()
    output_dir.mkdir()

    # person.csv
    person = input_dir / "person.csv"
    person.write_text(
        "\n".join(
            [
                "person_id,birth_date",
                "p1,2000-01-01",
                "p2,2001-02-03",
            ]
        )
        + "\n"
    )

    # observe.csv
    observe = input_dir / "observe.csv"
    observe.write_text(
        "\n".join(
            [
                "person_id,date,status",
                "p1,2020-05-06,Y",
                "p2,07/08/2021,N",
                "p3,2020-05-01,Y",  # invalid person -> should not be written
            ]
        )
        + "\n"
    )

    return input_dir, output_dir, person


@pytest.mark.unit
def test_execute_processing_happy_path(tmp_path: Path):
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
        write_mode="w",
    )

    result = orchestrator.execute_processing()

    # success and counts
    assert result.success is True
    # We expect one observation row written (for p1, Y)
    assert result.output_counts.get("observation", 0) == 1

    # output files exist and have content
    obs_path = output_dir / "observation.tsv"
    assert obs_path.is_file()
    lines = obs_path.read_text().strip().splitlines()
    # header + 1 data row
    assert len(lines) == 2

    # person_ids mapping saved
    pid_map = output_dir / "person_ids.tsv"
    assert pid_map.is_file()
    pid_lines = pid_map.read_text().strip().splitlines()
    # header + 2 assignments
    assert len(pid_lines) == 3

    # summary written
    summary = output_dir / "summary_mapstream.tsv"
    assert summary.is_file()
    sum_lines = summary.read_text().splitlines()
    # first line is the header from MapstreamSummaryRow.get_header()
    assert sum_lines[0].startswith(
        "dsname\tsource\tsource_field\ttarget\tconcept_id\tadditional\tincount"
    )
    # should include at least one row for observation target
    assert any("\tobservation\t" in line for line in sum_lines[1:])


@pytest.mark.unit
def test_rejects_non_v2_rules(tmp_path: Path):
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)

    # minimal non-v2 style rules (cdm table maps to simple dict without v2 keys)
    non_v2_rules = {
        "metadata": {"dataset": "TestDS"},
        "cdm": {
            "observation": {
                "some_field": {"source_table": "observe.csv", "source_field": "status"}
            }
        },
    }
    rules_path = tmp_path / "rules_v1_like.json"
    rules_path.write_text(json.dumps(non_v2_rules))

    with pytest.raises(ValueError):
        V2ProcessingOrchestrator(
            rules_file=rules_path,
            output_dir=output_dir,
            input_dir=input_dir,
            person_file=person_file,
            omop_ddl_file=ddl,
            omop_config_file=cfg,
        )


@pytest.mark.unit
def test_closes_files_on_exception(tmp_path: Path, monkeypatch):
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
        write_mode="w",
    )

    # Force the stream processor to raise in the top-level loop, so success=False
    from carrottransform.tools import orchestrator as orch_mod

    def boom(self, source_filename):  # type: ignore[no-redef]
        raise RuntimeError("boom")

    monkeypatch.setattr(orch_mod.StreamProcessor, "_process_input_file_stream", boom)

    result = orchestrator.execute_processing()
    assert result.success is False
    assert "boom" in (result.error_message or "")

    # Files should be closed and handle map cleared even on failure
    assert len(orchestrator.output_manager.file_handles) == 0


# StreamProcessor Unit Tests


@pytest.fixture
def mock_context():
    """Create a mock ProcessingContext for StreamProcessor tests"""
    context = Mock(spec=ProcessingContext)
    context.input_dir = Path("/mock/input")
    context.input_files = ["test.csv"]
    context.output_files = ["observation"]
    context.metrics = Mock(spec=Metrics)
    context.person_lookup = {"p1": "1", "p2": "2"}
    context.record_numbers = {"observation": 1}
    context.file_handles = {"observation": Mock()}
    context.target_column_maps = {"observation": {"observation_id": 0, "person_id": 1}}
    context.omopcdm = Mock(spec=OmopCDM)
    context.mappingrules = Mock(spec=MappingRules)

    # Setup omopcdm mock
    context.omopcdm.get_column_map.return_value = {
        "person_id": 0,
        "date": 1,
        "status": 2,
    }

    return context


@pytest.fixture
def mock_lookup_cache():
    """Create a mock StreamingLookupCache"""
    cache = Mock(spec=StreamingLookupCache)
    cache.input_to_outputs = {"test.csv": {"observation"}}
    cache.file_metadata_cache = {
        "test.csv": {
            "datetime_source": "date",
            "person_id_source": "person_id",
            "data_fields": {"observation": ["status"]},
        }
    }
    cache.target_metadata_cache = {
        "observation": {
            "auto_num_col": "observation_id",
            "person_id_col": "person_id",
            "date_col_data": {"observation_datetime": "observation_date"},
            "date_component_data": {},
            "notnull_numeric_fields": ["observation_type_concept_id"],
        }
    }
    return cache


@pytest.mark.unit
def test_stream_processor_process_all_data_success(mock_context, mock_lookup_cache):
    """Test StreamProcessor.process_all_data with successful processing"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    # Mock the _process_input_file_stream method
    with patch.object(processor, "_process_input_file_stream") as mock_process:
        mock_process.return_value = ({"observation": 2}, 0)

        result = processor.process_all_data()

        assert result.success is True
        assert result.output_counts == {"observation": 2}
        assert result.rejected_id_counts == {"test.csv": 0}
        mock_process.assert_called_once_with("test.csv")


@pytest.mark.unit
def test_stream_processor_process_all_data_with_error(mock_context, mock_lookup_cache):
    """Test StreamProcessor.process_all_data with processing error"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    with patch.object(processor, "_process_input_file_stream") as mock_process:
        mock_process.side_effect = Exception("Processing failed")

        result = processor.process_all_data()

        assert result.success is False
        assert "Processing failed" in result.error_message


@pytest.mark.unit
def test_stream_processor_process_input_file_missing(mock_context, mock_lookup_cache):
    """Test StreamProcessor._process_input_file_stream with missing input file"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    # File doesn't exist
    with patch.object(Path, "exists", return_value=False):
        output_counts, rejected_count = processor._process_input_file_stream(
            "missing.csv"
        )

        assert output_counts == {}
        assert rejected_count == 0


@pytest.mark.unit
def test_stream_processor_process_input_file_no_mappings(
    mock_context, mock_lookup_cache
):
    """Test StreamProcessor._process_input_file_stream with no mappings"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)
    mock_lookup_cache.input_to_outputs = {"test.csv": set()}  # No mappings

    with patch.object(Path, "exists", return_value=True):
        output_counts, rejected_count = processor._process_input_file_stream("test.csv")

        assert output_counts == {}
        assert rejected_count == 0


@pytest.mark.unit
def test_stream_processor_process_single_row_invalid_date(
    mock_context, mock_lookup_cache
):
    """Test StreamProcessor._process_single_row_stream with invalid date"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    input_data = ["p1", "invalid-date", "Y"]
    input_column_map = {"person_id": 0, "date": 1, "status": 2}
    applicable_targets = {"observation"}
    datetime_col_idx = 1
    file_meta = mock_lookup_cache.file_metadata_cache["test.csv"]

    # Mock normalise_to8601 to return None for invalid date
    with patch(
        "carrottransform.tools.orchestrator.normalise_to8601", return_value=None
    ):
        row_counts, rejected = processor._process_single_row_stream(
            "test.csv",
            input_data,
            input_column_map,
            applicable_targets,
            datetime_col_idx,
            file_meta,
        )

        assert row_counts == {}
        assert rejected == 1
        # Should increment invalid date metric
        mock_context.metrics.increment_key_count.assert_called()


@pytest.mark.unit
def test_stream_processor_process_single_row_valid_date(
    mock_context, mock_lookup_cache
):
    """Test StreamProcessor._process_single_row_stream with valid date"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    input_data = ["p1", "2023-01-01", "Y"]
    input_column_map = {"person_id": 0, "date": 1, "status": 2}
    applicable_targets = {"observation"}
    datetime_col_idx = 1
    file_meta = mock_lookup_cache.file_metadata_cache["test.csv"]

    # Mock the row processing to return success
    with patch.object(processor, "_process_row_for_target_stream") as mock_process_row:
        mock_process_row.return_value = (1, 0)
        with patch(
            "carrottransform.tools.orchestrator.normalise_to8601",
            return_value="2023-01-01 00:00:00",
        ):
            row_counts, rejected = processor._process_single_row_stream(
                "test.csv",
                input_data,
                input_column_map,
                applicable_targets,
                datetime_col_idx,
                file_meta,
            )

            assert row_counts == {"observation": 1}
            assert rejected == 0
            mock_process_row.assert_called_once()


@pytest.mark.unit
def test_stream_processor_process_data_column(mock_context, mock_lookup_cache):
    """Test StreamProcessor._process_data_column_stream"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    # Setup mock v2_mapping
    mock_v2_mapping = Mock()
    mock_context.mappingrules.v2_mappings = {
        "observation": {"test.csv": mock_v2_mapping}
    }

    input_data = ["p1", "2023-01-01 00:00:00", "Y"]
    input_column_map = {"person_id": 0, "date": 1, "status": 2}
    target_column_map = {"observation_id": 0, "person_id": 1}

    # Mock RecordBuilderFactory and result
    with patch(
        "carrottransform.tools.orchestrator.RecordBuilderFactory"
    ) as mock_factory:
        mock_builder = Mock()
        mock_result = Mock()
        mock_result.success = True
        mock_result.record_count = 1
        mock_result.metrics = mock_context.metrics
        mock_builder.build_records.return_value = mock_result
        mock_factory.create_builder.return_value = mock_builder

        record_count, rejected_count = processor._process_data_column_stream(
            "test.csv",
            input_data,
            input_column_map,
            "observation",
            mock_v2_mapping,
            target_column_map,
            "status",
            "observation_id",
            "person_id",
            {},
            {},
            [],
        )

        assert record_count == 1
        assert rejected_count == 0
        mock_builder.build_records.assert_called_once()


# V2ProcessingOrchestrator Unit Tests


@pytest.mark.unit
def test_orchestrator_initialize_components(tmp_path):
    """Test V2ProcessingOrchestrator.initialize_components"""
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
    )

    # Check components are initialized
    assert isinstance(orchestrator.omopcdm, OmopCDM)
    assert isinstance(orchestrator.mappingrules, MappingRules)
    assert isinstance(orchestrator.metrics, Metrics)
    assert isinstance(orchestrator.output_manager, OutputFileManager)
    assert isinstance(orchestrator.lookup_cache, StreamingLookupCache)
    assert orchestrator.mappingrules.is_v2_format is True


@pytest.mark.unit
def test_orchestrator_setup_person_lookup(tmp_path):
    """Test V2ProcessingOrchestrator.setup_person_lookup"""
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
    )

    person_lookup, rejected_count = orchestrator.setup_person_lookup()

    # Should have mappings for the 2 people in our test data
    assert len(person_lookup) == 2
    assert "p1" in person_lookup
    assert "p2" in person_lookup
    assert rejected_count == 0

    # Person IDs file should be created
    person_ids_file = output_dir / "person_ids.tsv"
    assert person_ids_file.exists()

    # Check content
    content = person_ids_file.read_text()
    lines = content.strip().split("\n")
    assert lines[0] == "SOURCE_SUBJECT\tTARGET_SUBJECT"
    assert len(lines) == 3  # header + 2 people


@pytest.mark.unit
def test_orchestrator_execute_processing_file_cleanup_on_success(tmp_path):
    """Test that files are properly closed after successful processing"""
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
    )

    # Execute processing
    result = orchestrator.execute_processing()

    assert result.success is True
    # Files should be closed after processing
    assert len(orchestrator.output_manager.file_handles) == 0


@pytest.mark.unit
def test_orchestrator_missing_input_file_handling(tmp_path):
    """Test orchestrator behavior with missing input files"""
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)

    # Create rules that reference a non-existent input file
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "nonexistent.csv")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
    )

    # Should still succeed but with zero output for the missing file
    result = orchestrator.execute_processing()
    assert result.success is True
    assert result.output_counts.get("observation", 0) == 0


@pytest.mark.unit
def test_orchestrator_invalid_person_file_data(tmp_path):
    """Test orchestrator with invalid person file data"""
    input_dir, output_dir, _ = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)

    # Create person file with invalid dates
    person_file = input_dir / "bad_person.csv"
    person_file.write_text(
        "\n".join(
            [
                "person_id,birth_date",
                "p1,invalid-date",  # Invalid date
                "p2,2001-02-03",  # Valid date
                ",2000-01-01",  # Missing person_id
            ]
        )
        + "\n"
    )

    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
    )

    person_lookup, rejected_count = orchestrator.setup_person_lookup()

    # Should only have 1 valid person (p2)
    assert len(person_lookup) == 1
    assert "p2" in person_lookup
    assert rejected_count == 2  # 2 invalid records


@pytest.mark.unit
def test_stream_processor_missing_metadata_fields(mock_context, mock_lookup_cache):
    """Test StreamProcessor with missing datetime or person_id metadata"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    # Modify cache to have missing datetime_source
    mock_lookup_cache.file_metadata_cache["test.csv"]["datetime_source"] = None

    with patch.object(Path, "exists", return_value=True):
        output_counts, rejected_count = processor._process_input_file_stream("test.csv")

        assert output_counts == {}
        assert rejected_count == 0


@pytest.mark.unit
def test_stream_processor_missing_date_column(mock_context, mock_lookup_cache):
    """Test StreamProcessor when date column is missing from input"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    # Mock CSV reading where date column is not found
    mock_csv_data = [
        ["person_id", "status"],  # Missing 'date' column
        ["p1", "Y"],
    ]

    with patch.object(Path, "exists", return_value=True):
        with (
            patch("builtins.open"),
            patch("csv.reader", return_value=iter(mock_csv_data)),
        ):
            # Mock get_column_map to return mapping without date column
            mock_context.omopcdm.get_column_map.return_value = {
                "person_id": 0,
                "status": 1,
            }

            output_counts, rejected_count = processor._process_input_file_stream(
                "test.csv"
            )

            assert output_counts == {}
            assert rejected_count == 0


@pytest.mark.unit
def test_orchestrator_write_mode_append(tmp_path):
    """Test orchestrator with append write mode"""
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    # Create existing output file to test append mode
    obs_file = output_dir / "observation.tsv"
    obs_file.write_text("existing_data\n")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
        write_mode="a",  # Append mode
    )

    result = orchestrator.execute_processing()
    assert result.success is True

    # File should still exist and contain both old and new data
    assert obs_file.exists()
    content = obs_file.read_text()
    assert "existing_data" in content


@pytest.mark.unit
def test_orchestrator_summary_file_content(tmp_path):
    """Test that summary file contains expected content structure"""
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
    )

    result = orchestrator.execute_processing()
    assert result.success is True

    # Check summary file content
    summary_file = output_dir / "summary_mapstream.tsv"
    content = summary_file.read_text()
    lines = content.strip().split("\n")

    # Verify header columns
    header = lines[0].split("\t")
    expected_columns = [
        "dsname",
        "source",
        "source_field",
        "target",
        "concept_id",
        "additional",
        "incount",
        "invalid_persid",
        "invalid_date",
        "invalid_source",
        "outcount",
    ]
    assert header == expected_columns

    # Should have at least one data row
    assert len(lines) > 1

    # Check that dataset name appears in the data
    assert any("TestDS" in line for line in lines[1:])


@pytest.mark.unit
def test_stream_processor_csv_reading_error(mock_context, mock_lookup_cache):
    """Test StreamProcessor handling CSV reading errors gracefully"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    with patch.object(Path, "exists", return_value=True):
        # Mock file open to raise an exception
        with patch("builtins.open", side_effect=IOError("File read error")):
            output_counts, rejected_count = processor._process_input_file_stream(
                "test.csv"
            )

            # Should handle error gracefully and return empty results
            assert output_counts == {"observation": 0}
            assert rejected_count == 0


@pytest.mark.unit
def test_orchestrator_dataset_name_from_rules(tmp_path):
    """Test that orchestrator correctly extracts dataset name from rules"""
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
    )

    # Check that dataset name was extracted correctly from rules
    assert orchestrator.metrics.dataset_name == "TestDS"


@pytest.mark.unit
def test_stream_processor_record_builder_failure(mock_context, mock_lookup_cache):
    """Test StreamProcessor when record builder returns failure"""
    processor = StreamProcessor(mock_context, mock_lookup_cache)

    # Setup mock v2_mapping
    mock_v2_mapping = Mock()
    mock_context.mappingrules.v2_mappings = {
        "observation": {"test.csv": mock_v2_mapping}
    }

    input_data = ["p1", "2023-01-01 00:00:00", "Y"]
    input_column_map = {"person_id": 0, "date": 1, "status": 2}
    target_column_map = {"observation_id": 0, "person_id": 1}

    # Mock RecordBuilderFactory to return failure
    with patch(
        "carrottransform.tools.orchestrator.RecordBuilderFactory"
    ) as mock_factory:
        mock_builder = Mock()
        mock_result = Mock()
        mock_result.success = False  # Failure
        mock_result.record_count = 0
        mock_result.metrics = mock_context.metrics
        mock_builder.build_records.return_value = mock_result
        mock_factory.create_builder.return_value = mock_builder

        record_count, rejected_count = processor._process_data_column_stream(
            "test.csv",
            input_data,
            input_column_map,
            "observation",
            mock_v2_mapping,
            target_column_map,
            "status",
            "observation_id",
            "person_id",
            {},
            {},
            [],
        )

        # Should report failure as rejection
        assert record_count == 0
        assert rejected_count == 1


@pytest.mark.unit
def test_orchestrator_component_initialization_order(tmp_path):
    """Test that orchestrator components are initialized in correct order"""
    input_dir, output_dir, person_file = _write_input_files(tmp_path)
    ddl, cfg = _write_test_omop_files(tmp_path)
    rules = _write_minimal_v2_rules(tmp_path, person_file.name, "observe.csv")

    # Test that initialization happens at construction time
    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules,
        output_dir=output_dir,
        input_dir=input_dir,
        person_file=person_file,
        omop_ddl_file=ddl,
        omop_config_file=cfg,
    )

    # All components should be available immediately after construction
    assert hasattr(orchestrator, "omopcdm")
    assert hasattr(orchestrator, "mappingrules")
    assert hasattr(orchestrator, "metrics")
    assert hasattr(orchestrator, "output_manager")
    assert hasattr(orchestrator, "lookup_cache")

    # Lookup cache should have been built with the correct mappingrules and omopcdm
    assert orchestrator.lookup_cache.mappingrules == orchestrator.mappingrules
    assert orchestrator.lookup_cache.omopcdm == orchestrator.omopcdm
