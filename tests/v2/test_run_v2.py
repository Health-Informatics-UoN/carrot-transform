"""
Tests for the v2 CLI command (run_v2.py)
"""

import pytest
import json
import csv
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock
from click.testing import CliRunner

from carrottransform.cli.subcommands.run_v2 import mapstream_v2


class TestRunV2CLI:
    """Test cases for the v2 CLI command"""

    @pytest.fixture
    def test_environment(self):
        """Create a complete test environment"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Create directories
            input_dir = tmp_path / "input"
            output_dir = tmp_path / "output"
            input_dir.mkdir()
            output_dir.mkdir()

            # Create v2 rules file
            rules_data = {
                "metadata": {
                    "date_created": "2025-01-01T00:00:00+00:00",
                    "dataset": "test_cli",
                },
                "cdm": {
                    "person": {
                        "test_persons.csv": {
                            "person_id_mapping": {
                                "source_field": "person_id",
                                "dest_field": "person_id",
                            },
                            "date_mapping": {
                                "source_field": "birth_date",
                                "dest_field": ["birth_datetime"],
                            },
                            "concept_mappings": {
                                "gender": {
                                    "M": {
                                        "gender_concept_id": [8507],
                                        "gender_source_concept_id": [8507],
                                    },
                                    "F": {
                                        "gender_concept_id": [8532],
                                        "gender_source_concept_id": [8532],
                                    },
                                    "original_value": ["gender_source_value"],
                                }
                            },
                        }
                    }
                },
            }

            rules_file = tmp_path / "v2_rules.json"
            with rules_file.open("w") as f:
                json.dump(rules_data, f)

            # Create person file
            person_file = input_dir / "test_persons.csv"
            with person_file.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["person_id", "birth_date", "gender"])
                writer.writerow(["1", "1990-01-01", "M"])
                writer.writerow(["2", "1985-06-15", "F"])

            # Create OMOP config file
            config_data = {
                "person_id_field": {"person": "person_id"},
                "auto_number_field": {"person": "person_id"},
            }

            config_file = tmp_path / "omop_config.json"
            with config_file.open("w") as f:
                json.dump(config_data, f)

            yield {
                "tmp_path": tmp_path,
                "input_dir": input_dir,
                "output_dir": output_dir,
                "rules_file": rules_file,
                "person_file": person_file,
                "config_file": config_file,
            }

    def test_cli_missing_required_args(self):
        """Test CLI with missing required arguments"""
        runner = CliRunner()
        result = runner.invoke(mapstream_v2, [])

        assert result.exit_code != 0
        assert "Missing option" in result.output

    def test_cli_help(self):
        """Test CLI help message"""
        runner = CliRunner()
        result = runner.invoke(mapstream_v2, ["--help"])

        assert result.exit_code == 0
        assert "Map to OMOP output using v2 format rules" in result.output

    @patch("carrottransform.cli.subcommands.run_v2.V2ProcessingOrchestrator")
    def test_cli_successful_execution(self, mock_orchestrator_class, test_environment):
        """Test successful CLI execution"""
        # Mock the orchestrator
        mock_orchestrator = Mock()
        mock_result = Mock()
        mock_result.success = True
        mock_orchestrator.execute_processing.return_value = mock_result
        mock_orchestrator_class.return_value = mock_orchestrator

        env = test_environment
        ddl_file = Path("tests/test_data/test_ddl.sql")

        runner = CliRunner()
        result = runner.invoke(
            mapstream_v2,
            [
                "--rules-file",
                str(env["rules_file"]),
                "--output-dir",
                str(env["output_dir"]),
                "--person-file",
                str(env["person_file"]),
                "--input-dir",
                str(env["input_dir"]),
                "--omop-ddl-file",
                str(ddl_file),
                "--omop-config-file",
                str(env["config_file"]),
            ],
        )

        assert result.exit_code == 0
        mock_orchestrator_class.assert_called_once()
        mock_orchestrator.execute_processing.assert_called_once()

    @patch("carrottransform.cli.subcommands.run_v2.V2ProcessingOrchestrator")
    def test_cli_processing_failure(self, mock_orchestrator_class, test_environment):
        """Test CLI execution with processing failure"""
        # Mock the orchestrator to fail
        mock_orchestrator = Mock()
        mock_result = Mock()
        mock_result.success = False
        mock_result.error_message = "Test processing error"
        mock_orchestrator.execute_processing.return_value = mock_result
        mock_orchestrator_class.return_value = mock_orchestrator

        env = test_environment
        ddl_file = Path("tests/test_data/test_ddl.sql")

        runner = CliRunner()
        result = runner.invoke(
            mapstream_v2,
            [
                "--rules-file",
                str(env["rules_file"]),
                "--output-dir",
                str(env["output_dir"]),
                "--person-file",
                str(env["person_file"]),
                "--input-dir",
                str(env["input_dir"]),
                "--omop-ddl-file",
                str(ddl_file),
                "--omop-config-file",
                str(env["config_file"]),
            ],
        )

        # The CLI should complete but log the error
        assert result.exit_code == 0
        mock_orchestrator.execute_processing.assert_called_once()

    @patch("carrottransform.cli.subcommands.run_v2.V2ProcessingOrchestrator")
    def test_cli_exception_handling(self, mock_orchestrator_class, test_environment):
        """Test CLI exception handling"""
        # Mock the orchestrator to raise an exception
        mock_orchestrator_class.side_effect = Exception("Initialization error")

        env = test_environment
        ddl_file = Path("tests/test_data/test_ddl.sql")

        runner = CliRunner()
        result = runner.invoke(
            mapstream_v2,
            [
                "--rules-file",
                str(env["rules_file"]),
                "--output-dir",
                str(env["output_dir"]),
                "--person-file",
                str(env["person_file"]),
                "--input-dir",
                str(env["input_dir"]),
                "--omop-ddl-file",
                str(ddl_file),
                "--omop-config-file",
                str(env["config_file"]),
            ],
        )

        assert result.exit_code != 0
        assert isinstance(result.exception, Exception)

    def test_cli_with_different_write_modes(self, test_environment):
        """Test CLI with different write modes"""
        env = test_environment
        ddl_file = Path("tests/test_data/test_ddl.sql")

        with patch(
            "carrottransform.cli.subcommands.run_v2.V2ProcessingOrchestrator"
        ) as mock_class:
            mock_orchestrator = Mock()
            mock_result = Mock()
            mock_result.success = True
            mock_orchestrator.execute_processing.return_value = mock_result
            mock_class.return_value = mock_orchestrator

            runner = CliRunner()

            # Test append mode
            result = runner.invoke(
                mapstream_v2,
                [
                    "--rules-file",
                    str(env["rules_file"]),
                    "--output-dir",
                    str(env["output_dir"]),
                    "--person-file",
                    str(env["person_file"]),
                    "--input-dir",
                    str(env["input_dir"]),
                    "--omop-ddl-file",
                    str(ddl_file),
                    "--omop-config-file",
                    str(env["config_file"]),
                    "--write-mode",
                    "a",
                ],
            )

            assert result.exit_code == 0

            # Verify the orchestrator was called with append mode
            call_args = mock_class.call_args
            assert call_args.kwargs["write_mode"] == "a"

    def test_cli_with_omop_version(self, test_environment):
        """Test CLI with OMOP version parameter"""
        env = test_environment

        with patch(
            "carrottransform.cli.subcommands.run_v2.V2ProcessingOrchestrator"
        ) as mock_class:
            with patch(
                "carrottransform.cli.subcommands.run_v2.set_omop_filenames"
            ) as mock_set_omop:
                mock_set_omop.return_value = (env["config_file"], Path("test_ddl.sql"))

                mock_orchestrator = Mock()
                mock_result = Mock()
                mock_result.success = True
                mock_orchestrator.execute_processing.return_value = mock_result
                mock_class.return_value = mock_orchestrator

                runner = CliRunner()
                result = runner.invoke(
                    mapstream_v2,
                    [
                        "--rules-file",
                        str(env["rules_file"]),
                        "--output-dir",
                        str(env["output_dir"]),
                        "--person-file",
                        str(env["person_file"]),
                        "--input-dir",
                        str(env["input_dir"]),
                        "--omop-version",
                        "5.4",
                    ],
                )

                assert result.exit_code == 0
                mock_set_omop.assert_called_once()


@pytest.mark.unit
class TestCliPathResolution:
    """Test path resolution in CLI"""

    @patch("carrottransform.cli.subcommands.run_v2.resolve_paths")
    @patch("carrottransform.cli.subcommands.run_v2.V2ProcessingOrchestrator")
    def test_path_resolution_called(self, mock_orchestrator_class, mock_resolve_paths):
        """Test that path resolution is called correctly"""
        # Create temp paths for testing
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            rules_file = tmp_path / "rules.json"
            output_dir = tmp_path / "output"
            person_file = tmp_path / "person.csv"
            input_dir = tmp_path / "input"
            ddl_file = Path("tests/test_data/test_ddl.sql")
            config_file = tmp_path / "config.json"

            # Create minimal files
            rules_file.write_text('{"metadata": {"dataset": "test"}, "cdm": {}}')
            person_file.write_text("person_id\\n1\\n")
            config_file.write_text("{}")
            output_dir.mkdir()
            input_dir.mkdir()

            # Mock resolve_paths to return the same paths
            mock_resolve_paths.return_value = [
                rules_file,
                output_dir,
                person_file,
                ddl_file,
                config_file,
                input_dir,
            ]

            # Mock orchestrator
            mock_orchestrator = Mock()
            mock_result = Mock()
            mock_result.success = True
            mock_orchestrator.execute_processing.return_value = mock_result
            mock_orchestrator_class.return_value = mock_orchestrator

            runner = CliRunner()
            result = runner.invoke(
                mapstream_v2,
                [
                    "--rules-file",
                    str(rules_file),
                    "--output-dir",
                    str(output_dir),
                    "--person-file",
                    str(person_file),
                    "--input-dir",
                    str(input_dir),
                    "--omop-ddl-file",
                    str(ddl_file),
                    "--omop-config-file",
                    str(config_file),
                ],
            )

            assert result.exit_code == 0
            mock_resolve_paths.assert_called_once()
