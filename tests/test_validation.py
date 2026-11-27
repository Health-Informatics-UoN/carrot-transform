import csv
import json
from pathlib import Path

import pytest

from carrottransform.tools import outputs, sources
from carrottransform.tools.orchestrator import V2ProcessingOrchestrator
from carrottransform.tools.validation import valid_date_value


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [
        "2000-01-01",
        "01-01-2000",
        "06/12/2004",
        "2000-01-01 00:00:00",
        "2000-01-01 12:30",
        "2000-01-01 12:30:45.123456",
        "01-01-2000 00:00:00",
        "01-01-2000 23:59",
        "01-01-2000 08:15:30.000000",
        "06/12/2004 08:15:00",
        "06/12/2004 08:15",
        "06/12/2004 08:15:00.500000",
    ],
)
def test_valid_date_value_accepts_dates_and_datetimes(value: str):
    assert valid_date_value(value) is True


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
        "1989",
        "1985/07/15",
        "1985/07/15 00:00:00",
        "2000-02-30 00:00:00",
        "2000-01-01 25:00:00",
        "01-01-2000 notatime",
    ],
)
def test_valid_date_value_rejects_invalid_dates_and_datetimes(value: str):
    assert valid_date_value(value) is False


@pytest.mark.unit
def test_v2_person_processing_with_birth_datetime_timestamps(tmp_path: Path):
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    output_dir.mkdir()

    person_file = input_dir / "test_persons.csv"
    with person_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["PersonID", "birth_date", "gender"])
        writer.writerow(["1", "01-01-2000 00:00:00", "M"])
        writer.writerow(["2", "1990-05-15 14:30:00", "F"])
        writer.writerow(["3", "06/12/2004 08:15:00", "F"])

    rules_file = tmp_path / "v2_rules.json"
    rules_file.write_text(
        json.dumps(
            {
                "metadata": {
                    "date_created": "2025-01-01T00:00:00+00:00",
                    "dataset": "test",
                },
                "cdm": {
                    "person": {
                        "test_persons.csv": {
                            "person_id_mapping": {
                                "source_field": "PersonID",
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
        )
    )

    config_file = tmp_path / "omop_config.json"
    config_file.write_text(
        json.dumps(
            {
                "date_field_components": {
                    "person": {
                        "birth_datetime": {
                            "year": "year_of_birth",
                            "month": "month_of_birth",
                            "day": "day_of_birth",
                        }
                    }
                },
                "person_id_field": {"person": "person_id"},
            }
        )
    )

    output_target = outputs.csv_output_target(output_dir)
    source_obj = sources.csv_source_object(input_dir, sep=",")
    orchestrator = V2ProcessingOrchestrator(
        rules_file=rules_file,
        output=output_target,
        inputs=source_obj,
        person="test_persons",
        omop_ddl_file=Path("tests/test_data/test_ddl.sql"),
        omop_config_file=config_file,
        write_mode="w",
    )
    orchestrator.execute_processing()
    source_obj.close()
    output_target.close()

    with (output_dir / "person.tsv").open("r") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))

    assert len(rows) == 3
    assert [
        (
            r["year_of_birth"],
            r["month_of_birth"],
            r["day_of_birth"],
            r["birth_datetime"],
        )
        for r in rows
    ] == [
        ("2000", "1", "1", "2000-01-01 00:00:00"),
        ("1990", "5", "15", "1990-05-15 14:30:00"),
        ("2004", "12", "6", "2004-12-06 08:15:00"),
    ]
