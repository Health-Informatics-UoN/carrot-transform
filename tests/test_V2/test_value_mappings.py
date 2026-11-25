"""
End-to-end test for `value_mappings` (https://carrot.ac.uk/transform/rules_v2_notes_nlp): a
note's concept field filled from the concept its note_type value maps to.
"""

import csv
import json
from pathlib import Path

from carrottransform.tools import outputs, sources
from carrottransform.tools.orchestrator import V2ProcessingOrchestrator
from carrottransform.tools.record_builder import RecordBuilderFactory

CONFIG = Path("carrottransform/config/config.json")
DDL_54 = Path("carrottransform/config/OMOPCDM_postgresql_5.4_ddl.sql")
OUTPATIENT, DISCHARGE = 706556, 706531  # illustrative note class concepts


def _write_csv(path: Path, rows: list[list[str]]):
    with path.open("w", newline="") as f:
        csv.writer(f).writerows(rows)


def test_note_class_from_note_type_values(tmp_path):
    input_dir, output_dir = tmp_path / "input", tmp_path / "output"
    input_dir.mkdir()
    _write_csv(
        input_dir / "person.csv",
        [["patient_id", "birth_date", "sex"], ["p1", "1970-01-01", "F"]],
    )
    _write_csv(
        input_dir / "notes.csv",
        [
            ["note_ref", "patient_id", "note_date", "note_type", "text"],
            ["N-1", "p1", "2025-01-10", "Outpatient note", "a"],
            ["N-2", "p1", "2025-01-11", "Discharge summary", "b"],
            ["N-3", "p1", "2025-01-12", "Radiology report", "c"],  # not mapped
            ["N-4", "p1", "2025-01-13", "", "d"],  # no value
        ],
    )
    rules = {
        "metadata": {"date_created": "2026-11-17T00:00:00+00:00", "dataset": "notes"},
        "cdm": {
            "person": {
                "person.csv": {
                    "person_id_mapping": {
                        "source_field": "patient_id",
                        "dest_field": "person_id",
                    },
                    "date_mapping": {
                        "source_field": "birth_date",
                        "dest_field": ["birth_datetime"],
                    },
                    "concept_mappings": {
                        "sex": {
                            "F": {"gender_concept_id": [8532]},
                            "original_value": [],
                        }
                    },
                }
            },
            "note": {
                "notes.csv": {
                    "person_id_mapping": {
                        "source_field": "patient_id",
                        "dest_field": "person_id",
                    },
                    "date_mapping": {
                        "source_field": "note_date",
                        "dest_field": ["note_datetime"],
                    },
                    "record_key": {"source_field": "note_ref"},
                    "field_mappings": {"note_text": "text"},
                    "value_mappings": {
                        "note_class_concept_id": {
                            "source_field": "note_type",
                            "values": {
                                "Outpatient note": OUTPATIENT,
                                "Discharge summary": DISCHARGE,
                            },
                        }
                    },
                }
            },
        },
    }
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(json.dumps(rules))

    RecordBuilderFactory.clear_person_cache()
    o = outputs.csv_output_target(output_dir)
    s = sources.csv_source_object(input_dir, sep=",")
    result = V2ProcessingOrchestrator(
        rules_file=rules_file,
        output=o,
        inputs=s,
        person="person",
        write_mode="w",
        omop_ddl_file=DDL_54,
        omop_config_file=CONFIG,
    ).execute_processing()
    s.close()
    o.close()
    assert result.success

    with (output_dir / "note.tsv").open() as f:
        notes = list(csv.DictReader(f, delimiter="\t"))
    # One note per row; unmapped and empty values get 0 (no matching concept)
    assert [(n["note_text"], n["note_class_concept_id"]) for n in notes] == [
        ("a", str(OUTPATIENT)),
        ("b", str(DISCHARGE)),
        ("c", "0"),
        ("d", "0"),
    ]
