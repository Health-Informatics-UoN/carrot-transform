"""
End-to-end tests for the v2 record-level extensions used by note / note_nlp
(docs/rules_v2_record_extensions.md), run against the CDM 5.4 DDL and the
package's own OMOP config.
"""

import csv
import json
from pathlib import Path

import pytest

from carrottransform.tools import outputs, sources
from carrottransform.tools.mapping_types import V2TableMapping
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.orchestrator import V2ProcessingOrchestrator
from carrottransform.tools.record_builder import RecordBuilderFactory

CONFIG = Path("carrottransform/config/config.json")
DDL_54 = Path("carrottransform/config/OMOPCDM_postgresql_5.4_ddl.sql")

NLP_TYPE = 32858
EHR_TYPE = 32817
MI_SNOMED = "22298006"
MI_CONCEPT = 4329847


def _write_csv(path: Path, rows: list[list[str]]):
    with path.open("w", newline="") as f:
        csv.writer(f).writerows(rows)


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open() as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _rules() -> dict:
    # note_nlp and the NLP file deliberately come first, so the test also
    # checks that note keys are registered before they are looked up.
    return {
        "metadata": {"date_created": "2026-10-30T00:00:00+00:00", "dataset": "notes"},
        "cdm": {
            "note_nlp": {
                "nlp.csv": {
                    "date_mapping": {
                        "source_field": "note_date",
                        "dest_field": ["nlp_datetime"],
                    },
                    "references": {
                        "note_id": {"table": "note", "source_field": "note_ref"}
                    },
                    "field_mappings": {
                        "lexical_variant": "term",
                        "snippet": "snippet",
                        "offset": "offset",
                    },
                    "constant_mappings": {"nlp_system": "TestNLP 1.0"},
                    "concept_mappings": {
                        "snomed_code": {
                            MI_SNOMED: {
                                "note_nlp_concept_id": [MI_CONCEPT],
                                "note_nlp_source_concept_id": [MI_CONCEPT],
                            },
                            "original_value": [],
                        }
                    },
                }
            },
            "condition_occurrence": {
                "nlp.csv": {
                    "person_id_mapping": {
                        "source_field": "patient_id",
                        "dest_field": "person_id",
                    },
                    "date_mapping": {
                        "source_field": "note_date",
                        "dest_field": [
                            "condition_start_datetime",
                            "condition_end_datetime",
                        ],
                    },
                    "constant_mappings": {"condition_type_concept_id": NLP_TYPE},
                    "concept_mappings": {
                        "snomed_code": {
                            MI_SNOMED: {
                                "condition_concept_id": [MI_CONCEPT],
                                "condition_source_concept_id": [MI_CONCEPT],
                            },
                            "original_value": ["condition_source_value"],
                        }
                    },
                }
            },
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
                            "M": {"gender_concept_id": [8507]},
                            "original_value": ["gender_source_value"],
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
                    "field_mappings": {"note_text": "text", "note_title": "title"},
                    "constant_mappings": {
                        "note_type_concept_id": EHR_TYPE,
                        "note_class_concept_id": 1,
                        "encoding_concept_id": 2,
                        "language_concept_id": 3,
                    },
                }
            },
        },
    }


@pytest.fixture
def run(tmp_path):
    """Write inputs + rules, run the orchestrator, return the output directory"""
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()

    _write_csv(
        input_dir / "person.csv",
        [
            ["patient_id", "birth_date", "sex"],
            ["p1", "1970-01-01", "F"],
            ["p2", "1980-05-05", "M"],
        ],
    )
    _write_csv(
        input_dir / "notes.csv",
        [
            ["note_ref", "patient_id", "note_date", "title", "text"],
            [
                "N-1",
                "p1",
                "2024-02-01 09:30:00",
                "Clinic letter",
                "Seen in clinic, MI noted.",
            ],
            ["N-2", "p2", "2024-03-01", "Discharge", "No acute findings."],
            # unknown person: the note is rejected, so its key is never registered
            ["N-3", "p9", "2024-04-01", "Orphan", "Not linked to a person."],
        ],
    )
    _write_csv(
        input_dir / "nlp.csv",
        [
            [
                "note_ref",
                "patient_id",
                "note_date",
                "snomed_code",
                "term",
                "snippet",
                "offset",
            ],
            ["N-1", "p1", "2024-02-01 09:30:00", MI_SNOMED, "MI", "MI noted", "20"],
            ["N-1", "p1", "2024-02-01 09:30:00", "999999", "other", "other", "3"],
            ["N-2", "p2", "2024-03-01", MI_SNOMED, "heart attack", "heart attack", "7"],
            ["N-3", "p9", "2024-04-01", MI_SNOMED, "MI", "MI", "1"],
            ["N-404", "p1", "2024-05-01", MI_SNOMED, "MI", "MI", "1"],
        ],
    )

    rules_file = tmp_path / "rules.json"
    rules_file.write_text(json.dumps(_rules()))

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
    return output_dir


def test_note_one_record_per_row(run):
    notes = _read_tsv(run / "note.tsv")
    person_ids = {
        r["SOURCE_SUBJECT"]: r["TARGET_SUBJECT"]
        for r in _read_tsv(run / "person_ids.tsv")
    }

    assert len(notes) == 2  # N-3 has no person
    n1, n2 = notes
    assert n1["note_id"] == "1" and n2["note_id"] == "2"
    assert n1["person_id"] == person_ids["p1"]
    assert n1["note_text"] == "Seen in clinic, MI noted."
    assert n1["note_title"] == "Clinic letter"
    assert n1["note_datetime"].startswith("2024-02-01")
    assert n1["note_date"] == "2024-02-01"
    assert n1["note_type_concept_id"] == str(EHR_TYPE)
    assert (
        n1["note_class_concept_id"],
        n1["encoding_concept_id"],
        n1["language_concept_id"],
    ) == ("1", "2", "3")


def test_note_nlp_links_to_note(run):
    notes = {r["note_text"]: r["note_id"] for r in _read_tsv(run / "note.tsv")}
    nlp = _read_tsv(run / "note_nlp.tsv")

    # Only mapped codes, and only rows whose note exists (not N-3 / N-404)
    assert [r["lexical_variant"] for r in nlp] == ["MI", "heart attack"]
    assert nlp[0]["note_id"] == notes["Seen in clinic, MI noted."]
    assert nlp[1]["note_id"] == notes["No acute findings."]
    assert nlp[0]["note_nlp_id"] == "1" and nlp[1]["note_nlp_id"] == "2"
    assert nlp[0]["note_nlp_concept_id"] == str(MI_CONCEPT)
    assert nlp[0]["offset"] == "20"
    assert nlp[0]["snippet"] == "MI noted"
    assert nlp[0]["nlp_system"] == "TestNLP 1.0"
    assert nlp[0]["nlp_date"] == "2024-02-01"
    assert "person_id" not in nlp[0]


def test_derived_conditions_carry_nlp_type(run):
    conditions = _read_tsv(run / "condition_occurrence.tsv")

    # One per mapped code with a known person (N-404's patient p1 is known)
    assert len(conditions) == 3
    assert {r["condition_concept_id"] for r in conditions} == {str(MI_CONCEPT)}
    assert {r["condition_type_concept_id"] for r in conditions} == {str(NLP_TYPE)}
    assert {r["condition_source_value"] for r in conditions} == {MI_SNOMED}


def _parse(rules: dict, tmp_path: Path) -> MappingRules:
    path = tmp_path / "r.json"
    path.write_text(json.dumps(rules))
    return MappingRules(path, OmopCDM(DDL_54, CONFIG))


def test_files_with_record_keys_are_processed_first(tmp_path):
    rules = _parse(_rules(), tmp_path)
    files = rules.get_all_infile_names()

    assert files.index("notes.csv") < files.index("nlp.csv")
    assert (
        rules.get_target_order({"note_nlp", "note", "condition_occurrence"})[-1]
        == "note_nlp"
    )


def test_reference_to_table_without_record_key_is_rejected(tmp_path):
    rules = _rules()
    del rules["cdm"]["note"]["notes.csv"]["record_key"]

    with pytest.raises(
        ValueError, match="no source table for 'note' defines a record_key"
    ):
        _parse(rules, tmp_path)


def test_block_without_concepts_or_extensions_is_not_row_mapped():
    mapping = V2TableMapping(
        source_table="x.csv",
        person_id_mapping=None,
        date_mapping=None,
        concept_mappings={},
    )
    assert not mapping.is_row_mapping
    mapping.constant_mappings["note_type_concept_id"] = 1
    assert mapping.is_row_mapping


def test_quoted_ddl_columns_are_parsed():
    columns = OmopCDM(DDL_54, CONFIG).get_omop_column_list("note_nlp")
    assert "offset" in columns
    assert len(columns) == 14
