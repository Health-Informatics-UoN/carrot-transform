"""
End-to-end tests for `derived_from` (https://carrot.ac.uk/transform/rules_v2_notes_nlp):
domain records derived from note_nlp records are only written when the row's
note_nlp record was, unless an exclusion matches, and are linked to it in
fact_relationship in both directions.
"""

import csv
import json
from pathlib import Path

import pytest

from carrottransform.tools import outputs, sources
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.orchestrator import V2ProcessingOrchestrator
from carrottransform.tools.record_builder import RecordBuilderFactory

CONFIG = Path("carrottransform/config/config.json")
DDL_54 = Path("carrottransform/config/OMOPCDM_postgresql_5.4_ddl.sql")

MI_CODE, MI = "22298006", 4329847
DIAB_CODE, DIAB_A, DIAB_B = "73211009", 201820, 201826  # one code, two concepts

# Illustrative ids: Mapper fills these in from the vocabulary / table settings
CONDITION_DOMAIN, NOTE_NLP_DOMAIN = 900019, 905085
DERIVED, DERIVED_REVERSE = 900101, 900102


def _write_csv(path: Path, rows: list[list[str]]):
    with path.open("w", newline="") as f:
        csv.writer(f).writerows(rows)


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open() as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _rules(fact_relationship: bool = True) -> dict:
    derived_from: dict = {
        "table": "note_nlp",
        "exclude_values": {"negated": ["true", "1"], "experiencer": ["family"]},
    }
    if fact_relationship:
        derived_from["fact_relationship"] = {
            "domain_concept_id": CONDITION_DOMAIN,
            "source_domain_concept_id": NOTE_NLP_DOMAIN,
            "relationship_concept_id": DERIVED,
            "reverse_relationship_concept_id": DERIVED_REVERSE,
        }
    codes = {
        MI_CODE: [MI],
        DIAB_CODE: [DIAB_A, DIAB_B],
    }
    return {
        "metadata": {"date_created": "2026-11-12T00:00:00+00:00", "dataset": "nlp"},
        "cdm": {
            # Derived table first, to check it's still processed after note_nlp
            "condition_occurrence": {
                "nlp.csv": {
                    "person_id_mapping": {
                        "source_field": "patient_id",
                        "dest_field": "person_id",
                    },
                    "date_mapping": {
                        "source_field": "note_date",
                        "dest_field": ["condition_start_datetime"],
                    },
                    "constant_mappings": {"condition_type_concept_id": 32858},
                    "derived_from": derived_from,
                    "concept_mappings": {
                        "snomed_code": {
                            **{
                                code: {"condition_concept_id": ids}
                                for code, ids in codes.items()
                            },
                            "original_value": ["condition_source_value"],
                        }
                    },
                }
            },
            "note_nlp": {
                "nlp.csv": {
                    "date_mapping": {
                        "source_field": "note_date",
                        "dest_field": ["nlp_datetime"],
                    },
                    "references": {
                        "note_id": {"table": "note", "source_field": "note_ref"}
                    },
                    "field_mappings": {"lexical_variant": "term"},
                    "concept_mappings": {
                        "snomed_code": {
                            **{
                                code: {"note_nlp_concept_id": ids}
                                for code, ids in codes.items()
                            },
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
                            "original_value": ["gender_source_value"],
                        }
                    },
                }
            },
        },
    }


def _run(tmp_path: Path, rules: dict) -> Path:
    input_dir, output_dir = tmp_path / "input", tmp_path / "output"
    input_dir.mkdir()
    _write_csv(
        input_dir / "person.csv",
        [
            ["patient_id", "birth_date", "sex"],
            ["p1", "1970-01-01", "F"],
            ["p2", "1980-01-01", "F"],
        ],
    )
    _write_csv(
        input_dir / "notes.csv",
        [
            ["note_ref", "patient_id", "note_date", "text"],
            ["N-1", "p1", "2025-01-10", "Seen for MI and diabetes."],
            ["N-2", "p2", "2025-02-20", "Mother had MI."],
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
                "negated",
                "experiencer",
            ],
            ["N-1", "p1", "2025-01-10", MI_CODE, "MI", "false", "patient"],
            ["N-1", "p1", "2025-01-10", DIAB_CODE, "diabetes", "", "Patient"],
            [
                "N-1",
                "p1",
                "2025-01-10",
                MI_CODE,
                "no MI",
                " TRUE ",
                "patient",
            ],  # negated
            [
                "N-2",
                "p2",
                "2025-02-20",
                MI_CODE,
                "MI",
                "false",
                "Family",
            ],  # family history
            [
                "N-404",
                "p1",
                "2025-03-01",
                MI_CODE,
                "MI",
                "false",
                "patient",
            ],  # no such note
            ["N-2", "p2", "2025-02-20", "99999999", "unmapped", "false", "patient"],
        ],
    )
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
    return output_dir


@pytest.fixture
def out(tmp_path):
    return _run(tmp_path, _rules())


def test_note_nlp_records_are_all_kept(out):
    nlp = _read_tsv(out / "note_nlp.tsv")

    # Negated and family-history terms are still NLP output; N-404 has no note
    assert [(r["lexical_variant"], r["note_nlp_concept_id"]) for r in nlp] == [
        ("MI", str(MI)),
        ("diabetes", str(DIAB_A)),
        ("diabetes", str(DIAB_B)),
        ("no MI", str(MI)),
        ("MI", str(MI)),
    ]


def test_conditions_only_for_linked_and_not_excluded_rows(out):
    conditions = _read_tsv(out / "condition_occurrence.tsv")

    assert [r["condition_concept_id"] for r in conditions] == [
        str(MI),
        str(DIAB_A),
        str(DIAB_B),
    ]
    assert {r["condition_type_concept_id"] for r in conditions} == {"32858"}


def test_fact_relationship_links_both_ways_by_concept(out):
    # The note_nlp records of N-1's linked rows (not the negated one)
    nlp_id = {
        (r["lexical_variant"], r["note_nlp_concept_id"]): r["note_nlp_id"]
        for r in _read_tsv(out / "note_nlp.tsv")
        if r["note_id"] == "1" and r["lexical_variant"] != "no MI"
    }
    condition_id = {
        r["condition_concept_id"]: r["condition_occurrence_id"]
        for r in _read_tsv(out / "condition_occurrence.tsv")
    }
    links = [tuple(r.values()) for r in _read_tsv(out / "fact_relationship.tsv")]

    expected = []
    for term, concept in (("MI", MI), ("diabetes", DIAB_A), ("diabetes", DIAB_B)):
        nlp = nlp_id[(term, str(concept))]
        cond = condition_id[str(concept)]
        expected += [
            (str(NOTE_NLP_DOMAIN), nlp, str(CONDITION_DOMAIN), cond, str(DERIVED)),
            (
                str(CONDITION_DOMAIN),
                cond,
                str(NOTE_NLP_DOMAIN),
                nlp,
                str(DERIVED_REVERSE),
            ),
        ]
    assert links == expected


def test_without_fact_relationship_records_are_gated_but_not_linked(tmp_path):
    out = _run(tmp_path, _rules(fact_relationship=False))

    assert len(_read_tsv(out / "condition_occurrence.tsv")) == 3
    assert not (out / "fact_relationship.tsv").exists()


def _parse(rules: dict, tmp_path: Path) -> MappingRules:
    path = tmp_path / "r.json"
    path.write_text(json.dumps(rules))
    return MappingRules(path, OmopCDM(DDL_54, CONFIG))


def test_derived_tables_are_written_after_their_source(tmp_path):
    rules = _parse(_rules(), tmp_path)

    assert rules.get_target_order({"condition_occurrence", "note_nlp", "note"}) == [
        "note",
        "note_nlp",
        "condition_occurrence",
    ]
    assert rules.uses_fact_relationship


def test_derived_from_table_must_be_fed_by_the_same_source(tmp_path):
    rules = _rules()
    rules["cdm"]["condition_occurrence"]["nlp.csv"]["derived_from"]["table"] = "note"

    with pytest.raises(ValueError, match="nlp.csv has no 'note' block"):
        _parse(rules, tmp_path)


def test_ddl_columns_with_digits_are_parsed():
    columns = OmopCDM(DDL_54, CONFIG).get_omop_column_list("fact_relationship")
    assert columns == [
        "domain_concept_id_1",
        "fact_id_1",
        "domain_concept_id_2",
        "fact_id_2",
        "relationship_concept_id",
    ]
