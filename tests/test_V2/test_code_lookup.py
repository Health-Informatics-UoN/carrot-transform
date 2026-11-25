"""
Tests for `code_lookup`: note_nlp terms whose codes (e.g. SNOMED CT codes) are
looked up in an OMOP vocabulary, read from Athena's files or from a database.
"""

import csv
import json
import sqlite3
from pathlib import Path

import pytest
import sqlalchemy

from carrottransform.tools import outputs, sources
from carrottransform.tools.code_lookup import CodeLookupError, expand_code_lookups
from carrottransform.tools.orchestrator import V2ProcessingOrchestrator
from carrottransform.tools.record_builder import RecordBuilderFactory
from carrottransform.tools.vocabulary import (
    DatabaseVocabulary,
    FolderVocabulary,
    VocabularyArgument,
)

CONFIG = Path("carrottransform/config/config.json")
DDL_54 = Path("carrottransform/config/OMOPCDM_postgresql_5.4_ddl.sql")

# Illustrative concepts. Codes: MI is standard (maps to itself); "OLD" is a
# non-standard code mapping to two Condition concepts; "SMOKER" maps to an
# Observation; "GONE" has a deprecated concept and no mapping.
MI_CODE, MI = "22298006", 4329847
OLD_CODE, OLD_SOURCE, OLD_A, OLD_B = "1111111", 9000001, 201820, 201826
SMOKER_CODE, SMOKER_SOURCE, SMOKER = "77176002", 9000002, 903657
GONE_CODE, GONE = "5555555", 9000003

CONCEPTS = [
    # concept_id, name, domain, vocabulary, class, standard, code, invalid
    (
        MI,
        "Myocardial infarction",
        "Condition",
        "SNOMED",
        "Clinical Finding",
        "S",
        MI_CODE,
        "",
    ),
    (
        OLD_SOURCE,
        "Old code",
        "Condition",
        "SNOMED",
        "Clinical Finding",
        "",
        OLD_CODE,
        "",
    ),
    (
        OLD_A,
        "Diabetes A",
        "Condition",
        "SNOMED",
        "Clinical Finding",
        "S",
        "44054006",
        "",
    ),
    (
        OLD_B,
        "Diabetes B",
        "Condition",
        "SNOMED",
        "Clinical Finding",
        "S",
        "46635009",
        "",
    ),
    (
        SMOKER_SOURCE,
        'Smoker "current"',
        "Observation",
        "SNOMED",
        "Finding",
        "",
        SMOKER_CODE,
        "",
    ),
    (
        SMOKER,
        "Current smoker",
        "Observation",
        "SNOMED",
        "Finding",
        "S",
        "449868002",
        "",
    ),
    (GONE, "Deprecated", "Condition", "SNOMED", "Clinical Finding", "", GONE_CODE, "D"),
    # Same code in another vocabulary: must not match
    (7000001, "Other vocab", "Condition", "ICD10", "ICD10 code", "", MI_CODE, ""),
]
RELATIONSHIPS = [
    # concept_id_1, concept_id_2, relationship, invalid
    (MI, MI, "Maps to", ""),
    (OLD_SOURCE, OLD_A, "Maps to", ""),
    (OLD_SOURCE, OLD_B, "Maps to", ""),
    (OLD_SOURCE, 201234, "Maps to", "D"),  # withdrawn
    (SMOKER_SOURCE, SMOKER, "Maps to", ""),
    (SMOKER_SOURCE, MI, "Is a", ""),  # not a mapping
    (7000001, 201234, "Maps to", ""),
]
CONCEPT_COLUMNS = [
    "concept_id",
    "concept_name",
    "domain_id",
    "vocabulary_id",
    "concept_class_id",
    "standard_concept",
    "concept_code",
    "invalid_reason",
]
RELATIONSHIP_COLUMNS = [
    "concept_id_1",
    "concept_id_2",
    "relationship_id",
    "invalid_reason",
]


@pytest.fixture
def athena_folder(tmp_path) -> Path:
    """Vocabulary files as Athena ships them: tab-separated, unquoted."""
    folder = tmp_path / "vocabulary"
    folder.mkdir()
    for name, columns, rows in (
        ("CONCEPT.csv", CONCEPT_COLUMNS, CONCEPTS),
        ("CONCEPT_RELATIONSHIP.csv", RELATIONSHIP_COLUMNS, RELATIONSHIPS),
    ):
        lines = ["\t".join(columns)] + ["\t".join(map(str, row)) for row in rows]
        (folder / name).write_text("\n".join(lines) + "\n")
    return folder


@pytest.fixture
def vocabulary_db(tmp_path) -> str:
    path = tmp_path / "vocabulary.db"
    connection = sqlite3.connect(path)
    connection.execute(f"CREATE TABLE concept ({', '.join(CONCEPT_COLUMNS)})")
    connection.execute(
        f"CREATE TABLE concept_relationship ({', '.join(RELATIONSHIP_COLUMNS)})"
    )
    connection.executemany(
        "INSERT INTO concept VALUES (?,?,?,?,?,?,?,?)",
        [tuple(None if v == "" else v for v in row) for row in CONCEPTS],
    )
    connection.executemany(
        "INSERT INTO concept_relationship VALUES (?,?,?,?)",
        [tuple(None if v == "" else v for v in row) for row in RELATIONSHIPS],
    )
    connection.commit()
    connection.close()
    return f"sqlite:///{path}"


def _expected(found):
    return {
        code: (c.source_concept_id, c.standard_concepts) for code, c in found.items()
    }


EXPECTED = {
    MI_CODE: (MI, [(MI, "Condition")]),
    OLD_CODE: (OLD_SOURCE, [(OLD_A, "Condition"), (OLD_B, "Condition")]),
    SMOKER_CODE: (SMOKER_SOURCE, [(SMOKER, "Observation")]),
    GONE_CODE: (GONE, []),
}


def test_folder_lookup(athena_folder):
    found = FolderVocabulary(athena_folder).lookup(
        "SNOMED", [MI_CODE, OLD_CODE, SMOKER_CODE, GONE_CODE, " 22298006 ", "404"]
    )
    assert _expected(found) == EXPECTED


def test_database_lookup(vocabulary_db):
    vocabulary = DatabaseVocabulary(sqlalchemy.create_engine(vocabulary_db))
    found = vocabulary.lookup(
        "SNOMED", [MI_CODE, OLD_CODE, SMOKER_CODE, GONE_CODE, "404"]
    )
    assert _expected(found) == EXPECTED


def test_vocabulary_argument(athena_folder, vocabulary_db, tmp_path):
    assert isinstance(
        VocabularyArgument.convert(str(athena_folder), None, None), FolderVocabulary
    )
    assert isinstance(
        VocabularyArgument.convert(vocabulary_db, None, None), DatabaseVocabulary
    )
    (tmp_path / "empty").mkdir()
    with pytest.raises(Exception, match="CONCEPT.csv"):
        VocabularyArgument.convert(str(tmp_path / "empty"), None, None)


def _write_csv(path: Path, rows: list[list[str]]):
    with path.open("w", newline="") as f:
        csv.writer(f).writerows(rows)


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open() as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _inputs(tmp_path: Path) -> Path:
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_csv(
        input_dir / "person.csv",
        [["patient_id", "birth_date", "sex"], ["p1", "1970-01-01", "F"]],
    )
    _write_csv(
        input_dir / "notes.csv",
        [
            ["note_ref", "patient_id", "note_date", "text"],
            ["N-1", "p1", "2025-01-10", "Seen for MI. Smoker."],
        ],
    )
    _write_csv(
        input_dir / "nlp.csv",
        [
            ["note_ref", "patient_id", "note_date", "snomed_code", "term", "negated"],
            ["N-1", "p1", "2025-01-10", MI_CODE, "MI", "false"],
            ["N-1", "p1", "2025-01-10", OLD_CODE, "diabetes", "false"],
            ["N-1", "p1", "2025-01-10", SMOKER_CODE, "smoker", "false"],
            ["N-1", "p1", "2025-01-10", MI_CODE, "no MI", "true"],  # excluded
            ["N-1", "p1", "2025-01-10", GONE_CODE, "old", "false"],  # no mapping
            ["N-1", "p1", "2025-01-10", "404", "unknown", "false"],  # not found
            ["N-1", "p1", "2025-01-10", "HAND", "hand", "false"],  # mapped in rules
        ],
    )
    return input_dir


HAND = 4000001


def _rules() -> dict:
    return {
        "metadata": {"date_created": "2026-11-25T00:00:00+00:00", "dataset": "nlp"},
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
                    "concept_mappings": {},
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
                    # A code mapped by hand in Carrot Mapper keeps its mapping
                    "concept_mappings": {
                        "snomed_code": {
                            "HAND": {
                                "note_nlp_concept_id": [HAND],
                                "note_nlp_source_concept_id": [HAND],
                            },
                            "original_value": [],
                        }
                    },
                    "code_lookup": {
                        "source_field": "snomed_code",
                        "vocabulary_id": "SNOMED",
                        "derived_records": {
                            "person_id_mapping": {
                                "source_field": "patient_id",
                                "dest_field": "person_id",
                            },
                            "date_mapping": {"source_field": "note_date"},
                            "type_concept_id": 32858,
                            "exclude_values": {"negated": ["true"]},
                        },
                    },
                }
            },
        },
    }


def test_expansion_matches_mapper_rules(tmp_path, athena_folder):
    inputs = sources.csv_source_object(_inputs(tmp_path), sep=",")
    cdm = expand_code_lookups(_rules(), inputs, FolderVocabulary(athena_folder))["cdm"]

    terms = cdm["note_nlp"]["nlp.csv"]["concept_mappings"]["snomed_code"]
    assert terms[MI_CODE] == {
        "note_nlp_concept_id": [MI],
        "note_nlp_source_concept_id": [MI],
    }
    assert terms[OLD_CODE] == {
        "note_nlp_concept_id": [OLD_A, OLD_B],
        "note_nlp_source_concept_id": [OLD_SOURCE, OLD_SOURCE],
    }
    assert terms[GONE_CODE] == {
        "note_nlp_concept_id": [0],
        "note_nlp_source_concept_id": [GONE],
    }
    assert terms["404"] == {
        "note_nlp_concept_id": [0],
        "note_nlp_source_concept_id": [0],
    }
    assert terms["HAND"]["note_nlp_concept_id"] == [HAND]

    conditions = cdm["condition_occurrence"]["nlp.csv"]
    assert conditions["date_mapping"] == {
        "source_field": "note_date",
        "dest_field": ["condition_start_datetime", "condition_end_datetime"],
    }
    assert conditions["constant_mappings"] == {"condition_type_concept_id": 32858}
    assert conditions["derived_from"] == {
        "table": "note_nlp",
        "exclude_values": {"negated": ["true"]},
    }
    codes = conditions["concept_mappings"]["snomed_code"]
    assert codes["original_value"] == ["condition_source_value"]
    assert codes[OLD_CODE] == {
        "condition_concept_id": [OLD_A, OLD_B],
        "condition_source_concept_id": [OLD_SOURCE, OLD_SOURCE],
    }
    assert set(codes) == {MI_CODE, OLD_CODE, "original_value"}
    assert set(cdm["observation"]["nlp.csv"]["concept_mappings"]["snomed_code"]) == {
        SMOKER_CODE,
        "original_value",
    }


def test_fact_relationship_links_use_each_tables_domain(tmp_path, athena_folder):
    rules = _rules()
    rules["cdm"]["note_nlp"]["nlp.csv"]["code_lookup"]["derived_records"][
        "fact_relationship"
    ] = {
        "domain_concept_ids": {"condition_occurrence": 19, "observation": 27},
        "source_domain_concept_id": 905085,
        "relationship_concept_id": 900101,
        "reverse_relationship_concept_id": 900102,
    }
    inputs = sources.csv_source_object(_inputs(tmp_path), sep=",")
    cdm = expand_code_lookups(rules, inputs, FolderVocabulary(athena_folder))["cdm"]
    link = cdm["observation"]["nlp.csv"]["derived_from"]["fact_relationship"]
    assert link["domain_concept_id"] == 27
    assert link["source_domain_concept_id"] == 905085


def test_code_lookup_needs_a_vocabulary(tmp_path):
    inputs = sources.csv_source_object(_inputs(tmp_path), sep=",")
    with pytest.raises(CodeLookupError, match="--vocabulary"):
        expand_code_lookups(_rules(), inputs, None)


@pytest.mark.parametrize("source", ["folder", "database"])
def test_run_with_code_lookup(tmp_path, athena_folder, vocabulary_db, source):
    input_dir, output_dir = _inputs(tmp_path), tmp_path / "output"
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(json.dumps(_rules()))
    vocabulary = (
        FolderVocabulary(athena_folder)
        if source == "folder"
        else DatabaseVocabulary(sqlalchemy.create_engine(vocabulary_db))
    )

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
        vocabulary=vocabulary,
    ).execute_processing()
    s.close()
    o.close()
    assert result.success

    note_ids = {row["note_id"] for row in _read_tsv(output_dir / "note.tsv")}
    terms = [
        (
            row["lexical_variant"],
            row["note_nlp_concept_id"],
            row["note_nlp_source_concept_id"],
        )
        for row in _read_tsv(output_dir / "note_nlp.tsv")
    ]
    assert sorted(terms) == sorted(
        [
            ("MI", str(MI), str(MI)),
            ("diabetes", str(OLD_A), str(OLD_SOURCE)),
            ("diabetes", str(OLD_B), str(OLD_SOURCE)),
            ("smoker", str(SMOKER), str(SMOKER_SOURCE)),
            ("no MI", str(MI), str(MI)),
            ("old", "0", str(GONE)),
            ("unknown", "0", "0"),
            ("hand", str(HAND), str(HAND)),
        ]
    )
    assert {
        row["note_id"] for row in _read_tsv(output_dir / "note_nlp.tsv")
    } <= note_ids

    conditions = [
        (
            row["condition_concept_id"],
            row["condition_source_concept_id"],
            row["condition_source_value"],
            row["condition_type_concept_id"],
            row["condition_start_date"],
        )
        for row in _read_tsv(output_dir / "condition_occurrence.tsv")
    ]
    # The negated MI and the codes without a standard concept give none
    assert sorted(conditions) == sorted(
        [
            (str(MI), str(MI), MI_CODE, "32858", "2025-01-10"),
            (str(OLD_A), str(OLD_SOURCE), OLD_CODE, "32858", "2025-01-10"),
            (str(OLD_B), str(OLD_SOURCE), OLD_CODE, "32858", "2025-01-10"),
        ]
    )
    observations = _read_tsv(output_dir / "observation.tsv")
    assert [
        (o["observation_concept_id"], o["observation_type_concept_id"])
        for o in observations
    ] == [(str(SMOKER), "32858")]


def test_command_line(tmp_path, athena_folder):
    from click.testing import CliRunner

    from carrottransform.cli.subcommands.run import run

    input_dir, output_dir = _inputs(tmp_path), tmp_path / "output"
    rules_file = tmp_path / "rules.json"
    rules_file.write_text(json.dumps(_rules()))
    common = [
        "v2",
        "--inputs",
        str(input_dir),
        "--rules-file",
        str(rules_file),
        "--person",
        "person",
        "--output",
        str(output_dir),
        "--omop-ddl-file",
        str(DDL_54),
    ]

    RecordBuilderFactory.clear_person_cache()
    result = CliRunner().invoke(run, [*common, "--vocabulary", str(athena_folder)])
    assert result.exit_code == 0, result.output
    observations = _read_tsv(output_dir / "observation.tsv")
    assert [o["observation_concept_id"] for o in observations] == [str(SMOKER)]

    # Without a vocabulary, the rules can't be run
    RecordBuilderFactory.clear_person_cache()
    result = CliRunner().invoke(run, common)
    assert result.exit_code != 0
    assert "--vocabulary" in str(result.exception)
