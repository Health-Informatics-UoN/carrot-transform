"""
`code_lookup` in v2 rules: a note_nlp block whose source column holds codes of
an OMOP vocabulary (e.g. SNOMED CT codes, not OMOP concept ids), for each code's
concepts to be found in the vocabulary rather than mapped one by one.
See https://carrot.ac.uk/transform/rules_v2_notes_nlp.

Before the rules are parsed, each lookup is expanded into the rules Carrot
Mapper would export had every code been mapped by hand: concept_mappings on the
note_nlp block, and a block with `derived_from` in each domain table the codes'
concepts belong to (e.g. condition_occurrence for Condition concepts). Codes
already mapped in the rules keep their mappings.

    "code_lookup": {
        "source_field": "snomed_code",
        "vocabulary_id": "SNOMED",
        "derived_records": {
            "person_id_mapping": {"source_field": "patient_id", "dest_field": "person_id"},
            "date_mapping": {"source_field": "note_date"},
            "type_concept_id": 32858,
            "exclude_values": {"negated": ["true"]},
            "fact_relationship": {
                "domain_concept_ids": {"condition_occurrence": 19},
                "source_domain_concept_id": ...,
                "relationship_concept_id": ...,
                "reverse_relationship_concept_id": ...
            }
        }
    }
"""

import copy
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any

from carrottransform.tools import sources
from carrottransform.tools.args import remove_csv_extension
from carrottransform.tools.vocabulary import Vocabulary

logger = logging.getLogger(__name__)

NOTE_NLP = "note_nlp"


@dataclass(frozen=True)
class DomainTable:
    """Where a domain's concepts go, as Carrot Mapper maps them."""

    table: str
    concept_field: str
    source_concept_field: str | None
    source_value_field: str
    date_fields: tuple[str, ...]
    type_field: str


def _table(prefix: str, table: str, date_fields: tuple[str, ...]) -> DomainTable:
    return DomainTable(
        table=table,
        concept_field=f"{prefix}_concept_id",
        source_concept_field=f"{prefix}_source_concept_id",
        source_value_field=f"{prefix}_source_value",
        date_fields=date_fields,
        type_field=f"{prefix}_type_concept_id",
    )


DOMAIN_TABLES: dict[str, DomainTable] = {
    "Condition": _table(
        "condition",
        "condition_occurrence",
        ("condition_start_datetime", "condition_end_datetime"),
    ),
    "Drug": _table(
        "drug",
        "drug_exposure",
        ("drug_exposure_start_datetime", "drug_exposure_end_datetime"),
    ),
    "Device": _table(
        "device",
        "device_exposure",
        ("device_exposure_start_datetime", "device_exposure_end_datetime"),
    ),
    "Measurement": _table("measurement", "measurement", ("measurement_datetime",)),
    "Observation": _table("observation", "observation", ("observation_datetime",)),
    "Procedure": _table("procedure", "procedure_occurrence", ("procedure_datetime",)),
    "Specimen": DomainTable(
        table="specimen",
        concept_field="specimen_concept_id",
        source_concept_field=None,
        source_value_field="specimen_source_value",
        date_fields=("specimen_datetime",),
        type_field="specimen_type_concept_id",
    ),
}


class CodeLookupError(Exception):
    pass


def has_code_lookups(rules_data: dict) -> bool:
    return any(
        "code_lookup" in block
        for block in rules_data.get("cdm", {}).get(NOTE_NLP, {}).values()
    )


def expand_code_lookups(
    rules_data: dict, inputs: sources.SourceObject, vocabulary: Vocabulary | None
) -> dict:
    """The rules with every `code_lookup` expanded (see the module docstring)."""
    if not has_code_lookups(rules_data):
        return rules_data
    if vocabulary is None:
        tables = [
            source_table
            for source_table, block in rules_data["cdm"][NOTE_NLP].items()
            if "code_lookup" in block
        ]
        raise CodeLookupError(
            f"The rules look up the codes of {', '.join(tables)} in a vocabulary: "
            "give the vocabulary with --vocabulary (a folder of vocabulary files "
            "from Athena, or a database URL)."
        )

    rules = copy.deepcopy(rules_data)
    for source_table, block in rules["cdm"][NOTE_NLP].items():
        if "code_lookup" in block:
            _expand(rules["cdm"], source_table, block, inputs, vocabulary)
    return rules


def _distinct_values(
    inputs: sources.SourceObject, source_table: str, source_field: str
) -> Counter:
    """How often each value of the column occurs in the source table."""
    rows = inputs.open(remove_csv_extension(source_table))
    header = [str(name).strip().lower() for name in next(rows)]
    if source_field.lower() not in header:
        raise CodeLookupError(f"{source_table} has no column {source_field}")
    index = header.index(source_field.lower())
    counts: Counter = Counter()
    for row in rows:
        if index < len(row) and row[index] is not None:
            value = str(row[index])
            if value.strip():
                counts[value] += 1
    return counts


def _expand(
    cdm: dict,
    source_table: str,
    block: dict[str, Any],
    inputs: sources.SourceObject,
    vocabulary: Vocabulary,
):
    lookup = block["code_lookup"]
    source_field = lookup["source_field"]
    vocabulary_id = lookup["vocabulary_id"]
    derived = lookup.get("derived_records") or {}

    term_mappings = block.setdefault("concept_mappings", {}).setdefault(
        source_field, {}
    )
    term_mappings.setdefault("original_value", [])
    # Codes mapped by hand in Carrot Mapper keep their mappings
    hand_mapped = {value for value in term_mappings if value != "original_value"}

    counts = _distinct_values(inputs, source_table, source_field)
    values = [value for value in counts if value not in hand_mapped]
    found = vocabulary.lookup(vocabulary_id, values)

    unmatched: Counter = Counter()
    other_domains: Counter = Counter()
    for value in values:
        concepts = found.get(value.strip())
        if concepts is None or not concepts.standard_concepts:
            # Written with "no matching concept" (0), and no derived records
            term_mappings[value] = {
                "note_nlp_concept_id": [0],
                "note_nlp_source_concept_id": [
                    concepts.source_concept_id if concepts else 0
                ],
            }
            unmatched[value] = counts[value]
            continue

        standard_ids = [concept_id for concept_id, _ in concepts.standard_concepts]
        term_mappings[value] = {
            "note_nlp_concept_id": standard_ids,
            "note_nlp_source_concept_id": [concepts.source_concept_id]
            * len(standard_ids),
        }
        for concept_id, domain in concepts.standard_concepts:
            domain_table = DOMAIN_TABLES.get(domain)
            if domain_table is None:
                other_domains[domain] += 1
                continue
            _add_derived(
                cdm,
                source_table,
                source_field,
                value,
                concept_id,
                concepts.source_concept_id,
                domain_table,
                derived,
            )

    logger.info(
        f"{source_table}: looked up {len(values)} {vocabulary_id} codes in "
        f"{source_field}; {len(values) - len(unmatched)} found"
        + (f", {len(hand_mapped)} mapped in the rules" if hand_mapped else "")
    )
    if unmatched:
        examples = ", ".join(code for code, _ in unmatched.most_common(10))
        logger.warning(
            f"{source_table}: {len(unmatched)} {vocabulary_id} codes "
            f"({sum(unmatched.values())} rows) have no standard concept, so their "
            f"note_nlp records have none (0) and no records are derived from them. "
            f"Most frequent: {examples}"
        )
    if other_domains:
        logger.info(
            f"{source_table}: concepts in domains without a table for derived "
            f"records only give note_nlp records: {dict(other_domains)}"
        )


def _add_derived(
    cdm: dict,
    source_table: str,
    source_field: str,
    value: str,
    concept_id: int,
    source_concept_id: int,
    domain_table: DomainTable,
    derived: dict[str, Any],
):
    """Add a derived record mapping for a code's standard concept."""
    if "person_id_mapping" not in derived or "date_mapping" not in derived:
        raise CodeLookupError(
            f"code_lookup for {source_table} needs derived_records with "
            "person_id_mapping and date_mapping"
        )
    block = cdm.setdefault(domain_table.table, {}).get(source_table)
    if block is None:
        block = {
            "person_id_mapping": dict(derived["person_id_mapping"]),
            "date_mapping": {
                "source_field": derived["date_mapping"]["source_field"],
                "dest_field": list(domain_table.date_fields),
            },
            "concept_mappings": {},
        }
        if derived.get("type_concept_id") is not None:
            block["constant_mappings"] = {
                domain_table.type_field: int(derived["type_concept_id"])
            }
        derived_from: dict[str, Any] = {"table": NOTE_NLP}
        if derived.get("exclude_values"):
            derived_from["exclude_values"] = derived["exclude_values"]
        link = derived.get("fact_relationship")
        domain_concept = (
            (link or {}).get("domain_concept_ids", {}).get(domain_table.table)
        )
        if link and domain_concept:
            derived_from["fact_relationship"] = {
                "domain_concept_id": int(domain_concept),
                "source_domain_concept_id": int(link["source_domain_concept_id"]),
                "relationship_concept_id": int(link["relationship_concept_id"]),
                "reverse_relationship_concept_id": int(
                    link["reverse_relationship_concept_id"]
                ),
            }
        block["derived_from"] = derived_from
        cdm[domain_table.table][source_table] = block

    field_mappings = block.setdefault("concept_mappings", {}).setdefault(
        source_field, {}
    )
    original = field_mappings.setdefault("original_value", [])
    if domain_table.source_value_field not in original:
        original.append(domain_table.source_value_field)
    mapping = field_mappings.setdefault(value, {})
    mapping.setdefault(domain_table.concept_field, []).append(concept_id)
    if domain_table.source_concept_field:
        mapping.setdefault(domain_table.source_concept_field, []).append(
            source_concept_id
        )
