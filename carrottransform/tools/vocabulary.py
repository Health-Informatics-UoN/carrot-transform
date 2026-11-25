"""
OMOP vocabulary lookups for `code_lookup` rules: finding the concepts for codes
of a vocabulary (e.g. SNOMED CT codes), as the vocabulary's "Maps to"
relationships give them.

The vocabulary is read from either
- a folder of vocabulary files as downloaded from Athena (CONCEPT.csv and
  CONCEPT_RELATIONSHIP.csv, tab-separated), or
- a database holding the OMOP `concept` and `concept_relationship` tables,
  given as an SQLAlchemy URL.

Only the concepts and relationships the lookup needs are kept in memory.
"""

import csv
import logging
import re
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

import click
import sqlalchemy

from carrottransform.tools import at_path

logger = logging.getLogger(__name__)

# Concept names in Athena's files can be long
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))


@dataclass
class CodeConcepts:
    """What a code of a vocabulary maps to."""

    source_concept_id: int
    # Standard concepts the source concept maps to, with their domains
    standard_concepts: list[tuple[int, str]] = field(default_factory=list)


class Vocabulary:
    """Looks up codes of a vocabulary. Subclasses read the vocabulary tables."""

    def lookup(
        self, vocabulary_id: str, codes: Iterable[str]
    ) -> dict[str, CodeConcepts]:
        """
        The concepts for each code found in the vocabulary: its own concept
        (preferring a valid one) and the standard concepts it "Maps to". A
        standard concept with no "Maps to" relationship maps to itself.
        Codes that aren't found are left out.
        """
        wanted = {code.strip() for code in codes if code and code.strip()}
        if not wanted:
            return {}

        # code -> (concept_id, valid, standard)
        found: dict[str, tuple[int, bool, bool]] = {}
        for concept_id, code, invalid_reason, standard in self._source_concepts(
            vocabulary_id, wanted
        ):
            valid = not invalid_reason
            current = found.get(code)
            if current is None or (valid and not current[1]):
                found[code] = (concept_id, valid, standard == "S")
        if not found:
            return {}

        source_ids = {concept_id for concept_id, _, _ in found.values()}
        maps_to: dict[int, list[int]] = {}
        for source_id, target_id in self._maps_to(source_ids):
            maps_to.setdefault(source_id, []).append(target_id)
        for concept_id, _, is_standard in found.values():
            if is_standard and concept_id not in maps_to:
                maps_to[concept_id] = [concept_id]

        target_ids = {t for targets in maps_to.values() for t in targets}
        domains = dict(self._domains(target_ids))

        result = {}
        for code, (concept_id, _, _) in found.items():
            targets = sorted(set(maps_to.get(concept_id, [])))
            result[code] = CodeConcepts(
                source_concept_id=concept_id,
                standard_concepts=[(t, domains[t]) for t in targets if t in domains],
            )
        return result

    def _source_concepts(
        self, vocabulary_id: str, codes: set[str]
    ) -> Iterator[tuple[int, str, str, str]]:
        """(concept_id, concept_code, invalid_reason, standard_concept) per match."""
        raise NotImplementedError

    def _maps_to(self, concept_ids: set[int]) -> Iterator[tuple[int, int]]:
        """Valid "Maps to" relationships (concept_id_1, concept_id_2) from the ids."""
        raise NotImplementedError

    def _domains(self, concept_ids: set[int]) -> Iterator[tuple[int, str]]:
        """(concept_id, domain_id) for the ids."""
        raise NotImplementedError


class FolderVocabulary(Vocabulary):
    """A folder of vocabulary files as downloaded from Athena."""

    def __init__(self, path: Path):
        if not path.is_dir():
            raise click.BadParameter(f"{path} isn't a folder")
        self._concept = self._find(path, "concept")
        self._relationship = self._find(path, "concept_relationship")

    @staticmethod
    def _find(path: Path, table: str) -> Path:
        for candidate in path.iterdir():
            if (
                candidate.is_file()
                and candidate.stem.lower() == table
                and (candidate.suffix.lower() in (".csv", ".tsv", ".txt"))
            ):
                return candidate
        raise click.BadParameter(
            f"{path} has no {table.upper()}.csv (vocabulary files as downloaded from Athena)"
        )

    @staticmethod
    def _rows(file: Path, columns: list[str]) -> Iterator[list[str]]:
        """The given columns of each row. Athena's files are tab-separated and
        unquoted; comma-separated files with a header are read too."""
        with file.open("r", encoding="utf-8-sig", newline="") as f:
            header_line = f.readline().rstrip("\r\n")
            if "\t" in header_line:
                header = header_line.split("\t")
                reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
            else:
                header = next(csv.reader([header_line]))
                reader = csv.reader(f)
            index = {name.strip().lower(): i for i, name in enumerate(header)}
            missing = [c for c in columns if c not in index]
            if missing:
                raise ValueError(f"{file.name} has no {', '.join(missing)} column")
            positions = [index[c] for c in columns]
            width = max(positions) + 1
            for row in reader:
                if len(row) >= width:
                    yield [row[i] for i in positions]

    def _source_concepts(self, vocabulary_id, codes):
        for concept_id, code, vocabulary, invalid, standard in self._rows(
            self._concept,
            [
                "concept_id",
                "concept_code",
                "vocabulary_id",
                "invalid_reason",
                "standard_concept",
            ],
        ):
            if vocabulary == vocabulary_id and code in codes:
                yield int(concept_id), code, invalid, standard

    def _maps_to(self, concept_ids):
        for source, target, relationship, invalid in self._rows(
            self._relationship,
            ["concept_id_1", "concept_id_2", "relationship_id", "invalid_reason"],
        ):
            if relationship == "Maps to" and not invalid:
                source_id = int(source)
                if source_id in concept_ids:
                    yield source_id, int(target)

    def _domains(self, concept_ids):
        for concept_id, domain in self._rows(
            self._concept, ["concept_id", "domain_id"]
        ):
            if int(concept_id) in concept_ids:
                yield int(concept_id), domain


class DatabaseVocabulary(Vocabulary):
    """The `concept` and `concept_relationship` tables of a database."""

    CHUNK = 1000

    def __init__(self, engine: sqlalchemy.engine.Engine):
        self._engine = engine

    def _query(self, sql: str, name: str, values: Iterable, **params):
        statement = sqlalchemy.text(sql).bindparams(
            sqlalchemy.bindparam(name, expanding=True)
        )
        values = list(values)
        with self._engine.connect() as connection:
            for start in range(0, len(values), self.CHUNK):
                chunk = values[start : start + self.CHUNK]
                yield from connection.execute(statement, {name: chunk, **params})

    def _source_concepts(self, vocabulary_id, codes):
        for row in self._query(
            "SELECT concept_id, concept_code, invalid_reason, standard_concept "
            "FROM concept WHERE vocabulary_id = :vocabulary AND concept_code IN :codes",
            "codes",
            codes,
            vocabulary=vocabulary_id,
        ):
            yield int(row[0]), str(row[1]), row[2] or "", row[3] or ""

    def _maps_to(self, concept_ids):
        for row in self._query(
            "SELECT concept_id_1, concept_id_2 FROM concept_relationship "
            "WHERE relationship_id = 'Maps to' "
            "AND (invalid_reason IS NULL OR invalid_reason = '') "
            "AND concept_id_1 IN :ids",
            "ids",
            concept_ids,
        ):
            yield int(row[0]), int(row[1])

    def _domains(self, concept_ids):
        for row in self._query(
            "SELECT concept_id, domain_id FROM concept WHERE concept_id IN :ids",
            "ids",
            concept_ids,
        ):
            yield int(row[0]), str(row[1])


class VocabularyArgumentType(click.ParamType):
    name = "vocabulary folder or database URL"

    def convert(self, value, param, ctx):
        if isinstance(value, Vocabulary):
            return value
        value = str(value)
        if re.match(r"[\w+]+://.+", value):
            return DatabaseVocabulary(sqlalchemy.create_engine(value))
        try:
            return FolderVocabulary(at_path.convert_path(value))
        except click.BadParameter as e:
            self.fail(str(e), param, ctx)


VocabularyArgument = VocabularyArgumentType()
