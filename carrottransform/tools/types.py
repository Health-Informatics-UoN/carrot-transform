from dataclasses import dataclass, field
from typing import Mapping, TextIO

from case_insensitive_dict import CaseInsensitiveDict

import carrottransform.tools as tools
import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources
from carrottransform.tools.mapping_types import V2TableMapping
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM


@dataclass
class ProcessingContext:
    """Context object containing all processing configuration and state"""

    mappingrules: MappingRules
    omopcdm: OmopCDM
    person_lookup: dict[str, str]
    record_numbers: dict[str, int]
    file_handles: dict[str, outputs.OutputTarget.Handle]
    target_column_maps: dict[str, CaseInsensitiveDict[str, int]]
    metrics: tools.metrics.Metrics
    inputs: sources.SourceObject
    # OMOP table -> source key -> generated record id (see `record_key`)
    record_keys: dict[str, dict[str, str]] = field(default_factory=dict)
    # OMOP table -> records written for the current source row, as
    # (record id, concept id), for linking derived records (see `derived_from`)
    row_records: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    # Rows written to tables not built from rules (fact_relationship)
    link_counts: dict[str, int] = field(default_factory=dict)

    @property
    def input_files(self) -> list[str]:
        return self.mappingrules.get_all_infile_names()

    @property
    def output_files(self) -> list[str]:
        return self.mappingrules.get_all_outfile_names()


@dataclass
class RecordResult:
    """Result of record building operation"""

    success: bool
    record_count: int
    metrics: tools.metrics.Metrics


@dataclass
class RecordContext:
    """Context object containing all the data needed for record building"""

    tgtfilename: str
    tgtcolmap: CaseInsensitiveDict[str, int]
    v2_mapping: V2TableMapping
    srcfield: str
    srcdata: list[str]
    srccolmap: CaseInsensitiveDict[str, int]
    srcfilename: str
    omopcdm: OmopCDM
    metrics: tools.metrics.Metrics
    person_lookup: dict[str, str]
    record_numbers: dict[str, int]
    file_handles: Mapping[str, TextIO | outputs.OutputTarget.Handle]
    auto_num_col: str | None
    person_id_col: str | None
    date_col_data: dict[str, str]
    date_component_data: dict[str, dict[str, str]]
    notnull_numeric_fields: list[str]
    # OMOP table -> source key -> generated record id (see `record_key`)
    record_keys: dict[str, dict[str, str]] = field(default_factory=dict)
    # OMOP table -> records written for the current source row, as
    # (record id, concept id), for linking derived records (see `derived_from`)
    row_records: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    # Rows written to tables not built from rules (fact_relationship)
    link_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class ProcessingResult:
    """Result of data processing operation"""

    output_counts: dict[str, int]
    rejected_id_counts: dict[str, int]
    success: bool = True
    error_message: str | None = None


@dataclass
class DBConnParams:
    """Parameters for connecting to an engine"""

    db_type: str
    username: str
    password: str
    host: str
    port: int
    db_name: str
    schema: str
