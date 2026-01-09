from dataclasses import dataclass
from typing import Mapping, TextIO

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
    target_column_maps: dict[str, dict[str, int]]
    metrics: tools.metrics.Metrics
    inputs: sources.SourceObject

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
    tgtcolmap: dict[str, int]
    v2_mapping: V2TableMapping
    srcfield: str
    srcdata: list[str]
    srccolmap: dict[str, int]
    srcfilename: str
    omopcdm: OmopCDM
    metrics: tools.metrics.Metrics
    person_lookup: dict[str, str]
    record_numbers: dict[str, int]
    file_handles: Mapping[str, TextIO | outputs.OutputTarget.Handle]
    auto_num_col: str | None
    person_id_col: str
    date_col_data: dict[str, str]
    date_component_data: dict[str, dict[str, str]]
    notnull_numeric_fields: list[str]


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
