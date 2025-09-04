from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, TextIO

from sqlalchemy.engine import Connection

import carrottransform.tools as tools
from carrottransform.tools.mapping_types import V2TableMapping
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM


@dataclass
class ProcessingContext:
    """Context object containing all processing configuration and state"""

    mappingrules: MappingRules
    omopcdm: OmopCDM
    person_lookup: Dict[str, str]
    record_numbers: Dict[str, int]
    file_handles: Dict[str, TextIO]
    target_column_maps: Dict[str, Dict[str, int]]
    metrics: tools.metrics.Metrics
    input_dir: Optional[Path] = None
    db_connection: Optional[Connection] = None
    schema: Optional[str] = None

    @property
    def input_files(self) -> List[str]:
        return self.mappingrules.get_all_infile_names()

    @property
    def output_files(self) -> List[str]:
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
    tgtcolmap: Dict[str, int]
    v2_mapping: V2TableMapping
    srcfield: str
    srcdata: List[str]
    srccolmap: Dict[str, int]
    srcfilename: str
    omopcdm: OmopCDM
    metrics: tools.metrics.Metrics
    person_lookup: Dict[str, str]
    record_numbers: Dict[str, int]
    file_handles: Dict[str, TextIO]
    auto_num_col: Optional[str]
    person_id_col: str
    date_col_data: Dict[str, str]
    date_component_data: Dict[str, Dict[str, str]]
    notnull_numeric_fields: List[str]


@dataclass
class ProcessingResult:
    """Result of data processing operation"""

    output_counts: Dict[str, int]
    rejected_id_counts: Dict[str, int]
    success: bool = True
    error_message: Optional[str] = None


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
