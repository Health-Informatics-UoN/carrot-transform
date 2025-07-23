from typing import Dict, List, Optional
from dataclasses import dataclass
import carrottransform.tools as tools
from carrottransform.tools.omopcdm import OmopCDM

@dataclass
class RecordResult:
    """Result of record building operation"""
    build_records: bool
    records: List[List[str]]
    metrics: tools.metrics.Metrics 

@dataclass
class PersonIdMapping:
    source_field: str
    dest_field: str


@dataclass
class DateMapping:
    source_field: str
    dest_fields: List[str]


@dataclass
class ConceptMapping:
    source_field: str
    value_mappings: Dict[
        str, Dict[str, List[int]]
    ]  # value -> dest_field -> concept_ids
    original_value_fields: List[str]


@dataclass
class V2TableMapping:
    source_table: str
    person_id_mapping: Optional[PersonIdMapping]
    date_mapping: Optional[DateMapping]
    concept_mappings: Dict[str, ConceptMapping]  # source_field -> ConceptMapping

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

@dataclass
class ProcessingResult:
    """Result of data processing operation"""
    output_counts: Dict[str, int]
    rejected_id_counts: Dict[str, int]
    success: bool = True
    error_message: Optional[str] = None