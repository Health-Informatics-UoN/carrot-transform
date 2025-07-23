import csv
from pathlib import Path
from typing import Dict, Tuple, Any, Optional, TextIO, List, cast

import carrottransform.tools as tools
from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.logger import logger_setup
from carrottransform.tools.person_helpers import (
    load_person_ids,
    set_saved_person_id_file,
)
from carrottransform.tools.date_helpers import normalise_to8601
from carrottransform.tools.types import (
    ProcessingResult,
    ProcessingContext
)
from carrottransform.tools.row_processor import RowProcessor

logger = logger_setup()

class InputFileProcessor:
    """Handles processing of individual input files"""
    
    def __init__(self, context: ProcessingContext):
        self.context = context
        
    def process_file(self, source_filename: str) -> Tuple[Dict[str, int], int]:
        """
        Process a single input file
        
        Returns:
            Tuple of (output_counts, rejected_id_count)
        """
        logger.info(f"Processing input file: {source_filename}")
        
        file_path = self.context.input_dir / source_filename
        if not file_path.exists():
            logger.warning(f"Input file not found: {source_filename}")
            return {}, 0
        
        output_counts = {outfile: 0 for outfile in self.context.output_files}
        rejected_id_count = 0
        
        try:
            with file_path.open(mode="r", encoding="utf-8-sig") as fh:
                csv_reader = csv.reader(fh)
                csv_column_headers = next(csv_reader)
                input_column_map = self.context.omopcdm.get_column_map(csv_column_headers)
                
                # Get file metadata
                file_metadata = self._get_file_metadata(source_filename, input_column_map)
                if not file_metadata:
                    return output_counts, rejected_id_count
                
                # Process each row
                for input_data in csv_reader:
                    row_result = self._process_row(
                        source_filename, input_data, input_column_map, file_metadata
                    )
                    
                    # Update counters
                    for outfile, count in row_result.output_counts.items():
                        output_counts[outfile] += count
                    rejected_id_count += row_result.rejected_id_counts.get(source_filename, 0)
                    
        except Exception as e:
            logger.error(f"Error processing file {source_filename}: {str(e)}")
            
        return output_counts, rejected_id_count
    
    def _get_file_metadata(self, source_filename: str, input_column_map: Dict[str, int]) -> Optional[Dict[str, Any]]:
        """Get metadata needed for processing this file"""
        datetime_source, person_id_source = self.context.mappingrules.get_infile_date_person_id(source_filename)
        
        if not datetime_source or not person_id_source:
            logger.warning(f"Missing date or person ID mapping for {source_filename}")
            return None
        
        if datetime_source not in input_column_map:
            logger.warning(f"Date field {datetime_source} not found in {source_filename}")
            return None
        
        return {
            'datetime_col': input_column_map[datetime_source],
            'data_fields': self.context.mappingrules.get_infile_data_fields(source_filename)
        }
    
    def _process_row(
        self, 
        source_filename: str, 
        input_data: List[str], 
        input_column_map: Dict[str, int], 
        file_metadata: Dict[str, Any]
    ) -> ProcessingResult:
        """Process a single row of data"""
        # Increment input count
        self.context.metrics.increment_key_count(
            source=source_filename,
            fieldname="all",
            tablename="all",
            concept_id="all",
            additional="",
            count_type="input_count",
        )
        
        # Normalize date
        fulldate = normalise_to8601(input_data[file_metadata['datetime_col']])
        if fulldate is None:
            self.context.metrics.increment_key_count(
                source=source_filename,
                fieldname="all",
                tablename="all",
                concept_id="all",
                additional="",
                count_type="input_date_fields",
            )
            return ProcessingResult({}, {})
        
        input_data[file_metadata['datetime_col']] = fulldate
        
        # Process each target table
        row_processor = RowProcessor(self.context)
        return row_processor.process_row_for_all_targets(
            source_filename, input_data, input_column_map, file_metadata['data_fields']
        )


class V2DataProcessor:
    """Main processor for v2 format data"""
    
    def __init__(self, context: ProcessingContext):
        self.context = context
    
    def process_all_data(self) -> ProcessingResult:
        """
        Process all data using v2 format rules
        
        Returns:
            ProcessingResult with output counts and rejected ID counts
        """
        logger.info("Processing data using v2 format...")
        
        # Initialize counters
        total_output_counts = {outfile: 0 for outfile in self.context.output_files}
        total_rejected_counts = {infile: 0 for infile in self.context.input_files}
        
        # Process each input file
        file_processor = InputFileProcessor(self.context)
        
        for source_filename in self.context.input_files:
            try:
                output_counts, rejected_count = file_processor.process_file(source_filename)
                
                # Update totals
                for outfile, count in output_counts.items():
                    total_output_counts[outfile] += count
                total_rejected_counts[source_filename] += rejected_count
                
            except Exception as e:
                logger.error(f"Error processing file {source_filename}: {str(e)}")
                return ProcessingResult(
                    total_output_counts, 
                    total_rejected_counts, 
                    success=False, 
                    error_message=str(e)
                )
        
        return ProcessingResult(total_output_counts, total_rejected_counts)


class OutputFileManager:
    """Manages output file creation and cleanup"""
    
    def __init__(self, output_dir: Path, omopcdm: OmopCDM):
        self.output_dir = output_dir
        self.omopcdm = omopcdm
        self.file_handles: Dict[str, TextIO] = {}
    
    def setup_output_files(self, output_files: List[str], write_mode: str) -> Tuple[Dict[str, TextIO], Dict[str, Dict[str, int]]]:
        """Setup output files and return file handles and column maps"""
        target_column_maps = {}
        
        for target_file in output_files:
            file_path = (self.output_dir / target_file).with_suffix(".tsv")
            self.file_handles[target_file] = cast(TextIO, file_path.open(mode=write_mode, encoding="utf-8"))
            if write_mode == "w":
                output_header = self.omopcdm.get_omop_column_list(target_file)
                self.file_handles[target_file].write("\t".join(output_header) + "\n")
            
            target_column_maps[target_file] = self.omopcdm.get_omop_column_map(target_file)
        
        return self.file_handles, target_column_maps
    
    def close_all_files(self):
        """Close all open file handles"""
        for fh in self.file_handles.values():
            fh.close()
        self.file_handles.clear()


class V2ProcessingOrchestrator:
    """Main orchestrator for the entire V2 processing pipeline"""
    
    def __init__(
        self,
        rules_file: Path,
        output_dir: Path,
        input_dir: Path,
        person_file: Path,
        omop_ddl_file: Optional[Path],
        omop_config_file: Optional[Path],
        write_mode: str = "w"
    ):
        self.rules_file = rules_file
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.person_file = person_file
        self.omop_ddl_file = omop_ddl_file
        self.omop_config_file = omop_config_file
        self.write_mode = write_mode
        
        # Initialize components immediately
        self.initialize_components()
    
    def initialize_components(self):
        """Initialize all processing components"""
        self.omopcdm = OmopCDM(self.omop_ddl_file, self.omop_config_file)
        self.mappingrules = MappingRules(self.rules_file, self.omopcdm)
        
        if not self.mappingrules.is_v2_format:
            raise ValueError("Rules file is not in v2 format!")
        
        self.metrics = tools.metrics.Metrics(self.mappingrules.get_dataset_name())
        self.output_manager = OutputFileManager(self.output_dir, self.omopcdm)
    
    def setup_person_lookup(self) -> Tuple[Dict[str, str], int]:
        """Setup person ID lookup and save mapping"""
        saved_person_id_file = set_saved_person_id_file(None, self.output_dir)

        person_lookup, rejected_person_count = load_person_ids(
            saved_person_id_file,
            self.person_file,
            self.mappingrules,
            use_input_person_ids="N",
        )
        
        # Save person IDs
        with saved_person_id_file.open(mode="w") as fhpout:
            fhpout.write("SOURCE_SUBJECT\tTARGET_SUBJECT\n")
            for person_id, person_assigned_id in person_lookup.items():
                fhpout.write(f"{str(person_id)}\t{str(person_assigned_id)}\n")
        
        return person_lookup, rejected_person_count
    
    def execute_processing(self) -> ProcessingResult:
        """Execute the complete processing pipeline"""
        
        try:
            # Setup person lookup
            person_lookup, rejected_person_count = self.setup_person_lookup()
            
            # Setup output files
            output_files = self.mappingrules.get_all_outfile_names()
            file_handles, target_column_maps = self.output_manager.setup_output_files(output_files, self.write_mode)
            
            # Create processing context
            context = ProcessingContext(
                mappingrules=self.mappingrules,
                omopcdm=self.omopcdm,
                input_dir=self.input_dir,
                person_lookup=person_lookup,
                record_numbers={output_file: 1 for output_file in output_files},
                file_handles=file_handles,
                target_column_maps=target_column_maps,
                metrics=self.metrics
            )
            
            # Process data
            processor = V2DataProcessor(context)
            result = processor.process_all_data()
            
            # Log results
            logger.info(f"person_id stats: total loaded {len(person_lookup)}, reject count {rejected_person_count}")
            for target_file, count in result.output_counts.items():
                logger.info(f"TARGET: {target_file}: output count {count}")
            
            # Write summary
            data_summary = self.metrics.get_mapstream_summary()
            with (self.output_dir / "summary_mapstream.tsv").open(mode="w") as dsfh:
                dsfh.write(data_summary)
            
            return result
            
        finally:
            # Always close files
            if self.output_manager:
                self.output_manager.close_all_files()
