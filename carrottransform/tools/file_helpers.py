import importlib.resources as resources
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, TextIO, Tuple, cast

from carrottransform.tools.omopcdm import OmopCDM

logger = logging.getLogger(__name__)


# Function inherited from the "old" CaRROT-CDM (modfied to exit on error)


def load_json(f_in: Path):
    try:
        data = json.load(f_in.open())
    except Exception:
        logger.exception("{0} not found. Or cannot parse as json".format(f_in))
        sys.exit()

    return data


def check_dir_isvalid(directory: Path, create_if_missing: bool = False) -> None:
    """Check if directory is valid, optionally create it if missing.

    Args:
        directory: Directory path as string or tuple
        create_if_missing: If True, create directory if it doesn't exist
    """
    ## if not a directory, create it if requested (including parents. This option is for the output directory only).
    if not directory.is_dir():
        if create_if_missing:
            try:
                ## deliberately not using the exist_ok option, as we want to know whether it was created or not to provide different logger messages.
                directory.mkdir(parents=True)
                logger.info(f"Created directory: {directory}")
            except OSError as e:
                logger.warning(f"Failed to create directory {directory}: {e}")
                sys.exit(1)
        else:
            logger.warning(f"Not a directory, dir {directory}")
            sys.exit(1)


def check_files_in_rules_exist(
    rules_input_files: list[str], existing_input_files: list[str]
) -> None:
    for infile in existing_input_files:
        if infile not in rules_input_files:
            msg = (
                "WARNING: no mapping rules found for existing input file - {0}".format(
                    infile
                )
            )
            logger.warning(msg)
    for infile in rules_input_files:
        if infile not in existing_input_files:
            msg = "WARNING: no data for mapped input file - {0}".format(infile)
            logger.warning(msg)


class OutputFileManager:
    """Manages output file creation and cleanup"""

    def __init__(self, output_dir: Path, omopcdm: OmopCDM):
        self.output_dir = output_dir
        self.omopcdm = omopcdm
        self.file_handles: Dict[str, TextIO] = {}

    def setup_output_files(
        self, output_files: List[str], write_mode: str
    ) -> Tuple[Dict[str, TextIO], Dict[str, Dict[str, int]]]:
        """Setup output files and return file handles and column maps"""
        target_column_maps = {}

        for target_file in output_files:
            file_path = (self.output_dir / target_file).with_suffix(".tsv")
            self.file_handles[target_file] = cast(
                TextIO, file_path.open(mode=write_mode, encoding="utf-8")
            )
            if write_mode == "w":
                output_header = self.omopcdm.get_omop_column_list(target_file)
                self.file_handles[target_file].write("\t".join(output_header) + "\n")

            target_column_maps[target_file] = self.omopcdm.get_omop_column_map(
                target_file
            )

        return self.file_handles, target_column_maps

    def close_all_files(self):
        """Close all open file handles"""
        for fh in self.file_handles.values():
            fh.close()
        self.file_handles.clear()
