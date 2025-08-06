import csv
import json
import logging
import sys
import importlib.resources as resources
from typing import IO, Iterator, List, Optional, Dict, TextIO, Tuple, cast
from pathlib import Path
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


def resolve_paths(args: List[Optional[Path]]) -> List[Optional[Path]]:
    """Resolve special path syntaxes in command line arguments."""
    try:
        # Fix for Traversable parent issue - convert to Path first
        package_files = resources.files("carrottransform")
        package_path = Path(str(package_files)).resolve()
    except Exception:
        # Fallback for development environment
        import carrottransform

        package_path = Path(carrottransform.__file__).resolve().parent

    # Handle None values and replace @carrot with the actual package path
    prefix = "@carrot"
    return [
        (
            package_path
            / Path(str(arg).replace(prefix, "").replace("\\", "/").lstrip("/"))
            if arg is not None and str(arg).startswith(prefix)
            else arg
        )
        for arg in args
    ]


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


def open_file(file_path: Path) -> tuple[IO[str], Iterator[list[str]]] | None:
    """opens a file and does something related to CSVs"""
    try:
        fh = file_path.open(mode="r", encoding="utf-8-sig")
        csvr = csv.reader(fh)
        return fh, csvr
    except IOError as e:
        logger.exception("Unable to open: {0}".format(file_path))
        logger.exception("I/O error({0}): {1}".format(e.errno, e.strerror))
        return None


def set_omop_filenames(
    omop_ddl_file: Optional[Path],
    omop_config_file: Optional[Path],
    omop_version: Optional[str],
) -> tuple[Optional[Path], Optional[Path]]:
    """
    Set default OMOP file paths when not explicitly provided.

    This function provides a convenience mechanism where users can specify just
    an OMOP version instead of providing full paths to both DDL and config files.

    Args:
        omop_ddl_file: Path to OMOP DDL file (optional)
        omop_config_file: Path to OMOP config file (optional)
        omop_version: OMOP version string (e.g., "5.3", "5.4")

    Returns:
        Tuple of (config_file_path, ddl_file_path) - either provided or defaults

    Example:
        # User provides version but no files - defaults will be used
        config, ddl = set_omop_filenames(None, None, "5.3")

        # User provides custom files - they will be returned unchanged
        config, ddl = set_omop_filenames(custom_ddl, custom_config, "5.3")
    """
    # Only set defaults if BOTH files are None AND version is provided
    if omop_ddl_file is None and omop_config_file is None and omop_version is not None:
        logger.info(f"Using default OMOP files for version {omop_version}")

        # Set default config file - convert Traversable to Path
        config_traversable = resources.files("carrottransform") / "config" / "omop.json"
        omop_config_file = Path(str(config_traversable))

        # Set version-specific DDL file - convert Traversable to Path
        omop_ddl_file_name = f"OMOPCDM_postgresql_{omop_version}_ddl.sql"
        ddl_traversable = (
            resources.files("carrottransform") / "config" / omop_ddl_file_name
        )
        omop_ddl_file = Path(str(ddl_traversable))

        # Validate that the default files exist (now safe since they're Path objects)
        if not omop_config_file.is_file():
            logger.warning(f"Default config file not found: {omop_config_file}")
        if not omop_ddl_file.is_file():
            logger.warning(f"Default DDL file not found: {omop_ddl_file}")

    return omop_config_file, omop_ddl_file


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
