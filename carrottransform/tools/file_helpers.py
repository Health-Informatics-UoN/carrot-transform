import csv
import json
import logging
import sys
import json
import importlib.resources as resources
from typing import IO, Iterator, List, Optional
from pathlib import Path
import importlib.resources

logger = logging.getLogger(__name__)


# Function inherited from the "old" CaRROT-CDM (modfied to exit on error)


def load_json(f_in: Path):
    try:
        data = json.load(f_in.open())
    except Exception as err:
        logger.exception("{0} not found. Or cannot parse as json".format(f_in))
        sys.exit()

    return data


def resolve_paths(args: List[Optional[Path]]) -> List[Optional[Path]]:
    """Resolve special path syntaxes in command line arguments."""
    try:
        package_path = (resources.files("carrottransform") / "__init__.py").parent
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

    ## check directory has been set
    if directory is None:
        logger.warning("Directory not provided.")
        sys.exit(1)

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


#  TODO: understand this function
def set_omop_filenames(
    omop_ddl_file: Path, omop_config_file: Path, omop_version: str
) -> tuple[Path, Path]:
    if (
        (omop_ddl_file is None)
        and (omop_config_file is None)
        and (omop_version is not None)
    ):
        omop_config_file = (
            importlib.resources.files("carrottransform") / "config/omop.json"
        )
        omop_ddl_file_name = "OMOPCDM_postgresql_" + omop_version + "_ddl.sql"
        omop_ddl_file = (
            importlib.resources.files("carrottransform") / "config" / omop_ddl_file_name
        )
    return omop_config_file, omop_ddl_file
