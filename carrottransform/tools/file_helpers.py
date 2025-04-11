import json
import logging
import os
import sys
import json
import importlib.resources as resources
from typing import List, Optional
from pathlib import Path

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
        with resources.files('carrottransform').joinpath('__init__.py') as f:
            package_path = f.parent
    except Exception:
        # Fallback for development environment
        import carrottransform
        package_path = Path(carrottransform.__file__).resolve().parent
    
    # Handle None values and replace @carrot with the actual package path
    prefix = '@carrot'
    return [
        package_path / Path(str(arg).replace(prefix, '').lstrip('/')) if arg is not None and str(arg).startswith(prefix) else arg 
        for arg in args
    ]
