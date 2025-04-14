
import json
import logging
import os
import sys

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
