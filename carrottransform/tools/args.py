"""
functions to handle args
"""

import carrottransform
import carrottransform.tools as tools
import click
import csv
import datetime
import fnmatch
import importlib.resources
import json
import logging
import os
import sys
import time

import logging
from pathlib import Path
import json

from carrottransform.tools.click import PathArgs
from carrottransform.tools.omopcdm import OmopCDM

from typing import Iterator, IO, List, Optional, Iterable
from importlib import resources
from pathlib import Path

logger = logging.getLogger(__name__)


def auto_person_in_rules(rules: Path) -> Path:
    """scan a rules file to see where it's getting its `PersonID` from"""

    # for better error reporting, record all the sourcetables
    source_tables = []
    # mark we haven't found one
    source_table = ""

    # grab the data
    data = json.load(rules.open())
    assert data is not None
    data = data["cdm"]
    assert data is not None
    data = data["person"]
    assert data is not None

    # go through each thing in it
    for g in data.keys():
        assert data[g] is not None
        assert data[g]["person_id"] is not None
        assert data[g]["person_id"]["source_field"] is not None
        assert data[g]["person_id"]["source_table"] is not None

        # grab the source table - check that the person_id is "good"
        assert "PersonID" == data[g]["person_id"]["source_field"]
        g_source_table = data[g]["person_id"]["source_table"]

        # check that we didn't get an unsable one
        assert g_source_table is not None
        assert "" != g_source_table

        # record the source table
        if g_source_table not in source_tables:
            source_tables.append(g_source_table)

    # determine what to do witht he source tabels we found
    if 1 != len(source_tables):
        message = f"couldn't determine --person-file automatically (found {len(source_tables)} suitable names)"
        for t in source_tables:
            message += "\n    >" + t + "<"
        raise Exception(message)
    else:
        return rules.parent / source_tables[0]
