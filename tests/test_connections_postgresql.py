import io
import logging
import os
import random
import shutil
import string
import subprocess
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path

import boto3
import docker
import pytest
import sqlalchemy
from click.testing import CliRunner
from sqlalchemy import Column, MetaData, Table, Text, create_engine, insert
from sqlalchemy.orm import sessionmaker

import tests.csvrow as csvrow
import tests.testools as testools
from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs, sources
from tests.testools import postgres, package_root


@pytest.mark.docker
def test_targetWriter(postgres, tmp_path: Path):
    heights = Path(__file__).parent / "test_data/measure_weight_height/heights.csv"
    persons = Path(__file__).parent / "test_data/measure_weight_height/persons.csv"
    weights = Path(__file__).parent / "test_data/measure_weight_height/weights.csv"

    # connect to a database
    # Create engine and connection
    engine = create_engine(postgres.config.connection)

    # create the target
    outputTarget = outputs.sql_output_target(engine)

    source: sources.SourceObject = sources.csv_source_object(
        Path(__file__).parent / "test_data/measure_weight_height/", ","
    )

    # open the three outputs
    targets = []
    for table in ["heights", "persons", "weights"]:
        iterator = source.open(table)
        header = next(iterator)
        targets.append((outputTarget.start(table, header), iterator))

    # randomly move records
    # i want to be sure that multiple read/write things can be active at once
    while 0 != len(targets):
        # select a random index
        import random

        index = random.randint(0, len(targets) - 1)

        # select a rangom one to move
        (target, iterator) = targets[index]

        # get a record to move, or, remove this target if it's already been finished
        try:
            record = next(iterator)
        except StopIteration:
            targets.pop(index)
            target.close()
            continue

        # move the record
        target.write(record)

    # create a source
    source = sources.sql_source_object(engine)

    # re-read and verify
    for table in ["heights", "persons", "weights"]:
        # read what was inserted
        actual = ""
        for line in source.open(table):
            actual += ",".join(line) + "\n"
        actual = actual.strip()

        # read the raw expected
        with open(heights.parent / f"{table}.csv", "r") as file:
            expected = file.read().strip()

        # compare the two values
        assert expected == actual, f"mismatch in {table}"
