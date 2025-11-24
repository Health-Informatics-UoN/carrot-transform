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
from sqlalchemy import Column, MetaData, Table, Text, create_engine, insert, text
from sqlalchemy.orm import sessionmaker

import tests.csvrow as csvrow
import tests.testools as testools
from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs, sources
from tests.click_tools import package_root
from tests.testools import trino

#
logger = logging.getLogger(__name__)


@pytest.mark.docker
def test_trino_updown(trino):
    logger.info("trino is working")


@pytest.mark.docker
def test_ttt():
    heights = Path(__file__).parent / "test_data/measure_weight_height/heights.csv"
    persons = Path(__file__).parent / "test_data/measure_weight_height/persons.csv"
    weights = Path(__file__).parent / "test_data/measure_weight_height/weights.csv"
    "C:/Users/peter/Desktop/test_out"

    source: sources.SourceObject = sources.csv_source_object(
        Path("C:/Users/peter/Desktop/test_out"), "\t"
    )

    csv = sources.csv_source_object(heights.parent,',')
    testools.compare_to_tsvs(Path("C:/Users/peter/Desktop/test_out"), csv, items= ["heights", "persons", "weights"])

@pytest.mark.docker
def test_targetWriter_trino(trino, tmp_path: Path):
    heights = Path(__file__).parent / "test_data/measure_weight_height/heights.csv"
    persons = Path(__file__).parent / "test_data/measure_weight_height/persons.csv"
    weights = Path(__file__).parent / "test_data/measure_weight_height/weights.csv"

    # connect to Trino
    outputTarget = outputs.sql_output_target(trino.config.connection)

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
    while 0 != len(targets):
        index = random.randint(0, len(targets) - 1)
        (target, iterator) = targets[index]

        try:
            record = next(iterator)
        except StopIteration:
            targets.pop(index)
            target.close()
            continue

        target.write(record)

    # 

    # create a source
    # copy stuff back out to the test filder
    source = sources.sql_source_object(trino.config.connection)
    logger.info(
        f"temp test data copy out to {(tmp_path / "test_out")}"
    )
    (tmp_path / "test_out").mkdir(parents=True)
    test_copy = outputs.csv_output_target(tmp_path / "test_out")
    
    testools.copy_across(ot=test_copy, so=source, names=["heights", "persons", "weights"])


    raise Exception('?sss')

    testools.compare_to_tsvs('measure_weight_height', source, items= ["heights", "persons", "weights"])

    raise Exception('?sss')

    # re-read and verify
    for table in ["heights", "persons", "weights"]:
        actual = ""
        for line in source.open(table):
            actual += ",".join(line) + "\n"
        actual = actual.strip()

        with open(heights.parent / f"{table}.csv", "r") as file:
            expected = file.read().strip()

        assert expected == actual, f"mismatch in {table}"