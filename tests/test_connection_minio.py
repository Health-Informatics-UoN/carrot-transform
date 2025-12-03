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
# from tests.testools import minio

#
logger = logging.getLogger(__name__)


@pytest.mark.docker
def test_minio_updown(minio_bucket):
    logger.info(f"minio is working {minio_bucket}")


# @pytest.mark.docker
# def test_targetWriter_minio(minio, tmp_path: Path):
#     heights = Path(__file__).parent / "test_data/measure_weight_height/heights.csv"
#     persons = Path(__file__).parent / "test_data/measure_weight_height/persons.csv"
#     weights = Path(__file__).parent / "test_data/measure_weight_height/weights.csv"

#     # connect to Trino
#     outputTarget = outputs.sql_output_target(trino.config.connection)

#     source: sources.SourceObject = sources.csv_source_object(
#         Path(__file__).parent / "test_data/measure_weight_height/", ","
#     )

#     # open the three outputs - we're mirrorng the way ct does it
#     targets = []
#     for table in ["heights", "persons", "weights"]:
#         iterator = source.open(table)
#         header = next(iterator)
#         targets.append((outputTarget.start(table, header), iterator))

#     # randomly move records
#     # ... it should randomly use different ones to mirror how ct does it
#     while 0 != len(targets):
#         index = random.randint(0, len(targets) - 1)
#         (target, iterator) = targets[index]

#         try:
#             record = next(iterator)
#         except StopIteration:
#             targets.pop(index)
#             target.close()
#             continue

#         target.write(record)

#     ####
#     ### assert
#     testools.compare_two_sources(
#         sources.csv_source_object(heights.parent, ","),
#         sources.sql_source_object(trino.config.connection),
#         ["heights", "persons", "weights"],
#     )
