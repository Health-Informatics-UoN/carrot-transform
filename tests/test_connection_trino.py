import logging
import random
from pathlib import Path

import pytest

import tests.testools as testools
from carrottransform.tools import outputs, sources

#
logger = logging.getLogger(__name__)


@pytest.mark.docker
def test_trino_updown(trino):
    logger.info("trino is working")


@pytest.mark.docker
def test_targetWriter_trino(trino, tmp_path: Path):
    test_data = Path(__file__).parent / "test_data/measure_weight_height"

    # connect to Trino
    outputTarget = outputs.sql_output_target(trino.connection)

    source: sources.SourceObject = sources.csv_source_object(test_data, ",")

    # open the three outputs - we're mirrorng the way ct does it
    targets = []
    for table in ["heights", "persons", "weights"]:
        iterator = source.open(table)
        header = next(iterator)
        targets.append((outputTarget.start(table, header), iterator))

    # randomly move records
    # ... it should randomly use different ones to mirror how ct does it
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

    ####
    ### assert
    testools.compare_two_sources(
        sources.csv_source_object(test_data, ","),
        sources.sql_source_object(trino.connection),
        ["heights", "persons", "weights"],
    )
