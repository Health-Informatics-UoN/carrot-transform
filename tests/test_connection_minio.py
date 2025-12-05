import logging
import random
from pathlib import Path
from tests import conftest
import pytest

import tests.testools as testools
from carrottransform.tools import outputs, sources

#
logger = logging.getLogger(__name__)


@pytest.mark.docker
def test_minio_updown(minio):
    logger.info(f"minio is working {minio}")


@pytest.mark.unit
def test_protocol_parser():
    text = "minio:minio_user_c91f6832f2d525dd:minio_pass_ea1b5ab58c2bc631@http://127.0.0.1:58384/test-bucket-bae51f90a75dddab"

    obj = outputs.MinioURL(text)

    assert obj._user == "minio_user_c91f6832f2d525dd"
    assert obj._pass == "minio_pass_ea1b5ab58c2bc631"
    assert obj._protocol == "http"
    assert obj._host == "127.0.0.1"
    assert obj._port == "58384"
    assert obj._bucket == "test-bucket-bae51f90a75dddab"
    assert obj._folder == ""  # (empty in this case)


@pytest.mark.docker
def test_targetWriter_minio(minio, tmp_path: Path):
    heights = Path(__file__).parent / "test_data/measure_weight_height/heights.csv"

    # connect to Minio
    outputTarget = outputs.minio_output_target(minio.connection)

    # grab some csvs
    source: sources.SourceObject = sources.csv_source_object(
        Path(__file__).parent / "test_data/measure_weight_height/", ","
    )

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
        sources.csv_source_object(heights.parent, ","),
        sources.minio_source_object(minio.connection, "\t"),
        ["heights", "persons", "weights"],
    )


@pytest.mark.docker
def test_connection_parsing(minio: conftest.MinIOBucket):
    """quick check of the connection parsing thing"""

    conn = minio.connection

    check = outputs.MinioURL(conn)

    assert minio.config.username == check._user
    assert minio.config.password == check._pass
    assert "http" == check._protocol
    assert minio.config.docker_ip == check._host
    assert minio.config.server_port == check._port
    assert minio.name == check._bucket
    assert "" == check._folder

    assert not conn.endswith("/")

    folder = f"some{testools.rand_hex()}folder/"
    conn += f"/{folder}"

    conn = conn.replace("http://", "https://")

    check = outputs.MinioURL(conn)

    assert minio.config.username == check._user
    assert minio.config.password == check._pass
    assert "https" == check._protocol
    assert minio.config.docker_ip == check._host
    assert minio.config.server_port == check._port
    assert minio.name == check._bucket
    assert folder == check._folder
