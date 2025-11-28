import logging
import random
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, Iterable

import docker
import pytest
import requests
import sqlalchemy
from botocore import client
from click.testing import CliRunner
from sqlalchemy import create_engine, text

import carrottransform.tools.sources as sources
from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs
from tests import testools

#
logger = logging.getLogger(__name__)

STARTUP_TIMEOUT = 60
STARTUP_SLEEP = 0.2

#### ==========================================================================
## Trino two-part fixture


@dataclass
class TrinoInstance:
    """the data for a running docker container"""

    docker_name: str = f"trino_test_docker_{testools.rand_hex()}"
    coordinator_port: int = random.randrange(9000, 9100)
    server_port: int = random.randrange(8080, 8180)

    trino_user: str = f"trino_user_{testools.rand_hex()}"
    catalog: str = "memory"

    @property
    def connection(self) -> str:
        return f"trino://{self.trino_user}@localhost:{self.server_port}/{self.catalog}"


@pytest.fixture(scope="session")
def trino_instance(docker_ip, tmp_path_factory) -> Iterable[TrinoInstance]:
    """Start a Trino container"""

    tmp_path = tmp_path_factory.mktemp("trino_instance")

    config = TrinoInstance()

    # Create a Trino configuration
    config_dir = tmp_path / "trino" / f"trino_config_{testools.rand_hex()}"
    config_dir.mkdir(parents=True, exist_ok=False)

    # 1. JVM Configuration
    (config_dir / "jvm.config").write_text(
        textwrap.dedent("""
        -server
    """).strip()
    )

    # Create node.properties
    (config_dir / "node.properties").write_text(
        textwrap.dedent(f"""
        node.environment=test
        node.id=test-node
        node.data-dir=/var/trino/data
    """)
    )

    # Create config.properties
    (config_dir / "config.properties").write_text(
        textwrap.dedent(f"""
        coordinator=true
        node-scheduler.include-coordinator=true
        http-server.http.port={config.server_port}
        query.max-memory=1GB
        query.max-memory-per-node=512MB
        discovery.uri=http://localhost:{config.server_port}
    """)
    )

    # Create catalog properties for memory connector
    catalog_dir = config_dir / "catalog"
    catalog_dir.mkdir(exist_ok=True)
    (catalog_dir / "memory.properties").write_text("connector.name=memory")

    client = docker.from_env()
    client.images.pull("trinodb/trino:latest")
    container = client.containers.run(
        "trinodb/trino:latest",
        name=config.docker_name,
        ports={f"{config.server_port}/tcp": ("127.0.0.1", config.server_port)},
        volumes={
            str(config_dir): {"bind": "/etc/trino", "mode": "rw"},
            str(tmp_path / "var"): {"bind": "/var/trino", "mode": "rw"},
        },
        detach=True,
        remove=True,
    )

    # start the container
    start_time = time.time()
    first_start_time = start_time
    while time.time() - start_time < STARTUP_TIMEOUT:
        try:
            import requests

            response = requests.get(f"http://localhost:{config.server_port}/v1/info")
            if response.status_code == 200:
                break
        except:
            time.sleep(STARTUP_SLEEP)
    else:
        # don't bother stopping the contianer if it didn't start
        logs = container.logs().decode("utf-8")
        logger.error(
            f"Trino container failed to start within {STARTUP_TIMEOUT} seconds"
        )
        logger.error(f"Full container logs:\n{logs}")
        raise Exception("Trino container failed to start")

    # wait for the server itself to start
    start_time = time.time()
    while time.time() - start_time < STARTUP_TIMEOUT:
        import requests

        response = requests.get(f"http://localhost:{config.server_port}/v1/info")
        if response.status_code != 200:
            raise Exception("trino container stopped working during startup")

        starting = response.json()["starting"]
        if not starting:
            break
    else:
        # don't bother stopping the contianer if it didn't start
        logs = container.logs().decode("utf-8")
        logger.error(
            f"Trino server (inside container) failed to start within {STARTUP_TIMEOUT} seconds"
        )
        logger.error(f"Full container logs:\n{logs}")
        raise Exception(
            f"Trino server failed to start within {STARTUP_TIMEOUT} seconds"
        )

    # we're ready now
    full_start_time = time.time() - first_start_time
    logger.info(f"Trino container is ready after {full_start_time}")

    yield config

    # cleanup
    container.stop()


@dataclass
class TrinoSchema:
    """the data for a schema within a running docker container"""

    instance: TrinoInstance
    schema: str = f"schema_{testools.rand_hex()}"

    @property
    def connection(self) -> str:
        return f"{self.instance.connection}/{self.schema}"


@pytest.fixture(scope="function")
def trino(trino_instance) -> Iterable[TrinoSchema]:
    """Start a Trino schema"""

    config = TrinoSchema(instance=trino_instance)

    # create a schema
    try:
        short_connection: str = f"trino://{config.instance.trino_user}@localhost:{config.instance.server_port}/{config.instance.catalog}/information_schema"
        logger.info(f"connecting to trino engine {short_connection}")

        engine = create_engine(short_connection, connect_args={"http_scheme": "http"})

        logger.info(f"create_engine() okay")
        with engine.connect() as conn:
            logger.info(f"engine.connect() : complete")

            # Now create the schema in the available catalog
            create_schema_sql = text(
                f"CREATE SCHEMA IF NOT EXISTS {config.instance.catalog}.{config.schema}"
            )
            conn.execute(create_schema_sql)
            conn.commit()

            # Verify schema was created
            result = conn.execute(text(f"SHOW SCHEMAS FROM {config.instance.catalog}"))
            schemas = [row[0] for row in result]
            logger.info(f"Available schemas in {config.instance.catalog}: {schemas}")
        engine.dispose()

        logger.info(f"Created schema {config.instance.catalog}.{config.schema}")
    except Exception as e:
        logger.error(f"Failed to create schema: {e}")
        raise

    # the schema is ready
    yield config

    # should we clean up the schema?


#### ==========================================================================
## MinIO two-part fixture

# (the minio is in another branch)
