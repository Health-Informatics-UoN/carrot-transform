import logging
import time
from dataclasses import dataclass
from typing import Generator

import docker
import pytest
from sqlalchemy import create_engine

from tests import testools

#
logger = logging.getLogger(__name__)

STARTUP_TIMEOUT = 60
STARTUP_SLEEP = 0.2


@dataclass
class PostgreSQLConfig:
    """config for a post-gresql connection"""

    docker_name: str
    db_name: str
    db_user: str
    db_pass: str
    db_port: int

    @property
    def connection(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_pass}@localhost:{self.db_port}/{self.db_name}"


@dataclass
class PostgreSQLContainer:
    """an object used by the fixture to tell a test about their postgres instance"""

    container: docker.models.containers.Container
    config: PostgreSQLConfig


@pytest.fixture(scope="function")
def postgres(docker_ip) -> Generator[PostgreSQLContainer, None, None]:
    """Start a PostgreSQL container for tests"""

    config: PostgreSQLConfig = PostgreSQLConfig(
        docker_name=f"carrot_test_docker_{testools.rand_hex()}",
        db_name=f"c_test_d_{testools.rand_hex()}",
        db_user=f"c_test_u_{testools.rand_hex()}",
        db_pass=f"c_test_p_{testools.rand_hex()}",
        db_port=5432,  # random.randrange(5200, 5400),
    )

    container = docker.from_env().containers.run(
        "postgres:13",
        name=config.docker_name,
        environment={
            "POSTGRES_DB": config.db_name,
            "POSTGRES_USER": config.db_user,
            "POSTGRES_PASSWORD": config.db_pass,
        },
        ports={f"{config.db_port}/tcp": ("127.0.0.1", config.db_port)},
        detach=True,
        remove=True,
    )

    # Wait for PostgreSQL to be ready
    start_time = time.time()
    while time.time() - start_time < STARTUP_TIMEOUT:
        try:
            engine = create_engine(config.connection)
            conn = engine.connect()
            conn.close()
            break
        except Exception:
            # logger.error(e)
            time.sleep(STARTUP_SLEEP)
    else:
        container.stop()
        raise Exception("PostgreSQL container failed to start")

    yield PostgreSQLContainer(container=container, config=config)

    # Cleanup
    container.stop()
