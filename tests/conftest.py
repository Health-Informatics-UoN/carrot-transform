import logging
import time
from dataclasses import dataclass
from typing import Generator, Iterable

import boto3
import docker
import pytest
import requests
from sqlalchemy import create_engine

from tests import testools

#
logger = logging.getLogger(__name__)

STARTUP_TIMEOUT = 60
STARTUP_SLEEP = 0.2

#### ==========================================================================
## PostgreSQL two-part fixture


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


#### ==========================================================================
## Trino two-part fixture


#### ==========================================================================
## MinIO two-part fixture


@dataclass
class MinIOContainer:
    docker_ip: str
    docker_name: str = f"minio_{testools.rand_hex()}"

    console_port: int = -1
    server_port: int = -1

    username: str = f"minio_user_{testools.rand_hex()}"
    password: str = f"minio_pass_{testools.rand_hex()}"


@pytest.fixture(scope="session")
def minio_config(docker_ip) -> Iterable[MinIOContainer]:
    # setup the logger function
    start_time = time.time()

    def log_info(message: str):
        now = time.time() - start_time
        logger.info(f"after {now} {message}")

    #
    config = MinIOContainer(docker_ip=docker_ip)

    # create docker and pull the container
    client = docker.from_env()
    client.images.pull("minio/minio:latest")
    log_info("container pulled")

    # start the container
    container = client.containers.run(
        name=config.docker_name,
        image="minio/minio:latest",
        command="server /data --console-address ':9001'",
        environment={
            "MINIO_ROOT_USER": config.username,
            "MINIO_ROOT_PASSWORD": config.password,
        },
        ports={
            "9000/tcp": None,  # Random host port for MinIO API
            "9001/tcp": None,  # Random host port for MinIO Console
        },
        detach=True,
        remove=True,
    )
    log_info("container started")

    # Get the assigned random ports
    container.reload()
    port_data = container.attrs["NetworkSettings"]["Ports"]
    config.server_port = port_data["9000/tcp"][0]["HostPort"]
    config.console_port = port_data["9001/tcp"][0]["HostPort"]
    log_info(f"read ports as {config.server_port} / {config.console_port}")

    # wait for the server itself to start
    start_time = time.time()
    while time.time() - start_time < STARTUP_TIMEOUT:
        try:
            # check the ready ienpoint
            response = requests.get(
                f"http://{config.docker_ip}:{config.server_port}/minio/health/ready"
            )
            if response.status_code == 200:
                log_info("found ready endpoint")
                break
        except requests.exceptions.RequestException:
            time.sleep(STARTUP_SLEEP)
    else:
        # don't bother stopping the contianer if it didn't start
        logs = container.logs().decode("utf-8")
        logger.error(
            f"MinIO container failed to start within {STARTUP_TIMEOUT} seconds"
        )
        logger.error(f"Full MinIO container logs:\n{logs}")
        raise Exception("MinIO container failed to start")

    log_info("container ready")
    yield config

    container.stop()
    log_info("container stopped")


@dataclass
class MinIOBucket:
    config: MinIOContainer
    name: str

    @property
    def connection(self) -> str:
        return f"minio:{self.config.username}:{self.config.password}@http://{self.config.docker_ip}:{self.config.server_port}/{self.name}"


@pytest.fixture(scope="function")
def minio(minio_config) -> Iterable[MinIOBucket]:
    bucket = MinIOBucket(config=minio_config, name=f"test-bucket-{testools.rand_hex()}")
    bucket_name = bucket.name

    # Connect to MinIO using boto3
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"http://{bucket.config.docker_ip}:{minio_config.server_port}",
        aws_access_key_id=minio_config.username,
        aws_secret_access_key=minio_config.password,
    )

    # Create bucket
    s3_client.create_bucket(Bucket=bucket_name)
    logger.info(f"Created bucket: {bucket_name}")

    # Verify bucket exists
    response = s3_client.list_buckets()
    bucket_names = [bucket["Name"] for bucket in response["Buckets"]]
    assert bucket_name in bucket_names

    yield bucket

    # the container will be destroyed; delete the bucket
