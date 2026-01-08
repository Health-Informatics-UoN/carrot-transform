import logging
import random
import textwrap
import time
from dataclasses import dataclass
from typing import Generator, Iterable

import boto3
import docker
import pytest
import requests
from requests.exceptions import ConnectionError, ConnectTimeout, ReadTimeout, Timeout
from sqlalchemy import create_engine, text

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
        textwrap.dedent("""
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

    # Create data directory with open permissions
    data_dir = tmp_path / "var"
    data_dir.mkdir(parents=True, exist_ok=True)
    data_dir.chmod(0o777)

    client = docker.from_env()
    client.images.pull("trinodb/trino:latest")
    container = client.containers.run(
        "trinodb/trino:latest",
        name=config.docker_name,
        ports={f"{config.server_port}/tcp": ("127.0.0.1", config.server_port)},
        volumes={
            str(config_dir): {"bind": "/etc/trino", "mode": "rw"},
            str(data_dir): {"bind": "/var/trino", "mode": "rw"},
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
        except (ConnectionError, Timeout, ConnectTimeout, ReadTimeout):
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

    def __init__(self, instance: TrinoInstance):
        self._instance = instance
        self._schema = f"schema_{testools.rand_hex()}"

    @property
    def connection(self) -> str:
        return f"{self._instance.connection}/{self._schema}"


@pytest.fixture(scope="function")
def trino(trino_instance) -> Iterable[TrinoSchema]:
    """Start a Trino schema"""

    config = TrinoSchema(instance=trino_instance)

    # create a schema
    try:
        short_connection: str = f"trino://{config._instance.trino_user}@localhost:{config._instance.server_port}/{config._instance.catalog}/information_schema"
        logger.info(f"connecting to trino engine {short_connection}")

        engine = create_engine(short_connection, connect_args={"http_scheme": "http"})

        logger.info("create_engine() okay")
        with engine.connect() as conn:
            logger.info("engine.connect() : complete")

            # Now create the schema in the available catalog
            create_schema_sql = text(
                f"CREATE SCHEMA IF NOT EXISTS {config._instance.catalog}.{config._schema}"
            )
            conn.execute(create_schema_sql)
            conn.commit()

            # Verify schema was created
            result = conn.execute(text(f"SHOW SCHEMAS FROM {config._instance.catalog}"))
            schemas = [row[0] for row in result]
            logger.info(f"Available schemas in {config._instance.catalog}: {schemas}")

        logger.info(f"Created schema {config._instance.catalog}.{config._schema}")
    except Exception as e:
        logger.error(f"Failed to create schema: {e}")
        raise

    # the schema is ready
    yield config

    # should we clean up the schema?


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
