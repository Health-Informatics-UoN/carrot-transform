import logging
import random
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, Iterable

import boto3
import docker
import pytest
import requests
import sqlalchemy
from botocore import client
from click.testing import CliRunner
from minio import Minio
from minio.error import S3Error
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
        except:
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
