import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

import boto3
import docker
import pytest
import sqlalchemy
from click.testing import CliRunner
from sqlalchemy import create_engine

import carrottransform.tools.sources as sources
from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs

#
logger = logging.getLogger(__name__)

# Get the package root directory
project_root: Path = Path(__file__).parent.parent
package_root: Path = project_root / "carrottransform"
test_data = Path(__file__).parent / "test_data"

# need/want to define the s3 bucket somewhere, so, let's do it here
CARROT_TEST_BUCKET = "carrot-transform-testtt"

#### ==========================================================================
## unit test cases - test the test functions


@pytest.mark.unit
def test_compare(caplog) -> None:
    """test the validator"""

    caplog.set_level(logging.INFO)

    path: Path = project_root / "tests/test_data/observe_smoking"

    compare_to_tsvs("observe_smoking", sources.csv_source_object(path, sep="\t"))


#### ==========================================================================
## verification functions


def compare_to_tsvs(subpath: str, so: sources.SourceObject) -> None:
    """scan all tsv files in a folder.

    open each .tsv in the tests subpath and compare it to the open'ed from the named SO.

    if the SO is missing a tsv? fail!
    if the SO has different rows? fail!
    if the SO has extra/too few rows? fail!
    if the SO has .tsv files we don't ... pass ...

    """

    from carrottransform.tools.args import PathArg

    test: Path
    if subpath.startswith("@carrot"):
        test = PathArg.convert(subpath, None, None)
    else:
        test = project_root / "tests/test_data" / subpath

    # open the saved .tsv file
    so_ex = sources.csv_source_object(test, sep="\t")

    person_ids_seen = False
    persons_seen = False

    for item in test.glob("*.tsv"):
        name: str = item.name[:-4]

        # just skip summary_mapstream
        if "summary_mapstream" == name:
            continue

        person_ids_seen = person_ids_seen or ("person_ids" == name)
        persons_seen = persons_seen or ("person" == name)

        import itertools

        idx = 0
        for e, a in itertools.zip_longest(so_ex.open(name), so.open(name)):
            assert e is not None, f"expected value {idx} is missing"
            assert a is not None, f"expected value {idx} is missing"

            assert e == a, (
                f"{name=} expected/actual values {idx} do not match\n\t{e=}\n\t{a=}"
            )
            idx += 1
        logger.info(f"matching {subpath=} for {name=}")

    assert person_ids_seen and persons_seen, (
        "verification data missing from the test case"
    )

    # it matches!
    logger.info(f"all match in {subpath=}")


#### ==========================================================================
## test case generation


def keyed_variations(**kv):
    """given some things passed as k=[v1,v2], yields all permutations"""
    keys = kv.keys()
    for values in product(*kv.values()):
        yield dict(zip(keys, values))


def variations(keys):
    """
    computes key -> bool dicts where all, none, or only one value is true or false, then, maps those dicts to simple lists
    """

    def permutations(keys):
        c = len(keys)
        assert c > 0
        if c == 1:
            yield {keys[0]: True}
            yield {keys[0]: False}
        else:
            k = keys[0]
            for p in permutations(keys[1:]):
                p = p.copy()
                p[k] = True
                yield p
                p = p.copy()
                p[k] = False
                yield p

    for p in permutations(list(keys)):
        values = list(p.values())
        tc = values.count(True)
        fc = values.count(False)

        if (tc in [0, 1]) or (fc in [0, 1]):
            o = []
            for k in p:
                if p[k]:
                    o += [k]
            yield o


def permutations(**name_to_list):
    """given a map of lists; yield all permutations of the contents"""

    def loop(listing):
        c = len(listing)

        if 0 == c:
            return

        head_key, head_items = listing[0]

        if 1 == c:
            for v in head_items:
                yield {head_key: v}

            return

        for t in loop(listing[1:]):
            for v in head_items:
                t[head_key] = v
                yield t.copy()

    for i in loop(list(map(lambda k: (k, name_to_list[k]), name_to_list.keys()))):
        yield i


def zip_loop(*ar: list[dict]):
    # convert them all to lists so that they're "stable"
    args = list(list(a) for a in ar)

    # find the longest length
    max_length = max(len(a) for a in args)

    def loop(a):
        while True:
            for i in a:
                yield i

    # turn them all into forever loops
    loopers = [loop(a) for a in args]

    # now build "rows" from each
    count = 0
    while count < max_length:
        count += 1

        # start an empty row
        row: dict = {}

        # each of those inputs will contribute some {k:v} so we union them togehter
        for c in loopers:
            row = row | next(c).copy()

        # yield this row before we continue
        yield row


class CarrotTestCase:
    """defines an integration test case in terms of the person file, and the optional mapper rules"""

    def __init__(self, person_name: str, mapper: str = "", suffix=""):
        self._suffix = suffix
        self._person_name = person_name

        self._folder = (test_data / person_name).parent

        # find the rules mapping
        if mapper == "":
            for json in self._folder.glob("*.json"):
                assert "" == mapper
                mapper = str(json).replace("\\", "/")
        assert "" != mapper
        self._mapper = mapper

        assert 1 == person_name.count("/")
        [label, person] = person_name.split("/")
        self._label = label
        assert person.endswith(".csv")
        self._person = person[:-4]

    def load_sqlite(self, tmp_path: Path):
        assert tmp_path.is_dir()

        # create an SQLite database and copy the contents into it
        sqlite3 = tmp_path / f"{self._label}.sqlite3"
        copy_across(
            ot=outputs.sql_output_target(
                sqlalchemy.create_engine(f"sqlite:///{sqlite3.absolute()}")
            ),
            so=self._folder,
        )
        return f"sqlite:///{sqlite3.absolute()}"

    def compare_to_tsvs(self, source, suffix=""):
        compare_to_tsvs(self._label + self._suffix, source)


#### ==========================================================================
## utility functions


def copy_across(ot: outputs.OutputTarget, so: sources.SourceObject | Path, names=None):
    assert isinstance(so, Path) == (names is None)
    if isinstance(so, Path):
        names = [file.name[:-4] for file in so.glob("*.csv")]
        so = sources.csv_source_object(path=so, sep=",")
    assert isinstance(so, sources.SourceObject)

    # copy all named ones across
    for name in names:
        i = so.open(name)
        o = None

        for r in i:
            if o is None:
                o = ot.start(name, r)
            else:
                o.write(r)
        # o.close()
        # i.close()

    #
    so.close()
    ot.close()


def run_v1(
    inputs: str,
    person: str,
    mapper: str,
    output: str,
):
    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--inputs",
            inputs,
            "--rules-file",
            mapper,
            "--person",
            person,
            "--output",
            output,
            "--omop-ddl-file",
            "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
        ],
    )

    if result.exception is not None:
        print(result.exception)
        raise (result.exception)

    assert 0 == result.exit_code


def rand_hex(length: int = 16) -> str:
    """genearttes a random hex string. used for test data"""

    out = ""
    src = "0123456789abcdef"

    for i in range(0, length):
        out += src[random.randint(0, len(src) - 1)]

    return out


def delete_s3_folder(coordinate):
    """
    Delete a folder and all its contents from an S3 bucket.

    Args:
        bucket (str): Name of the S3 bucket
        folder (str): Folder path to delete (e.g., 'my-folder/' or 'prefix/subfolder/')
    """

    [bucket, folder] = outputs.s3_bucket_folder(coordinate)

    client = boto3.client("s3")

    # Ensure the folder path ends with a slash
    if not folder.endswith("/"):
        folder = folder + "/"

    # List all objects in the folder
    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=folder)

    # Collect all objects to delete
    objects_to_delete = []
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                objects_to_delete.append({"Key": obj["Key"]})

    if not objects_to_delete:
        logger.info(f"No objects found in folder '{folder}'")
        return

    # Delete all objects in batches of 1000 (S3 API limit)
    for i in range(0, len(objects_to_delete), 1000):
        batch = objects_to_delete[i : i + 1000]
        response = client.delete_objects(Bucket=bucket, Delete={"Objects": batch})

        # Check for errors in deletion
        if "Errors" in response and response["Errors"]:
            logger.info(f"Errors deleting some objects: {response['Errors']}")

    logger.info(f"Successfully deleted folder '{folder}' and its contents")


#### ==========================================================================
##  functions specific to tests


##
# build the env and arg parameters
def passed_as(pass_as, *args):
    args = list(args)

    env = {}
    i = 0

    while i < len(args):
        k = args[i][2:]

        if k not in pass_as:
            i += 2
            continue

        # conver the key
        k = k.upper().replace("-", "_")

        # get the value
        v = args[i + 1]

        # save it to the evn vars
        env[k] = v

        # demove the key and value from teh list
        args = args[:i] + args[(i + 2) :]

    return (env, args)


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
        docker_name=f"carrot_test_docker_{rand_hex()}",
        db_name=f"c_test_d_{rand_hex()}",
        db_user=f"c_test_u_{rand_hex()}",
        db_pass=f"c_test_p_{rand_hex()}",
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
    timeout = 30
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            engine = create_engine(config.connection)
            conn = engine.connect()
            conn.close()
            break
        except:
            time.sleep(0.5)
    else:
        container.stop()
        raise Exception("PostgreSQL container failed to start")

    yield PostgreSQLContainer(container=container, config=config)

    # Cleanup
    container.stop()
