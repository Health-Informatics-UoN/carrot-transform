import csv
import io
import itertools
import logging
import re
from pathlib import Path
from typing import Iterator

import boto3
import click
import sqlalchemy
from sqlalchemy import MetaData, select

from carrottransform import require
from carrottransform.tools import at_path, outputs
from carrottransform.tools.outputs import s3_bucket_folder

logger = logging.getLogger(__name__)


def keen_head(data):
    """Force the generator to run until first yield"""

    try:
        first = next(data)
    except StopIteration:
        return data  # empty generator
    except Exception as e:
        raise e
    return itertools.chain([first], data)


class SourceNotFound(Exception):
    def __init__(self, path):
        super().__init__(f"couldn't open the source at {path=}")
        self._path = path


class SourceTableNotFound(Exception):
    def __init__(self, name: str):
        super().__init__(f"couldn't open table {name=}")
        self._name = name


class SourceObject:
    def __init__(self):
        pass

    def open(self, table: str) -> Iterator[list[str]]:
        require(not table.endswith(".csv"))  # debugging check
        raise Exception("virtual method called")

    def close(self):
        raise Exception("virtual method called")


class SourceObjectArgumentType(click.ParamType):
    name = "a connection to the/a source (whatever that may be)"

    def convert(self, value: str, param, ctx):
        value = str(value)
        if value.startswith("minio:"):
            # TODO; do something else with the separators
            return minio_source_object(value, "\t")

        if re.match(r"[\w]+://.+", value):
            return sql_source_object(sqlalchemy.create_engine(value))

        return csv_source_object(at_path.convert_path(value), sep=",")


# create a singleton for the Click settings
SourceArgument = SourceObjectArgumentType()


def sql_source_object(connection: sqlalchemy.engine.Engine | str) -> SourceObject:
    SQL_TO_LOWER: bool = True

    # if the parameter is not a connection; make it one
    # ... and fail-fast if it can't be used to open a connection
    engine: sqlalchemy.engine.Engine = (
        connection
        if isinstance(connection, sqlalchemy.engine.Engine)
        else sqlalchemy.create_engine(connection)
    )

    class SO(SourceObject):
        def __init__(self):
            pass

        def close(self):
            pass

        def open(self, table: str) -> Iterator[list[str]]:
            require(
                not table.endswith(".csv"),
                f"table names shouldn't have a file extension {table=}",
            )
            require("/" not in table, f"invalid table name {table=}")

            # trino needs table names to be lower case to match them (sometimes) and SQL is case insensitive anyway
            table = table.lower() if SQL_TO_LOWER else table

            def sql() -> Iterator[list[str]]:
                with engine.connect() as connection:
                    metadata = MetaData()
                    metadata.reflect(bind=connection, only=[table])
                    source = metadata.tables[table]
                    result = connection.execute(select(source))

                    header: list[str]
                    try:
                        header = list(result.keys())
                    except Exception as e:
                        raise Exception(f"{table} raised error on .keys(); {e=}")

                    if SQL_TO_LOWER:
                        header = list(map(lambda a: a.lower(), header))

                    yield header

                    for row in result:
                        yield list(row)

            return keen_head(sql())

    return SO()


def csv_source_object(path: Path, sep: str) -> SourceObject:
    ext: str = (
        {
            "\t": ".tsv",
            ",": ".csv",
        }
    )[sep]

    if not path.is_dir():
        raise SourceNotFound(path)

    class SO(SourceObject):
        def __init__(self):
            pass

        def close(self):
            pass

        def open(self, table: str) -> Iterator[list[str]]:
            return keen_head(self.open_really(table))

        def open_really(self, table: str) -> Iterator[list[str]]:
            require(not table.endswith(".csv"))

            file = path / (table + ext)

            if not file.is_file():
                logger.error(f"couldn't find {table=} in csvs at path {path=}")
                raise SourceTableNotFound(table)

            # csvs can have trailing commas (from excel)
            # we remove the last column if the column name is "" and check that each row's entry is also ""
            trimmed = False  # are we trimming off the last entry for this object?
            count = -1

            for row in csv.reader(file.open("r", encoding="utf-8-sig"), delimiter=sep):
                if count == -1:
                    count = len(row)
                    if row[-1].strip() == "":
                        trimmed = True
                        count = len(row) - 1

                if trimmed:
                    require(row[-1].strip() == "")
                    row = row[:-1]

                require(len(row) == count)

                yield row

    return SO()


def s3_source_object(coordinate: str, sep: str) -> SourceObject:
    class SO(SourceObject):
        def __init__(self, coordinate: str):
            [b, f] = s3_bucket_folder(coordinate)
            self._bucket_resource = boto3.resource("s3").Bucket(b)
            self._bucket_folder = f

        def close(self):
            self._bucket_resource = None

        def open(self, table: str) -> Iterator[list[str]]:
            require(not table.endswith(".csv"))

            key = self._bucket_folder + table

            # Example: read CSV from S3
            try:
                obj = self._bucket_resource.Object(key)

                # Stream the content without loading everything into memory
                stream = obj.get()["Body"]
                text_stream = io.TextIOWrapper(stream, encoding="utf-8")
                reader = csv.reader(text_stream, delimiter=sep)

                for row in reader:
                    yield row
                stream.close()
            except Exception as e:
                logger.error(f"Failed to read {table=} from S3: {e=} w/ {key=}")
                exit(1)

    return SO(coordinate)


def minio_source_object(coordinate: str, sep: str) -> SourceObject:
    class SO(SourceObject):
        def __init__(self, coordinate: str):
            bucket = outputs.MinioURL(coordinate)

            self._bucket_resource = boto3.resource(
                "s3",
                endpoint_url=f"{bucket._protocol}://{bucket._host}:{bucket._port}",
                aws_access_key_id=bucket._user,
                aws_secret_access_key=bucket._pass,
            ).Bucket(bucket._bucket)
            self._bucket_folder = bucket._folder

        def close(self):
            self._bucket_resource = None

        def open(self, table: str) -> Iterator[list[str]]:
            require(not table.endswith(".csv"))

            key = self._bucket_folder + table

            # Example: read CSV from S3
            try:
                obj = self._bucket_resource.Object(key)

                # Stream the content without loading everything into memory
                stream = obj.get()["Body"]
                text_stream = io.TextIOWrapper(stream, encoding="utf-8")
                reader = csv.reader(text_stream, delimiter=sep)

                for row in reader:
                    yield row
                stream.close()
            except Exception as e:
                logger.error(f"Failed to read {table=} from S3: {e=} w/ {key=}")
                exit(1)

    return SO(coordinate)
