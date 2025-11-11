import csv
import logging
from pathlib import Path
from typing import Iterator

import click
import sqlalchemy
from sqlalchemy import MetaData, select

from carrottransform import require
from carrottransform.tools.outputs import s3BucketFolder

logger = logging.getLogger(__name__)


def keen_head(data):
    """Force the generator to run until first yield"""
    import itertools

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
        assert not table.endswith(".csv")  # debugging check
        raise Exception("virtual method called")

    def close(self):
        raise Exception("virtual method called")


class SourceObjectArgumentType(click.ParamType):
    name = "a connection to the/a source (whatever that may be)"

    def convert(self, value: str, param, ctx):
        value: str = str(value)
        if value.startswith("s3:"):
            return s3SourceObject(
                value, "\t"
            )  # TODO; do something else with the separators

        if value.startswith(
            "sqlite:"
        ):  # TODO; allow other sorts of database connections
            return sqlSourceObject(sqlalchemy.create_engine(value))

        return csvSourceObject(Path(value), sep=",")


# create a singleton for the Click settings
SourceArgument = SourceObjectArgumentType()


def sqlSourceObject(connection: sqlalchemy.engine.Engine) -> SourceObject:
    class SO(SourceObject):
        def __init__(self):
            pass

        def close(self):
            pass

        def open(self, table: str) -> Iterator[list[str]]:
            assert not table.endswith(".csv")

            def sql():
                metadata = MetaData()
                metadata.reflect(bind=connection, only=[table])
                source = metadata.tables[table]
                with connection.connect() as conn:
                    result = conn.execute(select(source))
                    yield result.keys()

                    for row in result:
                        yield list(row)

            return keen_head(sql())

    return SO()


def csvSourceObject(path: Path, sep: str) -> SourceObject:
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
            assert not table.endswith(".csv")

            file = path / (table + ext)

            if not file.is_file():
                logger.error(f"couldn't find {table=} in csvs at path {path=}")
                raise SourceTableNotFound(table)

            # used to check "doking" where we remove the last entry if the colum name and each row's final cell are ''
            doked = False  # "doked" like curring a dog's tail off
            count = -1

            for row in csv.reader(file.open("r", encoding="utf-8-sig"), delimiter=sep):
                if count == -1:
                    count = len(row)
                    if row[-1].strip() == "":
                        doked = True
                        count = len(row) - 1

                if doked:
                    require("" == row[-1].strip())
                    row = row[:-1]

                require(len(row) == count)

                yield row

    return SO()


def s3SourceObject(coordinate: str, sep: str) -> SourceObject:
    class SO(SourceObject):
        def __init__(self, coordinate: str):
            import boto3

            [b, f] = s3BucketFolder(coordinate)
            self._bucket_resource = boto3.resource("s3").Bucket(b)
            self._bucket_folder = f

        def close(self):
            self._bucket_resource = None

        def open(self, table: str) -> Iterator[list[str]]:
            assert not table.endswith(".csv")

            import csv
            import io

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
                raise RuntimeError(f"Failed to read {table=} from S3: {e=} w/ {key=}")

    return SO(coordinate)
