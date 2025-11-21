import csv
import logging
from pathlib import Path
from typing import Iterator

import click
import sqlalchemy
from sqlalchemy import MetaData, select

from carrottransform import require
from carrottransform.tools.outputs import s3_bucket_folder

logger = logging.getLogger(__name__)


class SourceException(Exception):
    def __init__(self, source, message: str):
        self._source = source
        super().__init__(message)


class SourceFolderMissingException(SourceException):
    def __init__(self, source):
        super().__init__(
            source, f"Source folder '{str(source._folder)}' does not exist"
        )


class SourceNotFoundException(SourceException):
    def __init__(self, source, name: str, message: str):
        super().__init__(source, message)
        self._name = name


class SourceFileNotFoundException(SourceNotFoundException):
    def __init__(self, source, path: Path):
        super().__init__(
            source, path.name, f"Source file '{path.name}' not found by {source}"
        )
        self._path = path


class SourceOpener:
    def __init__(
        self,
        folder: Path | None = None,
        engine: sqlalchemy.engine.Engine | None = None,
    ) -> None:
        if folder is None and engine is None:
            raise RuntimeError("SourceOpener needs either an engine or a folder")

        if folder is not None and engine is not None:
            raise RuntimeError("SourceOpener cannot have both a folder and an engine")

        self._folder = folder
        if self._folder is not None:
            if not self._folder.is_dir():
                raise SourceFolderMissingException(self)

        if engine is None:
            self._engine = None
        else:
            self._engine = engine

    def open(self, name: str):
        if not name.endswith(".csv"):
            raise RuntimeError(f"source names must end with .csv but was {name=}")
        if "/" in name or "\\" in name:
            raise RuntimeError(
                f"source names must name a file not a path but was {name=}"
            )

        if self._folder is None:

            def open_sql(src, name: str):
                name = name[:-4]

                metadata = MetaData()
                metadata.reflect(bind=src._engine, only=[name])
                table = metadata.tables[name]

                with src._engine.connect() as conn:
                    result = conn.execute(select(table))
                    yield result.keys()  # list of column names

                    for row in result:
                        # we overwrite the date values so convert it to a list
                        yield list(row)

            src = open_sql(self, name)
        else:
            path: Path = self._folder / name
            if not path.is_file():
                raise SourceFileNotFoundException(self, path)

            def open_csv_rows(src: SourceOpener, path: Path):
                if not path.is_file():
                    raise SourceFileNotFoundException(src, path)

                with path.open(mode="r", encoding="utf-8-sig") as file:
                    for row in csv.reader(file):
                        yield row

            src = open_csv_rows(self, path)

        # Force the generator to run until first yield
        import itertools

        try:
            first = next(src)
        except StopIteration:
            return src  # empty generator
        except Exception as e:
            raise e
        return itertools.chain([first], src)


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
        require(not table.endswith(".csv"))  # debugging check
        raise Exception("virtual method called")

    def close(self):
        raise Exception("virtual method called")


class SourceObjectArgumentType(click.ParamType):
    name = "a connection to the/a source (whatever that may be)"

    def convert(self, value: str, param, ctx):
        value = str(value)
        if value.startswith("s3:"):
            return s3_source_object(
                value, "\t"
            )  # TODO; do something else with the separators

        if value.startswith(
            "sqlite:"
        ):  # TODO; allow other sorts of database connections
            return sql_source_object(sqlalchemy.create_engine(value))

        return csv_source_object(Path(value), sep=",")


# create a singleton for the Click settings
SourceArgument = SourceObjectArgumentType()


def sql_source_object(connection: sqlalchemy.engine.Engine) -> SourceObject:
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
                    require("" == row[-1].strip())
                    row = row[:-1]

                require(len(row) == count)

                yield row

    return SO()


def s3_source_object(coordinate: str, sep: str) -> SourceObject:
    class SO(SourceObject):
        def __init__(self, coordinate: str):
            import boto3

            [b, f] = s3_bucket_folder(coordinate)
            self._bucket_resource = boto3.resource("s3").Bucket(b)
            self._bucket_folder = f

        def close(self):
            self._bucket_resource = None

        def open(self, table: str) -> Iterator[list[str]]:
            require(not table.endswith(".csv"))

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
