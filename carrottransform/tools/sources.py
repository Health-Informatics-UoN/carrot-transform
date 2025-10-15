import csv
import logging
from pathlib import Path
from typing import Iterator
import click
import sqlalchemy

import sqlalchemy
from sqlalchemy import MetaData, select

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


class SourceObject:
    def __init__(self):
        pass

    def open(self, table: str) -> Iterator[list[str]]:
        assert not table.endswith(".csv")  # debugging check
        raise NotImplemented("virtual method called")

    def close(self):
        raise NotImplemented("virtual method called")

class SourceObjectArgumentType(click.ParamType):
    """"""

    name = "a connection to the/a source (whatever that may be)"

    def convert(self, value: str, param, ctx):
        value: str = str(value)
        if value.startswith("s3:"):
            bucket = value[len("s3:") :]
            return s3SourceObject(bucket, '\t') # TODO; do something else with the separators
        else:
            return csvSourceObject(Path(value), sep='\t')


# create a singleton for the Click settings
SourceArgument = SourceObjectArgumentType()

def csvSourceObject(path: Path, sep: str) -> SourceObject:
    ext: str = (
        {
            "\t": ".tsv",
            ",": ".csv",
        }
    )[sep]

    class SO(SourceObject):
        def __init__(self):
            pass

        def close(self):
            pass

        def open(self, table: str) -> Iterator[list[str]]:
            assert not table.endswith(".csv")
            file = (path / (table + ext)).open("r", encoding="utf-8")
            reader = csv.reader(file, delimiter=sep)

            for row in reader:
                yield row

            file.close()

    return SO()


def s3SourceObject(bucket: str, sep: str) -> SourceObject:
    class SO(SourceObject):
        def __init__(self, bucket):
            import boto3

            self._bucket = boto3.resource("s3").Bucket(bucket)

        def close(self):
            self._bucket = None

        def open(self, table: str) -> Iterator[list[str]]:
            assert not table.endswith(".csv")

            import csv
            import io

            # Example: read CSV from S3
            try:
                obj = self._bucket.Object(table)

                # Stream the content without loading everything into memory
                stream = obj.get()["Body"]
                text_stream = io.TextIOWrapper(stream, encoding="utf-8")
                reader = csv.reader(text_stream, delimiter=sep)

                for row in reader:
                    yield row
                stream.close()
            except Exception as e:
                raise RuntimeError(f"Failed to read {table=} from S3: {e=}")

    return SO(bucket)
