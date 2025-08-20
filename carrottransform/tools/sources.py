import csv
import logging
from pathlib import Path

import sqlalchemy
from sqlalchemy import MetaData, select

logger = logging.getLogger(__name__)


class SourceException(Exception):
    def __init__(self, source, message: str):
        self._source = source
        self._message = message
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
            raise SourceNotFoundException(
                self, name, f"source names must end with .csv but was {name=}"
            )
        if "/" in name or "\\" in name:
            raise SourceNotFoundException(
                self, name, f"source names must name a file not a path but was {name=}"
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
                with path.open(mode="r", encoding="utf-8-sig") as file:
                    for row in csv.reader(file):
                        yield row

            src = open_csv_rows(self, path)

        # Force the generator to run until first yield
        import itertools

        try:
            first = next(src)
        except StopIteration:
            # empty generator? can only happen if we tried to open a csv file, right?
            raise SourceNotFoundException(self, name, f"csv file is empty {name=}")
        except Exception as e:
            raise SourceNotFoundException(
                self, name, f"failed to find first row because {e=}"
            )
        return itertools.chain([first], src)
