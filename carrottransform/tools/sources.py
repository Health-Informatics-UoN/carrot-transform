from pathlib import Path
import csv
import logging
from sqlalchemy import MetaData, select

import sqlalchemy

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
        engine: sqlalchemy.engine.Engine | str | None = None,
    ) -> None:
        if folder is None and engine is None:
            raise RuntimeError("SourceOpener needs either an engine or a folder")

        if folder is not None and engine is not None:
            raise RuntimeError("SourceOpener cannot have both a folder and an engine")

        self._folder = folder
        if self._folder is not None:
            assert engine is None
            if not self._folder.is_dir():
                raise SourceFolderMissingException(self)

        if engine is None:
            assert self._folder is not None
            self._engine = None
        elif isinstance(engine, str):
            assert self._folder is None
            self._engine = sqlalchemy.create_engine(engine)
        else:
            self._engine = engine

        assert (self._folder is None) or (self._engine is None)
        assert (self._folder is not None) or (self._engine is not None)

    def open(self, name: str):
        assert name.endswith(".csv")

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
