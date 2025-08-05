from pathlib import Path
import csv
import logging
from sqlalchemy import Table, Column, String, MetaData, insert, select

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

    def load(self, tablename: str, csv: Path):
        """load a csv file into a database. does some adjustments to make sure the column names work"""

        assert self._engine is not None

        csvr = self._open_csv(csv)
        column_names = next(csvr)

        # if the column names have a blank at the end we need to stripit
        if "" == column_names[-1]:
            column_names = column_names[:-1]

        # make sure there's no other blankes
        for name in column_names:
            if "" == name:
                raise Exception("can't have a blank column name in the CSVs")
            if " " in name:
                raise Exception("can't have spaces in the CSV column names")

        # Create the table in the database
        metadata = MetaData()
        table = Table(
            tablename, metadata, *([Column(name, String(255)) for name in column_names])
        )
        metadata.create_all(self._engine, tables=[table])

        # # Insert rows
        with self._engine.begin() as conn:
            for row in csvr:
                record = dict(zip(column_names, row))
                conn.execute(insert(table), record)

    def open(self, name: str):
        assert name.endswith(".csv")

        if self._folder is not None:
            src = self._open_csv(name)
        else:
            src = self._open_sql(name)

        # Force the generator to run until first yield
        import itertools

        try:
            first = next(src)
        except StopIteration:
            return src  # empty generator
        except Exception as e:
            raise e
        return itertools.chain([first], src)

    def _open_csv(self, name: str | Path):
        """opens a file and returns the headers and rows"""

        assert isinstance(name, Path) or (
            self._folder is not None and isinstance(self._folder, Path)
        )

        path: Path = name if isinstance(name, Path) else (self._folder / name)

        if not path.is_file():
            raise SourceFileNotFoundException(self, path)

        with path.open(mode="r", encoding="utf-8-sig") as file:
            for row in csv.reader(file):
                yield row

    def _open_sql(self, name: str):
        assert name.endswith(".csv")
        name = name[:-4]

        metadata = MetaData()
        metadata.reflect(bind=self._engine, only=[name])
        table = metadata.tables[name]

        with self._engine.connect() as conn:
            result = conn.execute(select(table))
            yield result.keys()  # list of column names

            for row in result:
                # we overwrite the date values so convert it to a list
                yield list(row)
