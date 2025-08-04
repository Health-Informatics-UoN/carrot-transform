from pathlib import Path
import csv
import logging
from sqlalchemy import Table, Column, String, MetaData, insert, select

import sqlalchemy

logger = logging.getLogger(__name__)

# def open_csv(file_path: Path) -> Iterator[list[str]] | None:
#     """opens a file and does something related to CSVs"""
#     try:
#         fh = file_path.open(mode="r", encoding="utf-8-sig")
#         csvr = csv.reader(fh)
#         return csvr
#     except IOError as e:
#         logger.exception("Unable to open: {0}".format(file_path))
#         logger.exception("I/O error({0}): {1}".format(e.errno, e.strerror))
#         return None


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


def eager_generator(func):
    """forces a generator to evaluate the code leading up to the first yield. this is needed to detect missing files or broken connections"""
    import itertools

    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        # Force the generator to run until first yield
        try:
            first = next(gen)
        except StopIteration:
            return gen  # empty generator
        except Exception as e:
            raise e
        # Return a new generator that yields first, then the rest
        return itertools.chain([first], gen)

    return wrapper


class SourceOpener:
    def __init__(
        self,
        folder: Path | None = None,
        engine: sqlalchemy.engine.Engine | str | None = None,
    ) -> None:
        self._folder = folder

        if self._folder is not None:
            if not self._folder.is_dir():
                raise SourceFolderMissingException(self)

        if engine is None:
            self._engine = None
        elif isinstance(engine, str):
            self._engine = sqlalchemy.create_engine(engine)
        else:
            self._engine = engine

    def load(self, tablename: str, csv: Path):
        """load a csv file into a database"""

        assert self._engine is not None

        csvr = self.open_csv(csv)
        column_names = next(csvr)

        # Create the table in the database
        metadata = MetaData()
        table = Table(
            tablename, metadata, *([Column(name, String(255)) for name in column_names])
        )
        metadata.create_all(self._engine, tables=[table])

        records = [dict(zip(column_names, row)) for row in csvr]

        # Insert rows
        with self._engine.begin() as conn:
            conn.execute(insert(table), records)

    @eager_generator
    def open(self, name: str):
        assert name.endswith(".csv")

        assert (self._folder is None) or (self._engine is None)
        assert (self._folder is not None) or (self._engine is not None)

        if self._folder is not None:
            src = self.open_csv(name)
        else:
            src = self.open_sql(name)

        for i in src:
            yield i

    def open_csv(self, name: str | Path):
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

    def open_sql(self, name: str):
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
