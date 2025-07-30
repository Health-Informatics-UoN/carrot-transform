
from pathlib import Path
from typing import IO, Iterator, List, Optional
import csv
import logging
from pathlib import Path
from sqlalchemy import Table, Column, String, MetaData, insert, select
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)

def open_csv(file_path: Path) -> Iterator[list[str]] | None:
    """opens a file and does something related to CSVs"""
    try:
        fh = file_path.open(mode="r", encoding="utf-8-sig")
        csvr = csv.reader(fh)
        return csvr
    except IOError as e:
        logger.exception("Unable to open: {0}".format(file_path))
        logger.exception("I/O error({0}): {1}".format(e.errno, e.strerror))
        return None


def open_sql(query: str) -> Iterator[list[str]] | None:
    """opens an sql query. yields column names tehn all rows"""

    raise Exception("Not implemented")

import sqlalchemy

class SourceOpener():
    def __init__(self, input_dir: Path, engine: sqlalchemy.engine.Engine | str | None = None) -> None:
        self._input_dir = input_dir

        if engine is None:
            self._engine = None
        elif isinstance(engine, str):
            self._engine = sqlalchemy.create_engine(engine)
        else:
            self._engine = engine

    def load(self, csv: Path):
        """load a csv file into a database"""

        csvr = open_csv(csv)
        column_names = next(csvr)
 
        # Create the table in the database
        metadata = MetaData()
        table = Table(csv.name[:-4], metadata, *([Column(name, String(255)) for name in column_names]))
        metadata.create_all(self._engine, tables=[table])


        records = [dict(zip(column_names, row)) for row in csvr]

        # Insert rows
        with self._engine.begin() as conn: 
            conn.execute(insert(table), records)

    def open(self, name: str, sql: Path |None = None):
        csv: Path = self._input_dir / name
        key: str = csv.name[:-4]
        sql = sql if sql is not None else self._input_dir / (key + ".sql")

        if not sql.exists():
            csv = open_csv(csv)
            if csv is None:
                return None
            for row in csv:
                yield row
        else:
            assert self._engine is not None, "SQL file exists but no engine"

            

            metadata = MetaData()
            metadata.reflect(bind=self._engine, only=[key])
            table = metadata.tables[key]

            with self._engine.connect() as conn:
                result = conn.execute(select(table))
                yield result.keys()       # list of column names

                for row in result:
                    # we overwrite the date values so convert it to a list
                    yield list(row)
