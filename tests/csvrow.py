import csv
from pathlib import Path

from tests.testools import package_root, project_root
def back_get(person_ids):
    assert person_ids.is_file()

    with open(person_ids) as file:
        lines = file.readlines()
        lines = lines[1:]

        s2t = {}
        t2s = {}

        expected_id = 0

        for line_full in lines:
            line = line_full.strip().split("\t")

            source_id = line[0]
            target_id = line[1]
            expected_id += 1

            if str(int(target_id)) != str(expected_id):
                raise Exception(
                    "unexpected format or counting error with the person_ids"
                )

            assert target_id not in t2s
            assert source_id not in s2t

            t2s[target_id] = source_id
            s2t[source_id] = target_id

        return [s2t, t2s]


def csv2dict(path, key, delimiter=","):
    """converts a .csv (or .tsv) into a dictionary using key:() -> to detrmine key"""

    out = {}
    for row in csv_rows(path, delimiter):
        k = key(row)
        assert k not in out, f"{path=} {k=}"
        out[k] = row

    return out


def row_count(path: Path, delimiter: str = ",") -> int:
    count: int = 0
    for _ in csv_rows(path, delimiter):
        count += 1
    return count


def csv_rows(path, delimiter=","):
    """converts each row of a .csv (or .tsv) into an object (not a dictionary) for comparisons and such"""

    with open(path, newline="") as csvfile:
        reader = csv.DictReader(csvfile, delimiter=delimiter)
        for row in reader:

            class Row:
                def __init__(self, data):
                    self.__dict__.update(data)

                def __str__(self):
                    return str(self.__dict__)

                def __repr__(self):
                    return self.__str__()

            # Remove extra spaces from field names and values
            yield Row(
                {
                    key.strip(): value.strip()
                    for key, value in row.items()
                    if "" != key.strip()
                }
            )
