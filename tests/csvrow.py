
import csv

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

            # Remove extra spaces from field names and values
            yield Row(
                {
                    key.strip(): value.strip()
                    for key, value in row.items()
                    if "" != key.strip()
                }
            )

