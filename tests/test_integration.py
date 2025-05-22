"""
these test using carrot-transform end-to-end using fake spreadsheets and a rules.json

currently it's only checking birthdate and gender

to add more fields;
- add them to the existing .csv or create new ones
- produce a scan report
- upload the scan report to carrot-mapper and set rules to map the data
- download the rules.json, update this test, ensure that the fields all still match
"""
import pytest
from carrottransform.cli.subcommands.run import *
import pytest
from unittest.mock import patch
import importlib.resources
import logging

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream


from carrottransform.cli.subcommands.run import *
from pathlib import Path
from unittest.mock import patch

@pytest.mark.unit
def test_integration_test1(tmp_path: Path):

    # Get the package root directory
    package_root = importlib.resources.files("carrottransform")
    package_root = (
        package_root if isinstance(package_root, Path) else Path(str(package_root))
    )
    test_files = package_root.parent / "tests/test_data/integration_test1"

    ##
    # setup the args
    arg__input_dir = test_files
    arg__rules_file = test_files / 'Rules-intest6-301-2025-05-21-10_22_56.937314.json'
    arg__person_file = test_files / 'src_PERSON.csv'
    arg__output_dir = tmp_path

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--input-dir",
            f"{arg__input_dir}",
            "--rules-file",
            f"{arg__rules_file}",
            "--person-file",
            f"{arg__person_file}",
            "--output-dir",
            f"{arg__output_dir}",

            "--omop-ddl-file",
            f"@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
            "--omop-config-file",
            f"@carrot/config/omop.json",
        ],
    )

    # TODO; validate teh console output

    ##
    # load the src_PERSON.csv
    src_persons = {}
    for src_person in csv_rows(arg__person_file):
        src_persons[src_person.person_id] = src_person

    ### check data

    ##
    # backmap the ids.
    # source -> target
    # target -> source
    [s2t, t2s] = back_get(arg__output_dir / 'person_ids.tsv')

    ## check that the personid thing exists
    # ... we *could* map back from source<-target at some point

    ##
    # check the birdays
    for person in csv_rows(arg__output_dir / 'person.tsv', '\t'):

        # check the birth is internally consistent
        actual = person.year_of_birth + '-' + str(person.month_of_birth).rjust(2, '0') + '-' + str(person.day_of_birth).rjust(2, '0')
        assert actual == person.birth_datetime

        # check the ids
        assert person.person_id in t2s
        src_person = src_persons[t2s[person.person_id]]

        # check that the birth matches source ... somehow
        assert src_person.birth_datetime == person.birth_datetime

        # check that the gender is correct
        assert src_person.gender_source_value == person.gender_source_value

        if 'male' == person.gender_source_value:
            assert 8507 == int(person.gender_concept_id)
        elif 'female' == person.gender_source_value or 'femail' == person.gender_source_value:
            assert 8532 == int(person.gender_concept_id)
        else:
            raise Exception(f'unknown gender_source_value `{person.gender_source_value}`')

def back_get(person_ids):
    assert person_ids.is_file()

    with open(person_ids) as file:
        [head, line] = file.readlines()

        s2t = {}
        t2s = {}

        expected_id = 0

        while '' != line:
            source_id = ''
            target_id = ''
            expected_id += 1

            while '\t' != line[0]:
                source_id += line[0]
                line = line[1:]

            # drop the tab
            line = line[1:]

            target_id = str(expected_id)

            # check it is what we expect
            assert line.startswith(target_id)

            # remove the target id data
            line = line[len(target_id):]

            # save it
            # target_id = int(target_id)
            # source_id = int(source_id)

            assert target_id not in t2s
            assert source_id not in s2t

            t2s[target_id] = source_id
            s2t[source_id] = target_id
        
        return [s2t, t2s]

def csv_rows(path, delimiter=','):
    import csv
    with open(path, newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=delimiter)
        for row in reader:
            class Row():
                def __init__(self, data):
                    self.__dict__.update(data)
                def __str__(self):
                    return str(self.__dict__)
            # Remove extra spaces from field names and values
            yield Row({key.strip(): value.strip() for key, value in row.items() if '' != key.strip()})
