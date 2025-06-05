from carrottransform.cli.subcommands.run import *
import pytest
from unittest.mock import patch
import importlib.resources
import logging

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream
import clicktools
import csvrow
import re


# @pytest.mark.unit
# def test_normalise_to8601():
#     raise Exception(f"test {normalise_to8601=}")



@pytest.mark.unit
def test_dateimes_in_persons(tmp_path: Path, caplog):

    # capture all
    caplog.set_level(logging.DEBUG)

    clicktools.click_transform(tmp_path, limit= 10)

    ##
    # check the person.tsv created by the above steps
    people = list(csvrow.csv_rows(tmp_path / 'out/person.tsv', '\t'))
    assert 0 != len(people)
    for person in people:

        ##
        # concat the birtdatetime
        concat_birthdate = str(person.year_of_birth)
        concat_birthdate+= "-"
        concat_birthdate+= str(person.month_of_birth).rjust(2, "0")
        concat_birthdate+= "-"
        concat_birthdate+= str(person.day_of_birth).rjust(2, "0")

        assert person.birth_datetime.startswith(concat_birthdate), f"{person.birth_datetime=} shoudl start with {concat_birthdate=}"
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", person.birth_datetime
        ), f"{person.birth_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS` {tmp_path=}"

        raise Exception('??? verify that the date matches the source')


# @pytest.mark.unit
# def test_dateimes_in_observation(tmp_path: Path, caplog):
#     # capture all
#     caplog.set_level(logging.DEBUG)

#     clicktools.click_transform(tmp_path, limit= 10)

#     ##
#     # check the observation.tsv created by the above steps
#     observations = list(csvrow.csv_rows(tmp_path / 'out/observation.tsv', '\t'))
#     assert 0 != len(observations)
#     for observation in observations:
#         assert observation.observation_date == observation.observation_datetime[:10]

#         assert re.fullmatch(
#             r"\d{4}-\d{2}-\d{2}", observation.observation_date
#         ), f"{observation.observation_date=} is the wrong format, it should be `YYYY-MM-DD` {tmp_path=}"

#         assert re.fullmatch(
#             r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", observation.observation_datetime
#         ), f"{observation.observation_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS` {tmp_path=}"


# @pytest.mark.unit
# def test_dateimes_in_measurement(tmp_path: Path, caplog):
#     # capture all
#     caplog.set_level(logging.DEBUG)

#     clicktools.click_transform(tmp_path, limit= 10)

#     ##
#     # check the measurement.tsv created by the above steps
#     measurements = list(csvrow.csv_rows(tmp_path / 'out/measurement.tsv', '\t'))
#     assert 0 != len(measurements)
#     for measurement in measurements:
#         assert measurement.measurement_date == measurement.measurement_datetime[:10]

#         assert re.fullmatch(
#             r"\d{4}-\d{2}-\d{2}", measurement.measurement_date
#         ), f"{measurement.measurement_date=} is the wrong format, it should be `YYYY-MM-DD` {tmp_path=}"

#         assert re.fullmatch(
#             r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", measurement.measurement_datetime
#         ), f"{measurement.measurement_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS` {tmp_path=}"

# @pytest.mark.unit
# def test_dateimes_in_concept():
#     raise Exception('??? can concepts map to datetimes?')