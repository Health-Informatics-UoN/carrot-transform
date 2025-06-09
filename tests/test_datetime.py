import pytest
import logging

from pathlib import Path

import clicktools
import csvrow
import re
import carrottransform.cli.subcommands.run as run


@pytest.mark.unit
def test_normalise_to8601():
    test_data = [
        ["2025-01-19 12:32:57", "2025-01-19 12:32:57"],
        ["2022-03-29 13:43:00", "2022-03-29 13:43"],
        ["2781-03-21 09:17:01", "2781-03-21 09:17:01.218347"],
        ["1986-04-12 00:00:00", "12-04-1986"],
        ["1786-05-23 00:00:00", "23/05/1786"],
        ["1972-12-23 00:00:00", "1972/12/23"],
        ["2024-07-05 08:45:30", "05-07-2024 08:45:30"],  # DD-MM-YYYY hh:mm:ss
        ["1999-11-30 14:22:10", "30/11/1999 14:22:10"],  # DD/MM/YYYY hh:mm:ss
        ["2030-02-14 20:05:45", "2030/02/14 20:05:45"],   # YYYY/MM/DD hh:mm:ss
    ]

    for [e, s] in test_data:
        a = run.normalise_to8601(s)
        assert e == a, f"normalise_to8601({s=}) -> {a=} != {e=}"


@pytest.mark.unit
def test_dateimes_in_persons(tmp_path: Path, caplog):
    # capture all and run the transformation
    caplog.set_level(logging.DEBUG)
    clicktools.click_transform(tmp_path, limit=10)

    # get the target2source mapping
    [_, t2s] = csvrow.back_get(tmp_path / "out/person_ids.tsv")

    s_people = csvrow.csv2dict(tmp_path / "Demographics.csv", lambda p: p.PersonID)

    ##
    # check the person.tsv created by the above steps
    people = list(csvrow.csv_rows(tmp_path / "out/person.tsv", "\t"))
    assert 0 != len(people)
    for person in people:
        ##
        # concat the birtdatetime
        concat_birthdate = str(person.year_of_birth)
        concat_birthdate += "-"
        concat_birthdate += str(person.month_of_birth).rjust(2, "0")
        concat_birthdate += "-"
        concat_birthdate += str(person.day_of_birth).rjust(2, "0")

        assert person.birth_datetime.startswith(concat_birthdate), (
            f"{person.birth_datetime=} shoudl start with {concat_birthdate=}"
        )
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", person.birth_datetime
        ), (
            f"{person.birth_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS` {tmp_path=}"
        )

        s_person_id = t2s[person.person_id]
        s_person = s_people[s_person_id]

        n_s_date_of_birth = run.normalise_to8601(s_person.date_of_birth)

        assert n_s_date_of_birth == person.birth_datetime


@pytest.mark.unit
def test_dateimes_in_observation(tmp_path: Path, caplog):
    # capture all and run the transformation
    caplog.set_level(logging.DEBUG)
    clicktools.click_transform(tmp_path, limit=10)

    ##
    # check the observation.tsv created by the above steps
    observations = list(csvrow.csv_rows(tmp_path / "out/observation.tsv", "\t"))
    assert 0 != len(observations)
    for observation in observations:
        assert observation.observation_date == observation.observation_datetime[:10], (
            f"expected {observation.observation_datetime[:10]=} to be {observation.observation_date=}"
        )

        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", observation.observation_date), (
            f"{observation.observation_date=} is the wrong format, it should be `YYYY-MM-DD` {tmp_path=}"
        )

        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", observation.observation_datetime
        ), (
            f"{observation.observation_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS` {tmp_path=}"
        )


@pytest.mark.unit
def test_dateimes_in_measurement(tmp_path: Path, caplog):
    # capture all and run the transformation
    caplog.set_level(logging.DEBUG)
    clicktools.click_transform(tmp_path, limit=10)

    #
    # check the measurement.tsv created by the above steps
    measurements = list(csvrow.csv_rows(tmp_path / "out/measurement.tsv", "\t"))
    assert 0 != len(measurements)
    for measurement in measurements:
        assert measurement.measurement_date == measurement.measurement_datetime[:10], (
            f"expected {measurement.measurement_date[:10]=} to be {measurement.measurement_datetime=}"
        )

        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", measurement.measurement_date), (
            f"{measurement.measurement_date=} is the wrong format, it should be `YYYY-MM-DD` {tmp_path=}"
        )

        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", measurement.measurement_datetime
        ), (
            f"{measurement.measurement_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS` {tmp_path=}"
        )
