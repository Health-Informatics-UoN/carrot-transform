import datetime
import re
from pathlib import Path

import csvrow
import pytest

import carrottransform.tools.date_helpers as date_helpers
import tests.click_tools as click_tools


@pytest.mark.unit
@pytest.mark.parametrize(
    "expected, source",
    [
        ("2025-01-19 12:32:57", "2025-01-19 12:32:57"),
        ("2022-03-29 13:43:00", "2022-03-29 13:43"),
        ("2781-03-21 09:17:01", "2781-03-21 09:17:01.218347"),
        ("1986-04-12 00:00:00", "12-04-1986"),  # DD-MM-YYYY
        ("1786-05-23 00:00:00", "23/05/1786"),
        ("1972-12-23 00:00:00", "1972/12/23"),
        ("2024-07-05 08:45:30", "05-07-2024 08:45:30"),  # DD-MM-YYYY hh:mm:ss
        ("1999-11-30 14:22:10", "30/11/1999 14:22:10"),  # DD/MM/YYYY hh:mm:ss
        ("2030-02-14 20:05:45", "2030/02/14 20:05:45"),  # YYYY/MM/DD hh:mm:ss
        (Exception("invalid date format item='christmas 2024'"), "christmas 2024"),  #
    ],
)
def test_normalise_to8601(expected: str | Exception, source: str) -> None:

    if isinstance(expected, str):

        # first - do a sanity check to be sure that the value can pass through without being wrecked
        sanity = date_helpers.normalise_to8601(expected)
        assert (
            expected == sanity
        ), f"normalise_to8601() {expected=}) can't be loaded to itself"

        # now, check that the RHS value normalises to the LHS
        actual = date_helpers.normalise_to8601(source)
        assert (
            expected == actual
        ), f"normalise_to8601({source=}) -> {actual=} != {expected=}"
    else:
        assert isinstance(expected, Exception)

        caught: None | Exception = None
        try:
            date_helpers.normalise_to8601(source)
        except Exception as e:
            caught = e

        assert caught is not None

        assert str(caught) == str(expected)


@pytest.mark.unit
@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(False, id="date-time with CSV source"),
        pytest.param(True, id="date-time with SQL source"),
    ],
)
def test_dateimes_in_persons(tmp_path: Path, engine):
    (result, output, person_id_source2target, person_id_target2source) = (
        click_tools.click_test(
            tmp_path=tmp_path,
            person_file="/examples/test/inputs/Demographics.csv",
            rules_file="/examples/test/rules/rules_14June2021.json",
            engine=engine,
        )
    )

    # get the target2source mapping
    [_, t2s] = csvrow.back_get(output / "person_ids.tsv")
    s_people = csvrow.csv2dict(
        click_tools.package_root / "examples/test/inputs/Demographics.csv",
        lambda p: p.PersonID,
    )

    ##
    # check the person.tsv created by the above steps
    people = list(csvrow.csv_rows(output / "person.tsv", "\t"))
    assert 0 != len(people)
    for person in people:
        ##
        # concat the birtdatetime
        concat_birthdate = str(person.year_of_birth)
        concat_birthdate += "-"
        concat_birthdate += str(person.month_of_birth).rjust(2, "0")
        concat_birthdate += "-"
        concat_birthdate += str(person.day_of_birth).rjust(2, "0")

        assert person.birth_datetime.startswith(
            concat_birthdate
        ), f"{person.birth_datetime=} shoudl start with {concat_birthdate=}"
        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", person.birth_datetime
        ), f"{person.birth_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS` {tmp_path=}"

        s_person_id = t2s[person.person_id]
        s_person = s_people[s_person_id]

        n_s_date_of_birth = date_helpers.normalise_to8601(s_person.date_of_birth)

        assert n_s_date_of_birth == person.birth_datetime


@pytest.mark.unit
@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(False, id="date-time with CSV source"),
        pytest.param(True, id="date-time with SQL source"),
    ],
)
def test_dateimes_in_observation(tmp_path: Path, engine: bool):
    (result, output, person_id_source2target, person_id_target2source) = (
        click_tools.click_test(
            tmp_path=tmp_path,
            person_file="/examples/test/inputs/Demographics.csv",
            rules_file="/examples/test/rules/rules_14June2021.json",
            engine=engine,
        )
    )

    ##
    # check the observation.tsv created by the above steps
    observations = list(csvrow.csv_rows(output / "observation.tsv", "\t"))
    assert 0 != len(observations)
    for observation in observations:
        assert (
            observation.observation_date == observation.observation_datetime[:10]
        ), f"expected {observation.observation_datetime[:10]=} to be {observation.observation_date=}"

        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2}", observation.observation_date
        ), f"{observation.observation_date=} is the wrong format, it should be `YYYY-MM-DD` {tmp_path=}"

        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", observation.observation_datetime
        ), f"{observation.observation_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS` {tmp_path=}"


@pytest.mark.unit
@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(False, id="date-time with CSV source"),
        pytest.param(True, id="date-time with SQL source"),
    ],
)
def test_dateimes_in_measurement(tmp_path: Path, engine: bool):
    (result, output, person_id_source2target, person_id_target2source) = (
        click_tools.click_test(
            tmp_path=tmp_path,
            person_file="/examples/test/inputs/Demographics.csv",
            rules_file="/examples/test/rules/rules_14June2021.json",
            engine=engine,
        )
    )

    #
    # check the measurement.tsv created by the above steps
    measurements = list(csvrow.csv_rows(output / "measurement.tsv", "\t"))
    assert 0 != len(measurements)
    for measurement in measurements:
        assert (
            measurement.measurement_date == measurement.measurement_datetime[:10]
        ), f"expected {measurement.measurement_date[:10]=} to be {measurement.measurement_datetime=}"

        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2}", measurement.measurement_date
        ), f"{measurement.measurement_date=} is the wrong format, it should be `YYYY-MM-DD` {tmp_path=}"

        assert re.fullmatch(
            r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", measurement.measurement_datetime
        ), f"{measurement.measurement_datetime=} is the wrong format, it should be `YYYY-MM-DD HH:MM:SS` {tmp_path=}"


@pytest.mark.unit
def test_rever_iso():
    source = "15-06-1987"

    expected = datetime.datetime.strptime(source, "%d-%m-%Y")

    ###
    ## act

    actual = date_helpers.get_datetime_value(source)

    ###
    ## assert

    assert actual == expected


@pytest.mark.unit
def test_non_fotrmate():
    source = "15 of august 1985"

    ###
    ## act
    actual = date_helpers.get_datetime_value(source)

    ###
    ## assert

    assert actual is None


@pytest.mark.unit
def test_normalise_junk_time():
    source = "2023-09-27 the_morning"

    ###
    ## act

    actual = date_helpers.normalise_to8601(source)

    ###
    ## assert

    assert actual == "2023-09-27 00:00:00"


@pytest.mark.unit
def test_normalise_only_hours():
    source = "2023-09-27 12:12"

    ###
    ## act

    actual = date_helpers.normalise_to8601(source)

    ###
    ## assert

    assert actual == "2023-09-27 12:12:00"
