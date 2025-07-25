"""
these are various integration tests for carroti using pytest and the inbuild click tools

"""

import pytest

from pathlib import Path

import logging

import clicktools
import csvrow


import re

###
# tests for https://github.com/Health-Informatics-UoN/carrot-transform/issues/83


# end of tests for https://github.com/Health-Informatics-UoN/carrot-transform/issues/83
##


@pytest.mark.integration
def test_integration_test1(tmp_path: Path):
    # this test (is one of the ones that) needs to read the person file as part of verification
    person_file = Path(__file__).parent / "test_data/integration_test1/src_PERSON.csv"

    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_generic(
            tmp_path,
            person_file,
        )
    )

    assert 4 == len(person_id_target2source)
    assert 4 == len(person_id_source2target)

    ##
    # load the src_PERSON.csv
    src_persons = csvrow.csv2dict(person_file, lambda src: src.person_id)

    # load the weight rows
    src_weight_data = csvrow.csv2dict(
        person_file.parent / "src_WEIGHT.csv",
        lambda src: f"{src.person_id}#{src.measurement_date}",
    )
    src_weight_seen = set()

    ##
    # check the birthdays and gender
    seen_ids = []
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        # check the ids
        assert person.person_id in person_id_target2source
        tid = person_id_target2source[person.person_id]
        assert tid not in seen_ids
        seen_ids.append(tid)

        # locate the source record
        src_person = src_persons[person_id_target2source[person.person_id]]

        ##
        # check the birtdatetime
        concat_birthdate = (
            person.year_of_birth
            + "-"
            + str(person.month_of_birth).rjust(2, "0")
            + "-"
            + str(person.day_of_birth).rjust(2, "0")
        )
        assert_datetimes(
            expected=src_person.birth_datetime,
            datetime=person.birth_datetime,
            onlydate=concat_birthdate,
        )

        # check that the gender is correct
        assert src_person.gender_source_value == person.gender_source_value
        if person.gender_source_value == "male":
            assert int(person.gender_concept_id) == 8507
        elif person.gender_source_value == "female" or (
            # this misspelling is intentional. the misspelling is in the test-data, and test-rules; not the shipped code.
            # carrot doesn't do any spell checking
            "femail" == person.gender_source_value
        ):
            assert 8532 == int(person.gender_concept_id)
        else:
            raise Exception(
                f"unknown gender_source_value `{person.gender_source_value}`"
            )
    assert "321" in seen_ids
    assert "789345" in seen_ids
    assert "6789" in seen_ids
    # assert "289" in seen_ids, "this id was unused but should still be in there"
    # this is covered in another issue - and - we need to merge

    ##
    # check measurements
    for measurement in csvrow.csv_rows(output / "measurement.tsv", "\t"):
        # check the 35811769 value we're using for weight
        if measurement.measurement_concept_id != "35811769":
            raise Exception(
                f"unexpected {measurement.measurement_concept_id=} in {measurement=}"
            )
        else:
            ##
            # "standard" checks for measurements? maybe?
            assert measurement.person_id in person_id_target2source
            key = f"{person_id_target2source[measurement.person_id]}#{measurement.measurement_date}"
            assert key in src_weight_data
            assert key not in src_weight_seen
            src = src_weight_data[key]
            src_weight_seen.add(key)

            ##
            # bespoke checks here

            assert_datetimes(
                expected=src.measurement_date,
                datetime=measurement.measurement_datetime,
                onlydate=measurement.measurement_date,
            )

            assert "0" == measurement.measurement_type_concept_id
            assert "" == measurement.operator_concept_id
            assert src.body_kgs == measurement.value_as_number
            assert "" == measurement.value_as_concept_id
            assert "" == measurement.unit_concept_id
            assert "" == measurement.range_low
            assert "" == measurement.range_high
            assert "" == measurement.provider_id
            assert "" == measurement.visit_occurrence_id
            assert "" == measurement.visit_detail_id
            assert src.body_kgs == measurement.measurement_source_value
            assert "35811769" == measurement.measurement_source_concept_id
            assert "" == measurement.unit_source_value
            assert "" == measurement.value_source_value

    assert len(src_weight_data) == len(src_weight_seen)

    ##
    # check the observation
    for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
        print(observation.observation_concept_id)

        assert "0" == observation.observation_type_concept_id
        assert "" == observation.value_as_number
        assert "" == observation.value_as_concept_id
        assert "" == observation.qualifier_concept_id
        assert "" == observation.unit_concept_id
        assert "" == observation.provider_id
        assert "" == observation.visit_occurrence_id
        assert "" == observation.visit_detail_id
        assert "" == observation.unit_source_value
        assert "" == observation.qualifier_source_value
        assert observation.value_as_string == observation.observation_source_value

        if observation.observation_concept_id == "35810208":
            assert_datetimes(
                expected="2025-05-21 15:02",
                datetime=observation.observation_datetime,
                onlydate=observation.observation_date,
            )

            assert "CURRENT_SMOKER" == observation.value_as_string
            assert "789345" == person_id_target2source[observation.person_id]

        elif observation.observation_concept_id == "35810209":
            assert_datetimes(
                expected="2025-05-12 21:20",
                datetime=observation.observation_datetime,
                onlydate=observation.observation_date,
            )

            assert "FORMER_SMOKER" == observation.value_as_string
            assert "6789" == person_id_target2source[observation.person_id]

        elif observation.observation_concept_id == "35821355":
            assert_datetimes(
                expected="2025-05-22 03:14",
                datetime=observation.observation_datetime,
                onlydate=observation.observation_date,
            )

            assert "NEVER_SMOKER" == observation.value_as_string
            assert "321" == person_id_target2source[observation.person_id]
        else:
            raise Exception(
                f"Unexpected observation.observation_concept_id `{observation.observation_concept_id}`"
            )


@pytest.mark.integration
def test_floats(tmp_path: Path):
    """
    checks if floats are mapped correctly

    looks like they're left as-is when they're a whole number
    """

    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_generic(
            tmp_path,
            "floats/src_PERSON.csv",
        )
    )

    assert 4 == len(person_id_source2target)
    assert 4 == len(person_id_target2source)

    for measurement in csvrow.csv_rows(output / "measurement.tsv", "\t"):
        assert "35811769" == measurement.measurement_concept_id

        source_day = measurement.measurement_date
        source_id = int(person_id_target2source[measurement.person_id])

        if source_id == 6789:
            if source_day == "2023-10-12":
                assert "7532.1" == measurement.value_as_number
            elif source_day == "2023-10-11":
                assert "76.0" == measurement.value_as_number
            elif source_day == "2023-11-21":
                assert "86.123" == measurement.value_as_number
            else:
                raise Exception(f"bad value for 6789 {source_day=}, {measurement=}")
        elif source_id == 321:
            assert "2025-01-03" == source_day
            assert "23" == measurement.value_as_number
        else:
            raise Exception(f"bad value, {source_id=}, {source_day=}, {measurement=}")


@pytest.mark.integration
def test_duplications(tmp_path: Path):
    """
    checks if duplications are handled correctly. it duplicates a person and observations to see if each ios handled correctly
    """

    # this test needs to read the person file as part of verification
    person_file = Path(__file__).parent / "test_data/duplications/src_PERSON.csv"

    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_generic(
            tmp_path,
            person_file,
        )
    )

    assert 3 == len(person_id_source2target)
    assert 3 == len(person_id_target2source)

    # load and chcekc the output person(s)
    person_counts = {}
    people = {}
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        src_person_id = person_id_target2source[person.person_id]

        if src_person_id not in people:
            people[src_person_id] = person

            person_counts[src_person_id] = 1
        else:
            person_counts[src_person_id] += 1

    assert 3 == len(person_counts)
    assert 2 == person_counts["321"]  # there should be a single one here
    assert 1 == person_counts["789345"]
    assert 1 == person_counts["6789"]

    # load all of the src_PERSON.csv
    for src_person in csvrow.csv_rows(person_file):
        assert person_counts[src_person.person_id] > 0
        person_counts[src_person.person_id] -= 1

        person = people[src_person.person_id]

        ##
        # now perform the old checks

        # check the birtdatetime
        concat_birthdate = (
            person.year_of_birth
            + "-"
            + str(person.month_of_birth).rjust(2, "0")
            + "-"
            + str(person.day_of_birth).rjust(2, "0")
        )
        assert_datetimes(
            expected=src_person.birth_datetime,
            datetime=person.birth_datetime,
            onlydate=concat_birthdate,
        )

        # check that the gender is correct
        assert src_person.gender_source_value == person.gender_source_value
        if person.gender_source_value == "male":
            assert 8507 == int(person.gender_concept_id)
        elif person.gender_source_value == "female" or (
            # this misspelling is intentional. the misspelling is in the test-data, and test-rules; not the shipped code.
            # carrot doesn't do any spell checking
            "femail" == person.gender_source_value
        ):
            assert 8532 == int(person.gender_concept_id)
        else:
            raise Exception(
                f"unknown gender_source_value `{person.gender_source_value}`"
            )

    # check again to be sure all entries were visitied
    for k in person_counts:
        assert 0 == person_counts[k]

    for measurement in csvrow.csv_rows(output / "measurement.tsv", "\t"):
        person = people[person_id_target2source[measurement.person_id]]

        src_person_id = person_id_target2source[measurement.person_id]

        assert "35811769" == measurement.measurement_concept_id

        # this shouldn't be blank - but that needs a config change
        # https://github.com/Health-Informatics-UoN/carrot-transform/issues/89
        assert "" == measurement.measurement_time

        assert "0" == measurement.measurement_type_concept_id
        assert "" == measurement.value_as_concept_id
        assert "" == measurement.unit_concept_id
        assert "" == measurement.range_low
        assert "" == measurement.range_high
        assert "" == measurement.provider_id
        assert "" == measurement.visit_occurrence_id
        assert "" == measurement.visit_detail_id
        assert "" == measurement.operator_concept_id
        assert "35811769" == measurement.measurement_source_concept_id
        assert "" == measurement.unit_source_value
        assert "" == measurement.value_source_value
        assert measurement.measurement_date == measurement.measurement_datetime[:10]
        assert measurement.value_as_number == measurement.measurement_source_value

        if src_person_id == "321":
            assert "7" == measurement.measurement_id
            assert "2025-01-03 00:00:00" == measurement.measurement_datetime
            assert "23" == measurement.value_as_number

        elif src_person_id == "6789":
            if measurement.measurement_id == "1":
                assert "2023-10-12 00:00:00" == measurement.measurement_datetime
                assert "75" == measurement.value_as_number

            elif measurement.measurement_id == "2":
                assert "2023-10-11 00:00:00" == measurement.measurement_datetime
                assert "76" == measurement.value_as_number

            elif measurement.measurement_id == "3":
                assert "2023-11-21 00:00:00" == measurement.measurement_datetime
                assert "86" == measurement.value_as_number

            elif measurement.measurement_id == "4":
                assert "2023-11-21 00:00:00" == measurement.measurement_datetime
                assert "68" == measurement.value_as_number

            elif measurement.measurement_id == "5":
                assert "2023-11-21 00:00:00" == measurement.measurement_datetime
                assert "86" == measurement.value_as_number

            elif measurement.measurement_id == "6":
                assert "2023-11-21 00:00:00" == measurement.measurement_datetime
                assert "86" == measurement.value_as_number

            else:
                raise Exception(f"unexpected measurement {measurement=}")
        else:
            raise Exception("unexpected ")

    ##
    # set some expectations
    # ... should try to follow this pattern in the future
    expect = {
        "789345": {
            "2025-05-21 15:02:00": {"35810208": "CURRENT_SMOKER"},
        },
        "6789": {"2025-05-12 21:20:00": {"35810209": "FORMER_SMOKER"}},
        "321": {
            "2025-05-22 03:14:00": {"35821355": "NEVER_SMOKER"},
            "2025-05-22 02:14:00": {"35821355": "NEVER_SMOKER"},
        },
    }

    ##
    # now check the observations
    observation_count = {}  # count how many observations per psersn
    for k in person_id_source2target:
        observation_count[k] = 0
    for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
        person = people[person_id_target2source[observation.person_id]]

        src_person_id = person_id_target2source[observation.person_id]
        observation_count[src_person_id] += 1

        assert "0" == observation.observation_type_concept_id
        assert "" == observation.value_as_number
        assert observation.observation_date == observation.observation_datetime[:10]
        assert "" == observation.value_as_concept_id
        assert "" == observation.qualifier_concept_id
        assert "" == observation.unit_concept_id
        assert "" == observation.provider_id
        assert "" == observation.visit_occurrence_id
        assert "" == observation.visit_detail_id
        assert "" == observation.unit_source_value
        assert "" == observation.qualifier_source_value
        assert (
            observation.observation_concept_id
            == observation.observation_source_concept_id
        )
        assert observation.value_as_string == observation.observation_source_value

        assert src_person_id in expect
        assert observation.observation_datetime in expect[src_person_id]
        assert (
            observation.observation_concept_id
            in expect[src_person_id][observation.observation_datetime]
        )
        assert (
            observation.value_as_string
            == expect[src_person_id][observation.observation_datetime][
                observation.observation_concept_id
            ]
        )

    assert 1 == observation_count["789345"]
    assert 1 == observation_count["6789"]
    assert 5 == observation_count["321"]


@pytest.mark.integration
def test_mapping_person(tmp_path: Path):
    """test to see if basic person records map as expected"""
    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_generic(
            tmp_path,
            "mapping_person/demos.csv",
        )
    )

    assert 2 == len(person_id_source2target)
    assert 2 == len(person_id_target2source)

    # check the birthdateime and gender
    seen_ids: list[int] = []
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        # check the ids
        assert person.person_id in person_id_target2source
        src_person_id = int(person_id_target2source[person.person_id])
        assert src_person_id not in seen_ids
        seen_ids.append(src_person_id)

        # dataset is small so just check it like this
        if src_person_id == 28:
            assert "8532" == person.gender_concept_id
            assert "2020-09-12 00:00:00" == person.birth_datetime
        elif src_person_id == 82:
            assert "8507" == person.gender_concept_id
            assert "2020-09-11 00:00:00" == person.birth_datetime
        else:
            raise Exception(f"unexpected {src_person_id=} in {person=}")
    assert 28 in seen_ids
    assert 82 in seen_ids

    # ethnicity is an observation
    for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
        assert observation.person_id in person_id_target2source
        src_person_id = int(person_id_target2source[observation.person_id])
        if src_person_id == 28:
            assert "40618782" == observation.observation_concept_id
            assert "2020-09-12" == observation.observation_date
        elif src_person_id == 82:
            assert "40637268" == observation.observation_concept_id
            assert "2020-09-11" == observation.observation_date
        else:
            raise Exception(f"unexpected {src_person_id=} in {observation=}")


@pytest.mark.integration
def test_observe_smoking(tmp_path: Path):
    """
    this test checks to see if the smoking observations map correctly
    """
    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_generic(
            tmp_path,
            "observe_smoking/demos.csv",
        )
    )

    assert 4 == len(person_id_source2target)
    assert 4 == len(person_id_target2source)

    # declare the expectations
    # ... it's a lot nicer to declare it like this
    expect = {
        123: {
            "2018-01-01": {"3959110": "active"},
            "2018-02-01": {"3959110": "active"},
            "2018-03-01": {"3957361": "quit"},
            "2018-04-01": {"3959110": "active"},
            "2018-05-01": {"35821355": "never"},
        },
        456: {
            "2009-01-01": {"35821355": "never"},
            "2009-02-01": {"35821355": "never"},
            "2009-03-01": {"3957361": "quit"},
        },
    }

    # check the smoking state in observations
    observations_seen: int = 0
    for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
        observations_seen += 1

        assert "0" == observation.observation_type_concept_id
        assert "" == observation.value_as_number
        assert observation.observation_date == observation.observation_datetime[:10]
        assert "" == observation.value_as_concept_id
        assert "" == observation.qualifier_concept_id
        assert "" == observation.unit_concept_id
        assert "" == observation.provider_id
        assert "" == observation.visit_occurrence_id
        assert "" == observation.visit_detail_id
        assert "" == observation.unit_source_value
        assert "" == observation.qualifier_source_value
        assert (
            observation.observation_concept_id
            == observation.observation_source_concept_id
        )
        assert observation.value_as_string == observation.observation_source_value

        assert observation.person_id in person_id_target2source
        src_person_id = int(person_id_target2source[observation.person_id])

        observation_date = observation.observation_date
        observation_concept_id = observation.observation_concept_id
        observation_source_value = observation.observation_source_value

        assert src_person_id in expect, observation
        assert observation_date in expect[src_person_id], observation
        assert observation_concept_id in expect[src_person_id][observation_date], (
            observation
        )

        assert (
            observation_source_value
            == expect[src_person_id][observation_date][observation_concept_id]
        ), observation

    # check to be sure we saw all the observations
    assert 8 == observations_seen, "expected 8 observations, got %d" % observations_seen


@pytest.mark.integration
def test_measure_weight_height(tmp_path: Path):
    """
    this test checks to be sure that two measurements (width and height) don't "collide" or interfere with eachother
    """

    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_generic(
            tmp_path,
            "measure_weight_height/persons.csv",
        )
    )

    assert 4 == len(person_id_source2target)
    assert 4 == len(person_id_target2source)

    # this is the data we're expecting to see
    height = 903133
    weight = 903121
    expect = {
        21: {
            "2021-12-02": {height: 123, weight: 31},
            "2021-12-01": {height: 122},
            "2021-12-03": {height: 12, weight: 12},
            "2022-12-01": {weight: 28},
        },
        81: {
            "2022-12-02": {height: 23, weight: 27},
            "2021-03-01": {height: 92},
            "2020-03-01": {weight: 92},
        },
        91: {
            "2021-02-03": {height: 72, weight: 12},
            "2021-02-01": {weight: 1},
        },
    }

    # validate the results
    measurements: int = 0
    for measurement in csvrow.csv_rows(output / "measurement.tsv", "\t"):
        measurements += 1

        src_person_id = int(person_id_target2source[measurement.person_id])
        date = measurement.measurement_date
        concept = int(measurement.measurement_concept_id)
        value = int(measurement.value_as_number)

        assert src_person_id in expect, f"{src_person_id=} {measurement=} "
        assert date in expect[src_person_id], f"{src_person_id=} {date=} {measurement=}"
        assert concept in expect[src_person_id][date], (
            f"{src_person_id=} {date=} {concept=} {measurement=}"
        )

        assert value == expect[src_person_id][date][concept], (
            f"{date=} {concept=} {measurement=}"
        )

    assert 13 == measurements


@pytest.mark.integration
def test_condition(tmp_path: Path):
    """
    this checks that values are sent to the condition tsv as expected
    """
    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_generic(
            tmp_path,
            "condition/persons.csv",
        )
    )

    assert 4 == len(person_id_source2target)
    assert 4 == len(person_id_target2source)

    expect = {
        person_id_source2target["81"]: {
            "1998-02-01": {4227224: 1},
            "1998-02-03": {4227224: 13},
        },
        person_id_source2target["91"]: {
            "2001-01-03": {4227224: 1},
            "2001-01-05": {4227224: 7},
        },
    }

    # validate the results
    occurrences: int = 0
    for occurrence in csvrow.csv_rows(output / "condition_occurrence.tsv", "\t"):
        occurrences += 1

        person_id = occurrence.person_id
        date = occurrence.condition_start_datetime
        concept = int(occurrence.condition_concept_id)
        assert str(concept) == occurrence.condition_concept_id
        value = int(occurrence.condition_source_value)
        assert str(value) == occurrence.condition_source_value

        assert "" == occurrence.condition_start_date

        # there's a known shortcoming of the conditions that make them act like observations
        # https://github.com/Health-Informatics-UoN/carrot-transform/issues/88
        assert date == occurrence.condition_end_datetime
        date = date[:10]

        assert occurrence.condition_end_date == date

        assert person_id in expect
        assert date in expect[occurrence.person_id]
        assert concept in expect[occurrence.person_id][date]
        assert value == expect[occurrence.person_id][date][concept]

    assert 4 == occurrences


@pytest.mark.integration
def test_mireda_key_error(tmp_path: Path, caplog):
    """this is the oprignal buggy version that should trigger the key error"""


    # capture all
    caplog.set_level(logging.DEBUG)

    person_file = (
        Path(__file__).parent
        / "test_data/mireda_key_error/demographics_mother_gold.csv"
    )
    (result, output) = clicktools.click_generic(
        tmp_path,
        person_file,
        failure=True,
    )

    assert result.exit_code == -1

    message = caplog.text.splitlines(keepends=False)[-1]

    assert message.strip().endswith(
        "Person properties were mapped from ({'infant_data_gold.csv', 'demographics_child_gold.csv'}) but can only come from the person file person_file.name='demographics_mother_gold.csv'"
    )

    assert '-1' == str(result.exception)


def assert_datetimes(onlydate: str, datetime: str, expected: str):
    """
    this function performs date/time checking accounting for missing time info
    """
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", onlydate), (
        f"onlydate {onlydate=} is the wrong format"
    )
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", datetime), (
        f"datetime {datetime=} is the wrong format"
    )

    assert datetime[:10] == onlydate

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", expected):
        assert expected == onlydate
        assert datetime == f"{onlydate} 00:00:00"

    elif re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", expected):
        assert expected[:10] == onlydate
        assert datetime == f"{expected}:00"

    else:
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", expected), (
            f"the source data {expected=} is in the wrong format"
        )
        assert expected == datetime
