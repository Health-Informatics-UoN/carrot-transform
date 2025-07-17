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
import importlib.resources
import re

from pathlib import Path


import clicktools
import csvrow


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
    arg__rules_file = test_files / "transform-rules.json"
    arg__person_file = test_files / "src_PERSON.csv"

    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["src_PERSON.csv", "src_SMOKING.csv", "src_WEIGHT.csv"],
        arg__input_dir,
        arg__rules_file,
    )

    assert 0 == result.exit_code

    ##
    # load the src_PERSON.csv
    src_persons = csvrow.csv2dict(arg__person_file, lambda src: src.person_id)

    # load the weight rows
    src_weight_data = csvrow.csv2dict(
        test_files / "src_WEIGHT.csv",
        lambda src: f"{src.person_id}#{src.measurement_date}",
    )
    src_weight_seen = set()

    ### check data

    ##
    # backmap the ids.
    # source -> target
    # target -> source
    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")

    assert 4 == len(s2t)

    ##
    # check the birthdays and gender
    seen_ids = []
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        # check the ids
        assert person.person_id in t2s
        tid = t2s[person.person_id]
        assert tid not in seen_ids
        seen_ids.append(tid)

        # locate the source record
        src_person = src_persons[t2s[person.person_id]]

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
        if "male" == person.gender_source_value:
            assert 8507 == int(person.gender_concept_id)
        elif "female" == person.gender_source_value or (
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
        if "35811769" != measurement.measurement_concept_id:
            raise Exception(
                f"unexpected {measurement.measurement_concept_id=} in {measurement=}"
            )
        else:
            ##
            # "standard" checks for measurements? maybe?
            assert measurement.person_id in t2s
            key = f"{t2s[measurement.person_id]}#{measurement.measurement_date}"
            assert key in src_weight_data
            assert key not in src_weight_seen
            src = src_weight_data[key]
            src_weight_seen.add(key)

            ## bespoke checks here

            ##
            # date/time is hard. the
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

        if "35810208" == observation.observation_concept_id:
            assert_datetimes(
                expected="2025-05-21 15:02",
                datetime=observation.observation_datetime,
                onlydate=observation.observation_date,
            )

            assert "CURRENT_SMOKER" == observation.value_as_string
            assert "789345" == t2s[observation.person_id]

        elif "35810209" == observation.observation_concept_id:
            assert_datetimes(
                expected="2025-05-12 21:20",
                datetime=observation.observation_datetime,
                onlydate=observation.observation_date,
            )

            assert "FORMER_SMOKER" == observation.value_as_string
            assert "6789" == t2s[observation.person_id]

        elif "35821355" == observation.observation_concept_id:
            assert_datetimes(
                expected="2025-05-22 03:14",
                datetime=observation.observation_datetime,
                onlydate=observation.observation_date,
            )

            assert "NEVER_SMOKER" == observation.value_as_string
            assert "321" == t2s[observation.person_id]
        else:
            raise Exception(
                f"Unexpected observation.observation_concept_id `{observation.observation_concept_id}`"
            )


@pytest.mark.unit
def test_floats(tmp_path: Path):
    """
    checks if floats are mapped correctly

    looks like they're left as-is when they're a whole number
    """
    # Get the package root directory
    package_root = importlib.resources.files("carrottransform")
    package_root = (
        package_root if isinstance(package_root, Path) else Path(str(package_root))
    )

    test_files = package_root.parent / "tests/test_data/test_floats"

    ##
    # setup the args
    arg__input_dir = test_files
    arg__rules_file = test_files / "rules.json"

    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["src_PERSON.csv", "src_SMOKING.csv", "src_WEIGHT.csv"],
        arg__input_dir,
        arg__rules_file,
    )

    assert 0 == result.exit_code

    ### check data

    ##
    # backmap the ids.
    # source -> target
    # target -> source
    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")

    assert 4 == len(s2t)

    for measurement in csvrow.csv_rows(output / "measurement.tsv", "\t"):
        assert "35811769" == measurement.measurement_concept_id

        source_day = measurement.measurement_date
        source_id = int(t2s[measurement.person_id])

        if 6789 == source_id:
            if "2023-10-12" == source_day:
                assert "7532.1" == measurement.value_as_number
            elif "2023-10-11" == source_day:
                assert "76.0" == measurement.value_as_number
            elif "2023-11-21" == source_day:
                assert "86.123" == measurement.value_as_number
            else:
                raise Exception(f"bad value for 6789 {source_day=}, {measurement=}")
        elif 321 == source_id:
            assert "2025-01-03" == source_day
            assert "23" == measurement.value_as_number
        else:
            raise Exception(f"bad value, {source_id=}, {source_day=}, {measurement=}")


@pytest.mark.unit
def test_duplications(tmp_path: Path):
    # Get the package root directory
    package_root = importlib.resources.files("carrottransform")
    package_root = (
        package_root if isinstance(package_root, Path) else Path(str(package_root))
    )

    test_files = package_root.parent / "tests/test_data/test_duplications"

    ##
    # setup the args
    arg__input_dir = test_files
    arg__rules_file = test_files / "transform-rules.json"

    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["src_PERSON.csv", "src_SMOKING.csv", "src_WEIGHT.csv"],
        arg__input_dir,
        arg__rules_file,
    )

    assert 0 == result.exit_code

    ##
    #

    # test the person_ids table
    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")

    assert 3 == len(s2t)
    assert 3 == len(t2s)

    # load and chcekc the output person(s)
    person_counts = {}
    people = {}
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        src_person_id = t2s[person.person_id]

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
    for src_person in csvrow.csv_rows(arg__input_dir / "src_PERSON.csv"):
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
        if "male" == person.gender_source_value:
            assert 8507 == int(person.gender_concept_id)
        elif "female" == person.gender_source_value or (
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
        person = people[t2s[measurement.person_id]]

        src_person_id = t2s[measurement.person_id]

        assert "35811769" == measurement.measurement_concept_id

        # >>>
        # >>>
        # >>>
        print("heyheyh hey !!! should this be blank?")
        assert "" == measurement.measurement_time  #
        # >>>
        # >>>
        # >>>

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

        if "321" == src_person_id:
            assert "7" == measurement.measurement_id
            assert "2025-01-03 00:00:00" == measurement.measurement_datetime
            assert "23" == measurement.value_as_number

        elif "6789" == src_person_id:
            if "1" == measurement.measurement_id:
                assert "2023-10-12 00:00:00" == measurement.measurement_datetime
                assert "75" == measurement.value_as_number

            elif "2" == measurement.measurement_id:
                assert "2023-10-11 00:00:00" == measurement.measurement_datetime
                assert "76" == measurement.value_as_number

            elif "3" == measurement.measurement_id:
                assert "2023-11-21 00:00:00" == measurement.measurement_datetime
                assert "86" == measurement.value_as_number

            elif "4" == measurement.measurement_id:
                assert "2023-11-21 00:00:00" == measurement.measurement_datetime
                assert "68" == measurement.value_as_number

            elif "5" == measurement.measurement_id:
                assert "2023-11-21 00:00:00" == measurement.measurement_datetime
                assert "86" == measurement.value_as_number

            elif "6" == measurement.measurement_id:
                assert "2023-11-21 00:00:00" == measurement.measurement_datetime
                assert "86" == measurement.value_as_number

            else:
                raise Exception(f"unexpected measurement {measurement=}")
        else:
            raise Exception("unexpected ")

    ##
    # now check the observations
    observation_count = {}
    for k in s2t:
        observation_count[k] = 0
    for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
        person = people[t2s[observation.person_id]]

        src_person_id = t2s[observation.person_id]
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

        if "789345" == src_person_id:
            assert "35810208" == observation.observation_concept_id
            assert "2025-05-21 15:02:00" == observation.observation_datetime
            assert "CURRENT_SMOKER" == observation.value_as_string

        elif "6789" == src_person_id:
            assert "35810209" == observation.observation_concept_id
            assert "2025-05-12 21:20:00" == observation.observation_datetime
            assert "FORMER_SMOKER" == observation.value_as_string

        elif "321" == src_person_id:
            assert "35821355" == observation.observation_concept_id
            assert "2025-05-22 03:14:00" == observation.observation_datetime
            assert "NEVER_SMOKER" == observation.value_as_string

        else:
            raise Exception(
                f"unexpected person {src_person_id=} {observation.person_id=}"
            )

    assert 1 == observation_count["789345"]
    assert 1 == observation_count["6789"]
    assert 4 == observation_count["321"]

@pytest.mark.unit
def test_dual_weight_observations(tmp_path: Path):
    raise Exception('run me!')


def assert_datetimes(onlydate: str, datetime: str, expected: str):
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


