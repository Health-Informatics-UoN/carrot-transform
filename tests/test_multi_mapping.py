"""
these tests check the multi-mapping needs for integration tests.
"""

import pytest

from pathlib import Path


import clicktools
import csvrow

print("should this test people with birthdate on separte line?")


@pytest.mark.unit
def test_mapping_person(tmp_path: Path):
    arg__input_dir = Path(__file__).parent / "test_multi_mapping/test_mapping_person"
    arg__rules_file = arg__input_dir / "multi_mapping.json"

    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["demos.csv"],
        arg__input_dir,
        arg__rules_file,
    )

    assert 0 == result.exit_code

    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")
    assert 2 == len(s2t)
    assert 2 == len(t2s)

    # check the birthdateime and gender
    seen_ids: list[int] = []
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        # check the ids
        assert person.person_id in t2s
        src_person_id = int(t2s[person.person_id])
        assert src_person_id not in seen_ids
        seen_ids.append(src_person_id)

        # dataset is small so just check it like this
        if 28 == src_person_id:
            assert "8532" == person.gender_concept_id
            assert "2020-09-12 00:00:00" == person.birth_datetime
        elif 82 == src_person_id:
            assert "8507" == person.gender_concept_id
            assert "2020-09-11 00:00:00" == person.birth_datetime
        else:
            raise Exception(f"unexpected {src_person_id=} in {person=}")
    assert 28 in seen_ids
    assert 82 in seen_ids

    # ethnicity is an observation
    for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
        assert observation.person_id in t2s
        src_person_id = int(t2s[observation.person_id])
        if 28 == src_person_id:
            assert "40618782" == observation.observation_concept_id
            assert "2020-09-12" == observation.observation_date
        elif 82 == src_person_id:
            assert "40637268" == observation.observation_concept_id
            assert "2020-09-11" == observation.observation_date
        else:
            raise Exception(f"unexpected {src_person_id=} in {observation=}")


@pytest.mark.unit
def test_observe_smoking(tmp_path: Path):
    arg__input_dir = Path(__file__).parent / "test_multi_mapping/test_observe_smoking"
    arg__rules_file = arg__input_dir / "mapping.json"

    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["demos.csv", "smoke.csv"],
        arg__input_dir,
        arg__rules_file,
    )

    assert 0 == result.exit_code

    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")
    assert 4 == len(s2t)
    assert 4 == len(t2s)

    # declare the expectations
    # ... it's a lot nicer to declare it like this
    expect = {
        123: {
            "2018-01-01": "active",
            "2018-02-01": "active",
            "2018-03-01": "quit",
            "2018-04-01": "active",
            "2018-05-01": "never",
        },
        456: {
            "2009-01-01": "never",
            "2009-02-01": "never",
            "2009-03-01": "quit",
        },
    }

    concept = {
        "active": "3959110",
        "quit": "3957361",
        "never": "35821355",
    }

    # now rewrite the expectation to use concept ids
    for p in expect:
        for d in expect[p]:
            expect[p][d] = concept[expect[p][d]]

    # check the smoking state in observations
    seen: int = 0
    for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
        seen += 1

        assert observation.person_id in t2s
        src_person_id = int(t2s[observation.person_id])

        observation_date = observation.observation_date
        observation_concept_id = observation.observation_concept_id

        assert src_person_id in expect, observation
        assert observation_date in expect[src_person_id], observation
        assert observation_concept_id == expect[src_person_id][observation_date], (
            observation
        )

    assert 8 == seen


@pytest.mark.unit
def test_measure_weight_height(tmp_path: Path):
    arg__input_dir = (
        Path(__file__).parent / "test_multi_mapping/test_measure_weight_height"
    )
    arg__rules_file = arg__input_dir / "mapping.json"

    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["persons.csv", "weights.csv", "heights.csv"],
        arg__input_dir,
        arg__rules_file,
    )

    assert 0 == result.exit_code

    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")
    assert 4 == len(s2t)
    assert 4 == len(t2s)

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

        src_person_id = int(t2s[measurement.person_id])
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


@pytest.mark.unit
def test_condition(tmp_path: Path):
    arg__input_dir = Path(__file__).parent / "test_multi_mapping/test_condition"
    arg__rules_file = arg__input_dir / "mapping.json"

    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["persons.csv", "scans.csv"],
        arg__input_dir,
        arg__rules_file,
    )

    assert 0 == result.exit_code

    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")
    assert 4 == len(s2t)
    assert 4 == len(t2s)

    expect = {
        s2t["81"]: {
            "1998-02-01": {4227224: 1},
            "1998-02-03": {4227224: 13},
        },
        s2t["91"]: {
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

        # someday this will be fixed and not-true
        assert date == occurrence.condition_end_datetime
        date = date[:10]

        assert occurrence.condition_end_date == date

        assert person_id in expect
        assert date in expect[occurrence.person_id]
        assert concept in expect[occurrence.person_id][date]
        assert value == expect[occurrence.person_id][date][concept]

    assert 4 == occurrences
