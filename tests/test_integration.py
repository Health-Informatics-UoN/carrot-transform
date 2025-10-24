"""
these are various integration tests for carroti using pytest and the inbuild click tools

"""

import logging
import re
from pathlib import Path

import pytest
import sqlalchemy

import tests.click_tools as click_tools
import tests.csvrow as csvrow

import tests.testools as testools
import carrottransform.tools.outputs as outputs
import carrottransform.tools.sources as sources

test_data = Path(__file__).parent / 'test_data'

##
# concept constants
# these are concept values that happen to be used in these tests. they're not a universal thing - they're just what is being used here.

#
concept__height = 903133
concept__weight = 903121

#
concept__mammogram_01 = 4031867
concept__mammogram_10 = 4031244
concept__mitoses = 4240068
concept__pitting = 4227224

# smoking constants
concept__active = 3959110
concept__quit = 3957361
concept__never = 35821355

# end of concept constants
##


###
# tests for https://github.com/Health-Informatics-UoN/carrot-transform/issues/83


# end of tests for https://github.com/Health-Informatics-UoN/carrot-transform/issues/83
##


###
# these "check__" functions define (old) specific extra checks for the integration testing that'd otherwise be parameterised


def check__integration_test1(
    result, output, person_id_source2target, person_id_target2source
):
    """
    this is one of the bigger tests - it checks a lot of details that may be redundant

    TODO; convert this to the expectation-style (as below) possibly not for persons though
    """

    # this test (is one of the ones that) needs to read the person file as part of verification
    person_file = Path(__file__).parent / "test_data/integration_test1/src_PERSON.csv"

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


def check__floats(result, output, person_id_source2target, person_id_target2source):
    """
    checks if floats are mapped correctly
    """

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


def check__duplications(
    result, output, person_id_source2target, person_id_target2source
):
    """
    checks if duplications are handled correctly. it duplicates a person and observations to see if each ios handled correctly


    """

    # this test needs to read the person file as part of verification
    person_file = Path(__file__).parent / "test_data/duplications/src_PERSON.csv"

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


def check__mapping_person(
    result, output, person_id_source2target, person_id_target2source
):
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


@pytest.mark.unit  # it's an integration test ... but i need/want one that i can check quickly
def test_sql_read(tmp_path: Path):
    """
    this test checks to be sure that two measurements (width and height) don't "collide" or interfere with eachother
    """

    # this is the paramter
    testing_person_file = "measure_weight_height/persons.csv"

    # test_folder
    test_folder = (test_data / testing_person_file).parent

    # cool; find the rules mapping
    mapper = ''
    for json in test_folder.glob('*.json'):
        assert '' == mapper
        mapper = str(json).replace('\\','/')
    assert '' != mapper

    # TODO; compute these too
    test_name = 'measure_weight_height'
    person = 'persons.csv'

    # create an SQLite database and copy the contents into it
    sqlite3 = tmp_path / f"{test_name}.sqlite3"
    testools.copy_across(
        ot = outputs.sqlOutputTarget(
            sqlalchemy.create_engine(
                f"sqlite:///{sqlite3.absolute()}"
            )
        ),
        so = test_folder
    )

    # run the test sourcing that SQLite database but writing to disk
    testools.run_v1(
        inputs= f"sqlite:///{sqlite3.absolute()}",
        person= person,
        mapper= mapper,
        output= str((tmp_path / 'out').absolute())
    )

    # cool; now verify that the on-disk results are good
    testools.compare_to_tsvs(
        "measure_weight_height", sources.csvSourceObject(tmp_path / 'out', sep="\t")
    )


pass__arg_names = [
    "pass__input__as_arg",
    "pass__rules_file__as_arg", 
    "pass__person_file__as_arg",
    "pass__output_dir__as_arg",
    "pass__omop_ddl_file__as_arg",
    "pass__omop_config_file__as_arg"
]

@pytest.mark.integration

# generate the cases controlling how the parameters are passed
@pytest.mark.parametrize(
    ",".join(pass__arg_names),
    list(testools.bool_interesting(len(pass__arg_names)))
)

# declares actual test cases ... this could/should be refactored down
# - 1 - use output TSVs and feed them back in as the expected results
# - 2 - do this in a declarative generator way (like the bools)
@pytest.mark.parametrize(
    "patient_csv, persons, observations, measurements, conditions, post_check",
    [
        pytest.param(
            # patient_csv
            "integration_test1/src_PERSON.csv",
            # persons
            4,
            # observations
            None,
            # measurements
            None,
            # conditions
            None,
            check__integration_test1,
            id="the original integration check",
        ),
        pytest.param(
            # patient_csv
            "floats/src_PERSON.csv",
            # persons
            4,
            # observations
            None,
            # measurements
            None,
            # conditions
            None,
            check__floats,
            id="checks if floats are mapped correctly",
        ),
        pytest.param(
            # patient_csv
            "duplications/src_PERSON.csv",
            # persons
            3,
            # observations
            None,
            # measurements
            None,
            # conditions
            None,
            check__duplications,
            id="checks if duplications are handled correctly. it duplicates a person and observations to see if each is handled correctly",
        ),
        pytest.param(
            # patient_csv
            "mapping_person/demos.csv",
            # persons
            2,
            # observations
            None,
            # measurements
            None,
            # conditions
            None,
            check__mapping_person,
            id="test to see if basic person records map as expected",
        ),
        pytest.param(
            # patient_csv
            "observe_smoking/demos.csv",
            # persons
            4,
            # observations
            {
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
            },
            # measurements
            None,
            # conditions
            None,
            None,  # no extra post-test checks
            id="check if observations work as expected",
        ),
        pytest.param(
            # patient_csv
            "measure_weight_height/persons.csv",
            # persons
            4,
            # observations
            None,
            # measurements
            {
                21: {
                    "2021-12-02": {concept__height: 123, concept__weight: 31},
                    "2021-12-01": {concept__height: 122},
                    "2021-12-03": {concept__height: 12, concept__weight: 12},
                    "2022-12-01": {concept__weight: 28},
                },
                81: {
                    "2022-12-02": {concept__height: 23, concept__weight: 27},
                    "2021-03-01": {concept__height: 92},
                    "2020-03-01": {concept__weight: 92},
                },
                91: {
                    "2021-02-03": {concept__height: 72, concept__weight: 12},
                    "2021-02-01": {concept__weight: 1},
                },
            },
            # conditions
            None,
            None,  # no extra post-test checks
            id="measurements of weight and height",
        ),
        pytest.param(
            # patient_csv
            "condition/persons.csv",
            # persons
            4,
            # observations
            None,
            # measurements
            None,
            # conditions
            {
                81: {
                    "1998-02-01": {concept__pitting: 1},
                    "1998-02-03": {concept__pitting: 13},
                },
                91: {
                    "2001-01-03": {concept__pitting: 1},
                    "2001-01-05": {concept__pitting: 7},
                },
            },
            None,  # no post-check
            id="checks conditions are sent to the condition tsv as expected",
        ),
    ],
)

# controls wether we use the SQL or CSV things
@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(False, id="use CSV source"),
        pytest.param(True, id="use SQL source"),
    ],
)
def test_fixture(
    tmp_path: Path,
    engine: bool,
    # args or envar
    pass__input__as_arg: bool,
    pass__rules_file__as_arg: bool,
    pass__person_file__as_arg: bool,
    pass__output_dir__as_arg: bool,
    pass__omop_ddl_file__as_arg: bool,
    pass__omop_config_file__as_arg: bool,
    patient_csv,
    persons,
    observations,
    measurements,
    conditions,
    post_check,
):
    (result, output, person_id_source2target, person_id_target2source) = (
        click_tools.click_test(
            tmp_path=tmp_path,
            person_file=patient_csv,
            persons=persons,
            observations=observations,
            measurements=measurements,
            conditions=conditions,
            engine=engine,
            # args or envar
            pass__input__as_arg=pass__input__as_arg,
            pass__rules_file__as_arg=pass__rules_file__as_arg,
            pass__person_file__as_arg=pass__person_file__as_arg,
            pass__output_dir__as_arg=pass__output_dir__as_arg,
            pass__omop_ddl_file__as_arg=pass__omop_ddl_file__as_arg,
            pass__omop_config_file__as_arg=pass__omop_config_file__as_arg,
        )
    )

    if post_check is not None:
        post_check(result, output, person_id_source2target, person_id_target2source)


@pytest.mark.integration
def test_mireda_key_error(tmp_path: Path, caplog):
    """this is the original buggy version that should trigger the key error"""

    # capture all
    caplog.set_level(logging.DEBUG)

    person_file = (
        Path(__file__).parent
        / "test_data/mireda_key_error/demographics_mother_gold.csv"
    )

    (result, output) = click_tools.click_test(
        tmp_path,
        person_file,
        failure=True,
    )

    assert result.exit_code == -1

    message = caplog.text.splitlines(keepends=False)[-1]

    assert message.strip().endswith(
        "Person properties were mapped from (['demographics_child_gold.csv', 'infant_data_gold.csv']) but can only come from the person file person_file.name='demographics_mother_gold.csv'"
    )

    assert "-1" == str(result.exception)


def assert_datetimes(onlydate: str, datetime: str, expected: str):
    """
    this function performs date/time checking accounting for missing time info

    i would preffer new tests do somethign else
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
