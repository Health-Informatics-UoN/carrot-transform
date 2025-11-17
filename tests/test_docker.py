import os
import random
import shutil
import string
import subprocess
from pathlib import Path

import pytest

import tests.csvrow as csvrow
from tests.click_tools import package_root, project_root
from tests.testools import package_root, project_root


class DockerImage:
    """class to build a docker container with a distinct (random) name and retrn that name for a `with as:` block"""

    def __init__(self, name: str, root: Path):
        self._root = root
        self._name = name
        self._image_name = ""

    def __enter__(self) -> str:
        assert self._image_name == ""

        length: int = 16
        chars: str = string.ascii_lowercase

        self._image_name = self._name + "".join(
            random.choice(chars) for _ in range(length)
        )

        result = subprocess.run(
            ["docker", "build", ".", "-t", self._image_name], cwd=self._root
        )
        assert 0 == result.returncode
        return self._image_name

    def __exit__(self, exc_type, exc_value, traceback):
        assert self._image_name != "", "no image name set?"

        # remove the container
        result = subprocess.run(["docker", "rmi", self._image_name])
        assert 0 == result.returncode, f"failed to remove image {self._image_name}"

        self._image_name = ""

        # Return False to propagate exceptions, True to suppress them
        return False


@pytest.mark.docker
def test_dock_observations(tmp_path: Path):
    """does one of the (v1) integration tests using the docker container

    TODO; it'd be really cool to do this as another matrix/variation of the existing integration tests

    """

    # build a temp copy of the container
    with DockerImage("carrot_transform", project_root) as image_name:
        ###
        # arrange
        ##

        # compute the paths
        person_file = project_root / "tests/test_data" / "observe_smoking/demos.csv"
        test_home = person_file.parent

        # copy the files into our temp folder
        for item in os.listdir(test_home):
            if not (item.endswith(".csv") or item.endswith(".json")):
                continue
            shutil.copy(test_home / item, tmp_path / item)

        ###
        # act
        ##

        # invoke our process!

        command = [
            "docker",
            "run",
            "--rm",
            f"-v{tmp_path}:/run",
            image_name,
            "uv",
            "run",
            "--python",
            "3.11",
            "python",
            "-m",
            "carrottransform.cli.command",
            "run",
            "mapstream",
            "--omop-ddl-file",
            "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
            "--rules-file",
            "/run/mapping.json",
            "--output-dir",
            "/run/out",
            "--person-file",
            "/run/demos.csv",
            "--input-dir",
            "/run/",
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=tmp_path,
        )

        ##
        # print the containers stuff - the command seems to always return 0 so this isn't going to catch errors for us
        print(f"r = {result.returncode}")
        for o in result.stdout.splitlines(keepends=False):
            print(f"; {o}")
        for e in result.stderr.splitlines(keepends=False):
            print(f"! {e}")

        ###
        # assert
        ##

        output = tmp_path / "out"
        persons = 4
        observations = {
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
        measurements = None
        conditions = None

        validate(
            output=output,
            persons=persons,
            observations=observations,
            measurements=measurements,
            conditions=conditions,
        )


def validate(
    output: Path,
    persons: None | int,
    observations: None | dict,
    measurements: None | dict,
    conditions: None | dict,
):
    """reproduces the prior validation logic

    TODO; merge this with the other validation logic
    """

    # get the person_ids table
    [person_id_source2target, person_id_target2source] = csvrow.back_get(
        output / "person_ids.tsv"
    )

    if (
        persons is not None
        or measurements is not None
        or observations is not None
        or conditions is not None
    ):

        def assert_to_int(value: str) -> int:
            """used to convert strings to ints and double check that the conversion works both ways"""
            result = int(value)
            assert str(result) == value
            return result

        def record_count(collection):
            count = 0
            for person in collection:
                for date in collection[person]:
                    count += len(collection[person][date])
            return count

        if persons is None:
            pass
        elif isinstance(persons, int):
            assert len(person_id_source2target) == persons
            assert len(person_id_target2source) == persons
        else:
            raise Exception(f"persons check is {type(persons)=}")

        # the state in observations
        if observations is not None:
            observations_seen: int = 0
            for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
                observations_seen += 1

                assert assert_to_int(observation.observation_type_concept_id) == 0
                assert observation.value_as_number == ""
                assert (
                    observation.observation_date
                    == observation.observation_datetime[:10]
                )
                assert observation.value_as_concept_id == ""
                assert observation.qualifier_concept_id == ""
                assert observation.unit_concept_id == ""
                assert observation.provider_id == ""
                assert observation.visit_occurrence_id == ""
                assert observation.visit_detail_id == ""
                assert observation.unit_source_value == ""
                assert observation.qualifier_source_value == ""
                assert (
                    observation.observation_concept_id
                    == observation.observation_source_concept_id
                )
                assert (
                    observation.value_as_string == observation.observation_source_value
                )

                assert observation.person_id in person_id_target2source
                src_person_id = assert_to_int(
                    person_id_target2source[observation.person_id]
                )

                observation_date = observation.observation_date
                observation_concept_id = observation.observation_concept_id
                observation_source_value = observation.observation_source_value

                assert src_person_id in observations, observation
                assert observation_date in observations[src_person_id], observation
                assert (
                    observation_concept_id
                    in observations[src_person_id][observation_date]
                ), observation

                assert (
                    observations[src_person_id][observation_date][
                        observation_concept_id
                    ]
                    == observation_source_value
                ), observation

            # check to be sure we saw all the observations
            expected_observation_count = record_count(observations)
            assert expected_observation_count == observations_seen, (
                "expected %d observations, got %d"
                % expected_observation_count
                % observations_seen
            )

        # check measurements
        if measurements is not None:
            measurements_seen: int = 0
            for measurement in csvrow.csv_rows(output / "measurement.tsv", "\t"):
                measurements_seen += 1

                src_person_id = assert_to_int(
                    person_id_target2source[measurement.person_id]
                )
                date = measurement.measurement_date
                concept = assert_to_int(measurement.measurement_concept_id)
                value = assert_to_int(measurement.value_as_number)

                assert src_person_id in measurements, (
                    f"{src_person_id=} {measurement=} "
                )
                assert date in measurements[src_person_id], (
                    f"{src_person_id=} {date=} {measurement=}"
                )
                assert concept in measurements[src_person_id][date], (
                    f"{src_person_id=} {date=} {concept=} {measurement=}"
                )

                assert measurements[src_person_id][date][concept] == value, (
                    f"{date=} {concept=} {measurement=}"
                )
            expected_measurement_count = record_count(measurements)
            assert measurements_seen == expected_measurement_count

        # check for conditon occurences
        if conditions is not None:
            conditions_seen: int = 0
            for condition in csvrow.csv_rows(output / "condition_occurrence.tsv", "\t"):
                conditions_seen += 1

                src_person_id = assert_to_int(
                    person_id_target2source[condition.person_id]
                )
                src_date = condition.condition_start_datetime
                concept_id = assert_to_int(condition.condition_concept_id)
                src_value = assert_to_int(condition.condition_source_value)

                assert condition.condition_start_date == ""

                # there's a known shortcoming of the conditions that make them act like observations
                # https://github.com/Health-Informatics-UoN/carrot-transform/issues/88
                assert src_date == condition.condition_end_datetime
                src_date = src_date[:10]

                assert condition.condition_end_date == src_date

                assert src_person_id in conditions
                assert src_date in conditions[src_person_id]
                assert concept_id in conditions[src_person_id][src_date]
                assert conditions[src_person_id][src_date][concept_id] == src_value

            assert record_count(conditions) == conditions_seen
