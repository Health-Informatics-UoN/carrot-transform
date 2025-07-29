import importlib.resources

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream
import csvrow


def click_generic(
    tmp_path: Path,
    person_file: Path | str,
    persons: int | None = None,
    measurements: dict | None = None,
    observations: dict | None = None,
    conditions: dict | None = None,
):
    if isinstance(person_file, str):
        person_file = Path(__file__).parent / "test_data" / person_file

    if not person_file.is_file():
        raise ValueError(f"person_file {person_file} does not exist")

    # list all csvs in that folder, ensure the original file is the first one
    csv_files = [
        f.name
        for f in person_file.parent.glob("*.csv")
        if f.is_file() and f.name != person_file.name
    ]
    csv_files = list(csv_files)
    csv_files.insert(0, person_file.name)

    # find the only rules file in that folder
    rules = [f for f in person_file.parent.glob("*.json") if f.is_file()]
    rules = list(rules)
    if len(rules) != 1:
        raise ValueError(
            f"expected exactly one json file, found {rules=} in {person_file.parent}"
        )

    ##
    #
    (result, output) = click_mapstream(
        tmp_path,
        csv_files,
        person_file.parent,
        rules[0],
    )

    assert 0 == result.exit_code

    ##
    #

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
        verify_expectations(
            output=output,
            person_id_source2target=person_id_source2target,
            person_id_target2source=person_id_target2source,
            persons=persons,
            measurements=measurements,
            observations=observations,
            conditions=conditions,
        )

    return (result, output, person_id_source2target, person_id_target2source)


def click_example(tmp_path: Path, limit: int = -1):
    """sets up the/a test environment and runs the transform thing with it."""

    # Get the package root directory
    package_root = importlib.resources.files("carrottransform")
    package_root = (
        package_root if isinstance(package_root, Path) else Path(str(package_root))
    )

    # rules from carrot mapper
    rules_src = package_root / "examples/test/rules/rules_14June2021.json"
    rules = tmp_path / "rules.json"
    shutil.copy2(rules_src, rules)

    srcs = [
        "Demographics.csv",
        "covid19_antibody.csv",
        "Covid19_test.csv",
        "Symptoms.csv",
        "vaccine.csv",
    ]

    # the source files
    for src in srcs:
        with (
            open(package_root / "examples/test/inputs" / src) as s,
            open(tmp_path / src, "w") as o,
        ):
            for s in s.readlines() if limit < 0 else s.readlines()[: (limit + 1)]:
                o.write(s)

    return click_mapstream(tmp_path, srcs, tmp_path, rules)


def click_mapstream(tmp_path: Path, src_names, src_from, rules: Path):
    """sets up the/a test environment and runs the transform thing with it."""

    # Get the package root directory
    package_root = importlib.resources.files("carrottransform")
    package_root = (
        package_root if isinstance(package_root, Path) else Path(str(package_root))
    )

    # output dir needs to be pre-created
    output = tmp_path / "out"
    output.mkdir()

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--input-dir",
            f"{src_from}",
            "--rules-file",
            f"{rules}",
            "--person-file",
            f"{src_from / src_names[0]}",
            "--output-dir",
            f"{output}",
            "--omop-ddl-file",
            f"{package_root / 'config/OMOPCDM_postgresql_5.3_ddl.sql'}",
            "--omop-config-file",
            f"{package_root / 'config/omop.json'}",
        ],
    )
    return (result, output)


def verify_expectations(
    output: Path,
    person_id_source2target,
    person_id_target2source,
    persons: int | None = None,
    measurements: dict | None = None,
    observations: dict | None = None,
    conditions: dict | None = None,
) -> None:
    """
    this is a helper function to check that the expectations are what we expect
    """

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

    if persons is not None:
        assert len(person_id_source2target) == persons
        assert len(person_id_target2source) == persons

    # the state in observations
    if observations is not None:
        observations_seen: int = 0
        for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
            observations_seen += 1

            assert assert_to_int(observation.observation_type_concept_id) == 0
            assert observation.value_as_number == ""
            assert observation.observation_date == observation.observation_datetime[:10]
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
            assert observation.value_as_string == observation.observation_source_value

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
                observation_concept_id in observations[src_person_id][observation_date]
            ), observation

            assert (
                observations[src_person_id][observation_date][observation_concept_id]
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

            assert src_person_id in measurements, f"{src_person_id=} {measurement=} "
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

            src_person_id = assert_to_int(person_id_target2source[condition.person_id])
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
