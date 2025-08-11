import importlib.resources

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream
import csvrow
import carrottransform.tools.sources as sources
import logging
from sqlalchemy import Table, Column, String, MetaData, insert


logger = logging.getLogger(__name__)


# Get the package root directory
package_root: Path = Path(str(importlib.resources.files("carrottransform")))


def click_test(
    tmp_path: Path,
    person_file: Path | str,
    engine: bool = False,
    persons: int | None = None,
    measurements: dict | None = None,
    observations: dict | None = None,
    conditions: dict | None = None,
    rules_file: str | None = None,
    # some tests need the failure
    failure: bool = False,
):
    ##
    # check the person file
    if isinstance(person_file, str):
        if not person_file.startswith("/"):
            person_file = Path(__file__).parent / "test_data" / person_file
        else:
            person_file = person_file[1:]

            person_file = package_root / person_file

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
    rules_json_file: Path
    if rules_file is not None:
        rules_json_file = package_root / rules_file[1:]
    else:
        rules = [f for f in person_file.parent.glob("*.json") if f.is_file()]
        rules = list(rules)
        if len(rules) != 1:
            raise ValueError(
                f"expected exactly one json file, found {rules=} in {person_file.parent}"
            )
        rules_json_file = rules[0]

    ##
    #

    # output dir needs to be pre-created
    output = tmp_path / "out"
    output.mkdir(exist_ok=True)

    ##
    # create the sqlite database
    # ... also change enough parameters we know we're not cheating and looking at the .csv files
    if engine:
        connection_string = f"sqlite:///{(tmp_path / 'testing.db').absolute()}"
        source = sources.SourceOpener(engine=connection_string)
        for csv_file in csv_files:
            load_test_database_table(source, person_file.parent / csv_file)

        copied = tmp_path / "rules.json"
        shutil.copy(rules_json_file, copied)
        rules_json_file = copied
        person_file = tmp_path / person_file.name

    ##
    # build click args
    click_args: list[str] = []
    if not engine:
        click_args.append("--input-dir")

        click_args.append(str(person_file.parent))
    else:
        click_args.append("--input-db-url")
        click_args.append(connection_string)

    click_args.append("--rules-file")
    click_args.append(f"{rules_json_file}")

    click_args.append("--person-file")
    click_args.append(f"{person_file}")

    click_args.append("--output-dir")
    click_args.append(f"{output}")

    click_args.append("--omop-ddl-file")
    click_args.append(f"{package_root / 'config/OMOPCDM_postgresql_5.3_ddl.sql'}")

    click_args.append("--omop-config-file")
    click_args.append(f"{package_root / 'config/omop.json'}")

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        click_args,
    )

    if failure:
        if 0 == result.exit_code:
            raise ValueError(f"expected an error, found {result=}")
        else:
            return (result, output)

    if result.exception is not None:
        raise (result.exception)

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

    return (result, output, person_id_source2target, person_id_target2source)


def load_test_database_table(sourceOpener: sources.SourceOpener, csv: Path):
    """load a csv file into a database. does some adjustments to make sure the column names work"""

    assert sourceOpener._engine is not None

    assert csv.name.endswith(".csv")
    tablename: str = csv.name[:-4]

    csvr = sources.open_csv_rows(sourceOpener, csv)
    column_names = next(csvr)

    # if the column names have a blank at the end we need to stripit
    if "" == column_names[-1]:
        column_names = column_names[:-1]

    # make sure there's no other blankes
    for name in column_names:
        if "" == name:
            raise Exception("can't have a blank column name in the CSVs")
        if " " in name:
            raise Exception("can't have spaces in the CSV column names")

    # Create the table in the database
    metadata = MetaData()
    table = Table(
        tablename, metadata, *([Column(name, String(255)) for name in column_names])
    )
    metadata.create_all(sourceOpener._engine, tables=[table])

    # # Insert rows
    with sourceOpener._engine.begin() as conn:
        for row in csvr:
            record = dict(zip(column_names, row))
            conn.execute(insert(table), record)
