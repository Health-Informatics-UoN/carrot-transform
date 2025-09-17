import importlib.resources
import logging
import shutil
from pathlib import Path

import sqlalchemy
from click.testing import CliRunner
from sqlalchemy import Column, MetaData, Table, Text, insert

import carrottransform.tools.sources as sources
import tests.csvrow as csvrow
from carrottransform.cli.subcommands.run import mapstream

logger = logging.getLogger(__name__)


# Get the package root directory
package_root: Path = Path(str(importlib.resources.files("carrottransform")))


def click_test(
    # a folder that the test can read/write files into
    tmp_path: Path,
    # either a path to the person_file, or, a string denoting a relative path in the `tests/test_data/` folder
    person_file: Path | str,
    # if true: the data spreadsheets are copied to an SQL database and then read back from there
    engine: bool = False,
    # optional check of how many people should be in the output
    persons: int | None = None,
    # optional check of the measurements values (look for an example test)
    measurements: dict | None = None,
    # optional check of the observations values (look for an example test)
    observations: dict | None = None,
    # optional check of the conditions values (look for an example test)
    conditions: dict | None = None,
    # relative path to the rules_file.json if it's missing we assume there's only one .json sibling of the above person_file path and that .json is the rules file
    rules_file: str | None = None,
    # bool value controlling wether we expect a failure or not. failure tests do no checking and all other tests fail if transform didn't execute/terminate correctly
    failure: bool = False,
    # various booleans to control wether paramters are passed as args or evnars
    pass__input__as_arg: bool = True,
    pass__rules_file__as_arg: bool = True,
    pass__person_file__as_arg: bool = True,
    pass__output_dir__as_arg: bool = True,
    pass__omop_ddl_file__as_arg: bool = True,
    pass__omop_config_file__as_arg: bool = True,
):
    """this function tests carrot transform.


    carrot-transform was built on top of the "click" command line tool.
    a lot of the (integration) tests are minor variations on eachother, and, can be execuited and checked with different parameters.
    this function generalises a lot of that functionality - it also includes the option to create a junk database, import the original test csv files, and run the test against that configuration.

    the file is named "click_tools" since this functionality was orignally divided amongst several functions before being combined here.
    """

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

    # these checks aren't performed for failure tests - so raise an error if the developer tried to get them done
    if failure:
        assert persons is None
        assert measurements is None
        assert observations is None
        assert conditions is None

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
        engine_connection: sqlalchemy.engine.Engine = sqlalchemy.create_engine(
            connection_string
        )

        for csv_file in person_file.parent.glob("*.csv"):
            load_test_database_table(engine_connection, person_file.parent / csv_file)

        copied = tmp_path / "rules.json"
        shutil.copy(rules_json_file, copied)
        rules_json_file = copied
        person_file = tmp_path / person_file.name

    ##
    # build click args

    # these are the containers for the args
    click_args: list[str] = []
    click_env: dict[str, str] = {}

    # to allow swithcing between passign args via ENVAR or command line; we use this
    def pass_as_arg(arg: bool, key: str, value):
        assert key.startswith("--")
        assert key == key.lower()
        if arg:
            click_args.append(key)
            click_args.append(str(value))
        else:
            key = key[2:].upper().replace("-", "_")
            assert key not in click_env
            click_env[key] = str(value)

    if not engine:
        pass_as_arg(pass__input__as_arg, "--input-dir", person_file.parent)
    else:
        pass_as_arg(pass__input__as_arg, "--input-db-url", connection_string)
    pass_as_arg(pass__rules_file__as_arg, "--rules-file", rules_json_file)
    pass_as_arg(pass__person_file__as_arg, "--person-file", person_file)
    pass_as_arg(pass__output_dir__as_arg, "--output-dir", output)
    pass_as_arg(
        pass__omop_ddl_file__as_arg,
        "--omop-ddl-file",
        f"{package_root / 'config/OMOPCDM_postgresql_5.3_ddl.sql'}",
    )
    pass_as_arg(
        pass__omop_config_file__as_arg,
        "--omop-config-file",
        f"{package_root / 'config/config.json'}",
    )

    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(mapstream, click_args, env=click_env)

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


def load_test_database_table(connection: sqlalchemy.engine.Engine, csv: Path):
    """load a csv file into a testing database.

    does some adjustments to make sure the column names work, but, generally dumps it itno a "dumb" database for testing
    """

    # the table name will be inferred from the csv file name; this enforces consistency
    assert csv.name.endswith(".csv")
    tablename: str = csv.name[:-4]

    # open the csv using a sourceOpener
    csvr = sources.SourceOpener(folder=csv.parent).open(csv.name)

    # if the column names have a blank at the end we need to remove it.
    #   sometimes people (named Peter) write csvs like `user,data,data,value,` which would lead to a blank 5th column name; this removes that
    column_names = next(csvr)
    if "" == column_names[-1]:
        column_names = column_names[:-1]

    # make sure there are no other blankes
    for name in column_names:
        if "" == name:
            raise Exception("can't have a blank column name in the CSVs")
        if " " in name:
            raise Exception("can't have spaces in the CSV column names")

    # Create the table in the testing database
    metadata = MetaData()
    table = Table(
        # we're reading from .csv files. all data in csv files will be
        #   text. the data may encode numbers (as text) but we don't
        #   have a way to know which columns are and which aren't.
        #   that's why all columns in this testing database are encoded
        #   as text.
        tablename,
        metadata,
        *([Column(name, Text()) for name in column_names]),
    )
    metadata.create_all(connection, tables=[table])

    # Insert each row from teh csv, into the sql table
    #   thios is tested and works when "columns with blank names" which were removed above
    with connection.begin() as conn:
        for row in csvr:
            record = dict(zip(column_names, row))
            conn.execute(insert(table), record)
