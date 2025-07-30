import importlib.resources

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream
import csvrow


def click_generic(tmp_path: Path, person_file: Path | str):
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

    # throw the exception
    if 0 != result.exit_code and None is not result.exception:
        raise result.exception

    # check the return code anyway
    assert 0 == result.exit_code

    ##
    #

    # test the person_ids table
    [person_id_source2target, person_id_target2source] = csvrow.back_get(
        output / "person_ids.tsv"
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
