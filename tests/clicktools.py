import importlib.resources

from pathlib import Path

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream
import csvrow


def click_generic(
    tmp_path: Path,
    person_file: Path | str,
    rules: Path | str | None = None,
    failure: bool = False,
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

    # if the rules file is just a string
    if isinstance(rules, str):
        rules = person_file.parent / rules

    # find the only rules file in that folder ... if we need to fine that
    if rules is None:
        r = [f for f in person_file.parent.glob("*.json") if f.is_file()]
        r = list(r)
        if len(r) != 1:
            raise ValueError(
                f"expected exactly one json file, found {r=} in {person_file.parent}"
            )
        rules = r[0]

    ##
    #
    (result, output) = click_mapstream(
        tmp_path,
        csv_files,
        person_file.parent,
        rules,
    )

    ##
    # if there was an error, return somethign different
    if result.exit_code != 0:
        if failure:
            return (result, output)
        else:
            raise ValueError(f"expected no error, found {result=}")
    elif failure:
        raise ValueError(f"expected an error, found {result=}")
    ##
    #

    # test the person_ids table
    [person_id_source2target, person_id_target2source] = csvrow.back_get(
        output / "person_ids.tsv"
    )

    return (result, output, person_id_source2target, person_id_target2source)


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
