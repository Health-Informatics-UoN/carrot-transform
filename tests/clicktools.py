import importlib.resources

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream


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
