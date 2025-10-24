
import logging
import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from carrottransform.cli.subcommands.run import mapstream
import tests.click_tools as click_tools
from tests.click_tools import package_root
from carrottransform.tools import outputs, sources
import tests.csvrow as csvrow

#
logger = logging.getLogger(__name__)

# Get the package root directory
project_root: Path = Path(__file__).parent.parent

#### ==========================================================================
## unit test cases - test the test functions

@pytest.mark.unit
def test_compare(caplog):
    """test the validator"""

    caplog.set_level(logging.INFO)

    path: Path = project_root / 'tests/test_data/observe_smoking'

    compare_to_tsvs(
        "observe_smoking", sources.csvSourceObject(path, sep="\t")
    )

#### ==========================================================================
## verification functions

def compare_to_tsvs(subpath: str, so: sources.SourceObject) -> None:
    """scan all tsv files in a folder.

    open each .tsv in the tests subpath and compare it to the open'ed from the named SO.

    if the SO is missing a tsv? fail!
    if the SO has different rows? fail!
    if the SO has extra/too few rows? fail!
    if the SO has .tsv files we don't ... pass ...

    """

    from carrottransform.tools.args import PathArg


    if subpath.startswith('@carrot'):
        test: Path = PathArg.convert(subpath, None, None)
    else:
        test: Path = project_root / "tests/test_data" / subpath

    # open the saved .tsv file
    so_ex = sources.csvSourceObject(test, sep="\t")

    for item in test.glob("*.tsv"):
        name: str = item.name[:-4]

        import itertools

        for e, a in itertools.zip_longest(so_ex.open(name), so.open(name)):
            assert e is not None
            assert a is not None

            assert e == a
        logger.info(f"matching {subpath=} for {name=}")

    # it matches!
    logger.info(f"all match in {subpath=}")

#### ==========================================================================
## utility functions



def bool_interesting(count: int):
    """yield every permutation where only one is true, and the two cases where all are true or all are false
    
    used for test case generation
    """

    def bool_permutations(count: int):
        """just yield all permutations"""
        if count <= 0:
            pass
        elif count == 1:
            yield [True]
            yield [False]
            return
        else:
            for tail in bool_permutations(count - 1):
                yield ([True] + tail)
                yield ([False] + tail)
    
    for e in bool_permutations(count):
        a = sum(e)

        if 0 == a:
            yield e
        if 1 == a:
            yield e
        if count == a:
            yield e
    

def copy_across(ot: outputs.OutputTarget, so: sources.SourceObject | Path, names = None):

    assert isinstance(so, Path) == (names is None)
    if isinstance(so, Path):
        names = [file.name[:-4] for file in so.glob('*.csv')]
        so = sources.csvSourceObject(
            path = so,
            sep = ','
        )
    assert isinstance(so, sources.SourceObject)

    # copy all named ones across
    for name in names:

        i = so.open(name)
        o = None

        for r in i:
            if o is None:
                o = ot.start(name, r)
            else:
                o.write(r)
        # o.close()
        # i.close()
    
    #
    so.close()
    ot.close()



def run_v1(
    inputs: str,
    person: str,
    mapper: str,
    output: str,
):
    ##
    # run click
    runner = CliRunner()
    result = runner.invoke(
        mapstream,
        [
            "--inputs",
            inputs,
            "--rules-file",
            mapper,
            "--person-file",
            person,
            "--output",
            output,
            "--omop-ddl-file",
            "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
            "--omop-config-file",
            "@carrot/config/config.json",
        ],
    )

    if result.exception is not None:
        print(result.exception)
        raise (result.exception)

    assert 0 == result.exit_code


def rand_hex(length: int = 16) -> str:
    """genearttes a random hex string. used for test data"""
    import random

    out = ""
    src = "0123456789abcdef"

    for i in range(0, length):
        out += src[random.randint(0, len(src) - 1)]

    return out
