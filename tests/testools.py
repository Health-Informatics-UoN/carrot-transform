import logging
import re
from pathlib import Path

import pytest
from click.testing import CliRunner

import tests.click_tools as click_tools
import tests.csvrow as csvrow
from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs, sources
from tests.click_tools import package_root

#
logger = logging.getLogger(__name__)

# Get the package root directory
project_root: Path = Path(__file__).parent.parent

# need/want to define the s3 bucket somewhere, so, let's do it here
CARROT_TEST_BUCKET = "carrot-transform-testtt"

#### ==========================================================================
## unit test cases - test the test functions


@pytest.mark.unit
def test_compare(caplog):
    """test the validator"""

    caplog.set_level(logging.INFO)

    path: Path = project_root / "tests/test_data/observe_smoking"

    compare_to_tsvs("observe_smoking", sources.csvSourceObject(path, sep="\t"))


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

    if subpath.startswith("@carrot"):
        test: Path = PathArg.convert(subpath, None, None)
    else:
        test: Path = project_root / "tests/test_data" / subpath

    # open the saved .tsv file
    so_ex = sources.csvSourceObject(test, sep="\t")

    person_ids_seen = False
    persons_seen = False

    for item in test.glob("*.tsv"):
        name: str = item.name[:-4]

        person_ids_seen = person_ids_seen or ("person_ids" == name)
        persons_seen = persons_seen or ("person" == name)

        import itertools

        for e, a in itertools.zip_longest(so_ex.open(name), so.open(name)):
            assert e is not None
            assert a is not None

            assert e == a
        logger.info(f"matching {subpath=} for {name=}")

    assert person_ids_seen and persons_seen, (
        "verification data missing from the test case"
    )

    # it matches!
    logger.info(f"all match in {subpath=}")


#### ==========================================================================
## utility functions
def variations(keys):
    """
    computes key -> bool dicts where all, none, or only one value is true or false, then, maps those dicts to simple lists
    """

    def permutations(keys):
        c = len(keys)
        assert c > 0
        if c == 1:
            yield {keys[0]: True}
            yield {keys[0]: False}
        else:
            k = keys[0]
            for p in permutations(keys[1:]):
                p = p.copy()
                p[k] = True
                yield p
                p = p.copy()
                p[k] = False
                yield p

    for p in permutations(list(keys)):
        values = list(p.values())
        tc = values.count(True)
        fc = values.count(False)

        if (tc in [0, 1]) or (fc in [0, 1]):
            o = []
            for k in p:
                if p[k]:
                    o += [k]
            yield o


def permutations(**name_to_list):
    """given a map of lists; yield all permutations of the contents"""

    def loop(listing):
        c = len(listing)

        if 0 == c:
            return

        k, l = listing[0]

        if 1 == c:
            for v in l:
                yield {k: v}

            return

        for t in loop(listing[1:]):
            for v in l:
                t[k] = v
                yield t.copy()

    for i in loop(list(map(lambda k: (k, name_to_list[k]), name_to_list.keys()))):
        yield i


def repeating_unions(*args: list[list]):
    height = []
    column = []

    for a in args:
        c = list(a)
        column += [c]
        height += [len(c)]

    for i in range(0, max(height)):
        row = {}
        for c in column:
            row = row | c[i % len(c)].copy()
        yield row


def copy_across(ot: outputs.OutputTarget, so: sources.SourceObject | Path, names=None):
    assert isinstance(so, Path) == (names is None)
    if isinstance(so, Path):
        names = [file.name[:-4] for file in so.glob("*.csv")]
        so = sources.csvSourceObject(path=so, sep=",")
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
