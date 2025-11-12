"""
copy of the test_integration - but - with only a few cases so it can be run quicker

"""

import logging
import re
from pathlib import Path

import pytest
from click.testing import CliRunner

import tests.click_tools as click_tools
import tests.csvrow as csvrow
import tests.testools as testools
from carrottransform.cli.subcommands.run import mapstream
from carrottransform.tools import outputs, sources
from tests.click_tools import package_root

logger = logging.getLogger(__name__)


#### ==========================================================================
## test case generator


def pair_test_cases(s3: bool = False):
    from types import SimpleNamespace

    class Circular:
        """indefinietly loops through items, but, has an indicator to check when we've seen all values.

        should do something more elabourate to cross multiples together
        """

        def __init__(self, need: int, *data):
            self._data = data
            self._need: int = need
            self._loops: int = 0
            self._iter = iter(data)

        def more(self):
            return self._need >= self._loops

        def get_next(self):
            try:
                return next(self._iter)
            except StopIteration:
                self._loops += 1
                self._iter = iter(self._data)
                return self.get_next()

    ##
    # define the circular thigns that're sources of data

    test = Circular(
        2,
        # TODO; add more test cases
        SimpleNamespace(folder="observe_smoking", person="demos", rules=""),
    )

    input_from = Circular(
        1,
        "csv",
    )

    if s3:
        output_to = Circular(3, "s3")
    else:
        output_to = Circular(3, "csv")

    # TODO; add in the envirnment variable switches

    # TODO: loop differntly. change one var each iteration

    ##
    # now loop through them.

    while test.more() or input_from.more() or output_to.more():
        yield SimpleNamespace(
            test=test.get_next(),
            input_from=input_from.get_next(),
            output_to=output_to.get_next(),
        )


#### ==========================================================================
## test front-ends so i don't do s3 tests (and eat my budget) when i don't want


@pytest.mark.integration
@pytest.mark.parametrize("case", list(pair_test_cases(s3=False)))
def test_local(
    tmp_path: Path,
    caplog,
    case,
):
    run_test(tmp_path, caplog, case)


@pytest.mark.s3tests
@pytest.mark.parametrize("case", list(pair_test_cases(s3=True)))
def test_s3(
    tmp_path: Path,
    caplog,
    case,
):
    run_test(tmp_path, caplog, case)


#### ==========================================================================
## the test body
def run_test(
    tmp_path: Path,
    caplog,
    case,
):
    caplog.set_level(logging.INFO)

    # setup the output
    if "s3" == case.output_to:
        output: str = f"s3:{testools.CARROT_TEST_BUCKET}/local"

    elif "csv" == case.output_to:
        output: str = str(tmp_path / "out")

    else:
        raise Exception(f"unknown {case.output_to=}")

    # rules
    mapper: str = case.test.rules
    if "" == mapper:
        for json in (package_root.parent / f"tests/test_data/{case.test.folder}/").glob(
            "*.json"
        ):
            assert "" == mapper
            mapper = str(json)
    assert "" != mapper

    # determine person file and input
    if "csv" == case.input_from:
        person: str = case.test.person
        inputs: str = str(package_root.parent / f"tests/test_data/{case.test.folder}/")
    else:
        raise Exception(f"unknown {case.input_from}")

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
            "--person",
            person,
            "--output",
            output,
            # "--omop-ddl-file",
            # "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
        ],
    )

    if result.exception is not None:
        print(result.exception)
        raise (result.exception)

    ##
    #
    readback: sources.SourceObject
    if "s3" == case.output_to:
        readback = sources.s3SourceObject(
            f"s3:{testools.CARROT_TEST_BUCKET}/local", sep="\t"
        )

    elif "csv" == case.output_to:
        readback = sources.csvSourceObject(Path(output), "\t")

    else:
        raise Exception(f"unknown {case.output_to=}")

    ##
    # verify / assert
    testools.compare_to_tsvs("observe_smoking", readback)


## end (of test cases)
#### --------------------------------------------------------------------------
