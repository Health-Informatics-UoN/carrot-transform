"""
tests the complete system in a few ways. needs better verification of the outputs
"""

import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from carrottransform.cli.subcommands.run import mapstream
from tests.testools import package_root


@pytest.mark.integration
def test_run_the_command_line():
    """simple test/check to see if the project can "run" - which is good for checking things like imports"""

    import os

    # we only care about the return value, and, subprocess kept failing
    assert 0 == os.system("uv run carrot-transform run mapstream --help")

