import os
import random
import shutil
import string
import subprocess
from pathlib import Path

import pytest

import carrottransform.tools.sources as sources
import tests.csvrow as csvrow
import tests.testools as testools
from tests.click_tools import package_root

project_root: Path = package_root.parent


class DockerImage:
    """class to build a docker container with a distinct (random) name and retrn that name for a `with as:` block"""

    def __init__(self, name: str, root: Path):
        self._root = root
        self._name = name
        self._image_name = ""

    def __enter__(self):
        assert self._image_name == ""

        length: int = 16
        chars: str = string.ascii_lowercase

        self._image_name = self._name + "".join(
            random.choice(chars) for _ in range(length)
        )

        result = subprocess.run(
            ["docker", "build", ".", "-t", self._image_name], cwd=self._root
        )
        assert 0 == result.returncode
        return self._image_name

    def __exit__(self, exc_type, exc_value, traceback):
        assert self._image_name != "", "no image name set?"

        # remove the container
        result = subprocess.run(["docker", "rmi", self._image_name])
        assert 0 == result.returncode, f"failed to remove image {self._image_name}"

        self._image_name = ""

        # Return False to propagate exceptions, True to suppress them
        return False


@pytest.mark.docker
def test_dock_observations(tmp_path: Path):
    """does one of the (v1) integration tests using the docker container

    TODO; it'd be really cool to do this as another matrix/variation of the existing integration tests

    """

    test_person_file = "observe_smoking/demos.csv"

    # build a temp copy of the container
    with DockerImage("carrot_transform", project_root) as image_name:
        ###
        # arrange
        ##

        # compute the paths
        person_file = project_root / "tests/test_data" / test_person_file
        test_home = person_file.parent

        # copy the files into our temp folder
        for item in os.listdir(test_home):
            if not (item.endswith(".csv") or item.endswith(".json")):
                continue
            shutil.copy(test_home / item, tmp_path / item)

        # this is how to run the container
        command = [
            "docker",
            "run",
            "--rm",
            f"-v{tmp_path}:/run",
            image_name,
            "uv",
            "run",
            "--python",
            "3.11",
            "python",
            "-m",
            "carrottransform.cli.command",
            "run",
            "mapstream",
        ]

        # these are is the specific program options
        command += [
            "--omop-ddl-file",
            "@carrot/config/OMOPCDM_postgresql_5.3_ddl.sql",
        ]
        command += [
            "--inputs",
            "/run/",
        ]
        command += [
            "--person-file",
            "/run/demos.csv",
        ]
        command += [
            "--rules-file",
            "/run/mapping.json",
        ]
        command += [
            "--output",
            "/run/out",
        ]

        ###
        # act
        ##
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            cwd=tmp_path,
        )

        ##
        # print the containers stuff - the command seems to always return 0 so this isn't going to catch errors for us
        print(f"r = {result.returncode}")
        for o in result.stdout.splitlines(keepends=False):
            print(f"; {o}")
        for e in result.stderr.splitlines(keepends=False):
            print(f"! {e}")

        ###
        # assert
        ##

        # check the return code
        assert 0 == result.returncode

        #
        testools.compare_to_tsvs(
            test_person_file.split("/")[0],
            sources.csvSourceObject(tmp_path / "out", sep="\t"),
        )
