"""
https://github.com/Health-Informatics-UoN/carrot-transform/issues/78

"""
"""
regression tests
"""

from carrottransform.cli.subcommands.run import *
import pytest
from unittest.mock import patch
import importlib.resources
import logging

from pathlib import Path
import shutil

from click.testing import CliRunner
from carrottransform.cli.subcommands.run import mapstream


import clicktools
import csvrow


@pytest.mark.unit
def test_non_gender_persons_present(tmp_path: Path):

    # Get the package root directory
    arg__input_dir = importlib.resources.files("tests") / "test_data" / "gender_need"
    arg__rules_file = arg__input_dir / 'gender-need.json'

    ##
    # run the click module
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["src_demographics.csv", "src_smoking.csv"],
        arg__input_dir,
        arg__rules_file
    )

    if None is not result.exception:
        print(result.output)
        raise result.exception

    assert 0 == result.exit_code
    
    ### check data
    
    # check that the six ids came through
    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")
    assert 6 == len(s2t), "there should be six ids"

    # collect ids 
    seen_person_ids: list[int] = []
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        # check the ids
        assert person.person_id in t2s
        source_id = int(t2s[person.person_id])
        assert source_id not in seen_person_ids
        seen_person_ids.append(source_id )

    # check to be sure no ids are missing
    assert 312 in seen_person_ids
    assert 323 in seen_person_ids
    assert  23 in seen_person_ids
    assert 912 in seen_person_ids
    assert 823 in seen_person_ids
    assert 723 in seen_person_ids
    assert 6 == len(seen_person_ids)

@pytest.mark.unit
def test_non_gender_onservations_present(tmp_path: Path):

    # Get the package root directory
    arg__input_dir = importlib.resources.files("tests") / "test_data" / "gender_need"
    arg__rules_file = arg__input_dir / 'gender-need.json'

    ##
    # run the click module
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["src_demographics.csv", "src_smoking.csv"],
        arg__input_dir,
        arg__rules_file
    )

    if None is not result.exception:
        print(result.output)
        raise result.exception

    assert 0 == result.exit_code
    
    ### check data
    
    # check that the six ids came through
    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")
    assert 6 == len(s2t), "there should be six ids"

    # collect persons in observation
    # ... this always worked
    seen_observation_person_ids: list[int] = []
    for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
        source_id = int(t2s[observation.person_id])
        seen_observation_person_ids.append(source_id)

    assert 312 in seen_observation_person_ids
    assert 323 in seen_observation_person_ids
    assert  23 in seen_observation_person_ids
    assert 3 == len(seen_observation_person_ids)
