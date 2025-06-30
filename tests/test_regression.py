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


# other regression tests

import clicktools
import csvrow


@pytest.mark.unit
def test_original_problem(tmp_path: Path):
    """
    the users are imported lazily - that needs to be fixed

    the ids are created iin the main loop
    280 - 378


    debgger is probably the way to go

    should flush all the file handles

    """

    # Get the package root directory
    assets = importlib.resources.files("tests")
    
    arg__input_dir = assets / 'regression/lazy-user'
    arg__rules_file = arg__input_dir / 'transform-rules.json'

    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["src_PERSON.csv", "src_SMOKING.csv", "src_WEIGHT.csv"],
        arg__input_dir,
        arg__rules_file
    )

    if None is not result.exception:
        print(result.output)
        raise result.exception

    assert 0 == result.exit_code

    print(
        f"out is {tmp_path=}"
    )

    ### check data

    ##
    # backmap the ids.
    # source -> target
    # target -> source
    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")

    assert 6 == len(s2t), "shouldbe 5 people in the csv"

    # just check the 
    seen_ids = []
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        # check the ids
        assert person.person_id in t2s
        tid = t2s[person.person_id]
        assert tid not in seen_ids
        seen_ids.append(tid)

        print(
            f" {person.person_id=}"
        )

    assert "321" in seen_ids
    assert "789345" in seen_ids
    assert "6789" in seen_ids
    assert "339" in seen_ids, "this id was unused but has a gender"
    assert "319" in seen_ids, "this id is used with a blank gender"
    assert "289" in seen_ids, "this id is unused and has no gender"

    assert 6 == len(seen_ids), "there are extra persons"


@pytest.mark.unit
def test_gender_as_race(tmp_path: Path):
    """
    it's the lazy user tests - but - the gender values are mapped to "race"

    this fails for a new reasson
    """

    

    # Get the package root directory
    assets = importlib.resources.files("tests")
    
    arg__input_dir = assets / 'regression/lazy-user'
    arg__rules_file = arg__input_dir / 'gender-as-race.json'

    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["src_PERSON.csv", "src_SMOKING.csv", "src_WEIGHT.csv"],
        arg__input_dir,
        arg__rules_file
    )



    if None is not result.exception:
        print(result.output)
        raise result.exception

    assert 0 == result.exit_code
    
    ### check data

    ##
    # backmap the ids.
    # source -> target
    # target -> source
    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")

    assert 6 == len(s2t), "shouldbe 5 people in the csv"

    # just check the 
    seen_ids = []
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        # check the ids
        assert person.person_id in t2s
        tid = t2s[person.person_id]
        assert tid not in seen_ids
        seen_ids.append(tid)

        print(
            f" {person.person_id=}"
        )

    assert "321" in seen_ids
    assert "789345" in seen_ids
    assert "6789" in seen_ids
    assert "339" in seen_ids, "this id was unused but has a gender"
    assert "319" in seen_ids, "this id is used with a blank gender"
    assert "289" in seen_ids, "this id is unused and has no gender"

    assert 6 == len(seen_ids), "there are extra persons"