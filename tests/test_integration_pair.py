"""
copy of the test_integration - but - with only a few cases so it can be run quicker

"""

import logging
import re
from pathlib import Path

import pytest

import tests.click_tools as click_tools
import tests.csvrow as csvrow

##
# concept constants
# these are concept values that happen to be used in these tests. they're not a universal thing - they're just what is being used here.

#
concept__height = 903133
concept__weight = 903121

#
concept__mammogram_01 = 4031867
concept__mammogram_10 = 4031244
concept__mitoses = 4240068
concept__pitting = 4227224

# smoking constants
concept__active = 3959110
concept__quit = 3957361
concept__never = 35821355

# end of concept constants
##


@pytest.mark.integration
def test_observations(
    tmp_path: Path,
):
    (patient_csv, persons, observations, measurements, conditions, post_check) = (
        # patient_csv
        "observe_smoking/demos.csv",
        # persons
        4,
        # observations
        {
            123: {
                "2018-01-01": {"3959110": "active"},
                "2018-02-01": {"3959110": "active"},
                "2018-03-01": {"3957361": "quit"},
                "2018-04-01": {"3959110": "active"},
                "2018-05-01": {"35821355": "never"},
            },
            456: {
                "2009-01-01": {"35821355": "never"},
                "2009-02-01": {"35821355": "never"},
                "2009-03-01": {"3957361": "quit"},
            },
        },
        # measurements
        None,
        # conditions
        None,
        None,  # no extra post-test checks
    )

    from tests import test_integration

    test_integration.test_fixture(
        engine=True,
        pass__input__as_arg=True,
        pass__rules_file__as_arg=True,
        pass__person_file__as_arg=True,
        pass__output_dir__as_arg=True,
        pass__omop_ddl_file__as_arg=True,
        pass__omop_config_file__as_arg=True,
        tmp_path=tmp_path,
        patient_csv=patient_csv,
        persons=persons,
        observations=observations,
        measurements=measurements,
        conditions=conditions,
        post_check=post_check,
    )


@pytest.mark.integration
def test_conditions(
    tmp_path: Path,
):
    (patient_csv, persons, observations, measurements, conditions, post_check) = (
        # patient_csv
        "condition/persons.csv",
        # persons
        4,
        # observations
        None,
        # measurements
        None,
        # conditions
        {
            81: {
                "1998-02-01": {concept__pitting: 1},
                "1998-02-03": {concept__pitting: 13},
            },
            91: {
                "2001-01-03": {concept__pitting: 1},
                "2001-01-05": {concept__pitting: 7},
            },
        },
        None,  # no post-check
    )
    from tests import test_integration

    test_integration.test_fixture(
        engine=True,
        pass__input__as_arg=True,
        pass__rules_file__as_arg=True,
        pass__person_file__as_arg=True,
        pass__output_dir__as_arg=True,
        pass__omop_ddl_file__as_arg=True,
        pass__omop_config_file__as_arg=True,
        tmp_path=tmp_path,
        patient_csv=patient_csv,
        persons=persons,
        observations=observations,
        measurements=measurements,
        conditions=conditions,
        post_check=post_check,
    )
