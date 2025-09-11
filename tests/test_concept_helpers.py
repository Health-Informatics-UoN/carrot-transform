import datetime
import logging
import re
from pathlib import Path

import pytest

import carrottransform.tools.concept_helpers as helpers
import carrottransform.tools.date_helpers as date_helpers
import tests.click_tools as click_tools
import tests.csvrow as csvrow


@pytest.mark.unit
def test_Non():
    assert [] == helpers.generate_combinations(None)
