import pytest

import carrottransform.tools.concept_helpers as helpers


@pytest.mark.unit
def test_Non():
    assert [] == helpers.generate_combinations(None)
