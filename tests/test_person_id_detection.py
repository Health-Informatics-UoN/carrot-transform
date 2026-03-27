import logging
from pathlib import Path

from carrottransform.tools.person_helpers import person_id_file_v1 as v1
from carrottransform.tools.person_helpers import person_id_file_v2 as v2
from carrottransform.tools.person_helpers import IndeterminatePersonFiles
import pytest

import carrottransform.tools.sources as sources
from carrottransform.tools import file_helpers

logger = logging.getLogger(__name__)

carrot_root = Path(__file__).parent.parent


@pytest.mark.parametrize(
    "rules_file, person_id_file, expected",
    [
        ("tests/test_data/wrong-person-table-rules.json", v1, "src_PERSON"),
        ("tests/test_V2/rules-v2.json", v2, "src_PERSON"),
        ("tests/test_data/args/no-person-rules.json", v1, []),
        (
            "tests/test_data/args/reads-from-other-tables.json",
            v1,
            ["demos_f", "demos_m"],
        ),
        ("tests/test_data/condition/mapping.json", v1, "persons"),
        ("tests/test_data/duplications/transform-rules.json", v1, "src_PERSON"),
        ("tests/test_data/floats/rules.json", v1, "src_PERSON"),
        ("tests/test_data/integration_test1/transform-rules.json", v1, "src_PERSON"),
        ("tests/test_data/mapping_person/multi_mapping.json", v1, "demos"),
        ("tests/test_data/measure_weight_height/mapping.json", v1, "persons"),
        (
            "tests/test_data/mireda_key_error/original_rules.json",
            v1,
            ["demographics_child_gold", "infant_data_gold"],
        ),
        ("tests/test_data/observe_smoking/mapping.json", v1, "demos"),
        ("tests/test_data/only_m/v1-rules.json", v1, "patients"),
    ],
)
def test_person_file(rules_file, person_id_file, expected):
    """
    Test that the person_id column is detected correctly.
    """
    obtained: str | list[str]
    try:
        obtained = person_id_file(carrot_root / rules_file)
    except IndeterminatePersonFiles as e:
        obtained = sorted(e._files)

    match expected:
        case str(person_file):
            assert person_file == obtained
        case list(expected):
            expected = sorted(expected)
            assert expected == obtained
