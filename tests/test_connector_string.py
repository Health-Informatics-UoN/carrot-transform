import logging

import pytest

from carrottransform.tools import args

#
logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "parameters, expected",
    [
        [
            {
                "example": "",
                "example_mode": "minio",
                "example_user": "minio_user_c91f6832f2d525dd",
                "example_pass": "minio_pass_ea1b5ab58c2bc631",
                "example_protocol": "http",
                "example_host": "127.0.0.1",
                "example_port": "58384",
                "example_bucket": "test-bucket-bae51f90a75dddab",
                "example_folder": "",  # (empty in this case)
            },
            "minio:minio_user_c91f6832f2d525dd:minio_pass_ea1b5ab58c2bc631@http://127.0.0.1:58384/test-bucket-bae51f90a75dddab",
        ],
        [
            {
                "example": "",
                "example_mode": "minio",
                "example_user": "minio_user_c91f6832f2d525dd",
                "example_pass": "asdas",
                "example_protocol": "ftps", # can minio use ftps? well let's test that
                "example_host": "127.0.0.1",
                "example_port": "584",
                "example_bucket": "test-buckasdet-bae51f90a75dddab",
                "example_folder": "itchy/tasty/",
            },
            "minio:minio_user_c91f6832f2d525dd:asdas@ftps://127.0.0.1:584/test-buckasdet-bae51f90a75dddab/itchy/tasty",
        ],
        [
            {
                "example": "",
                "example_mode": "postgresql", # all sql connections are done like this i guess
                "example_user": "fred",
                "example_pass": "duplamub",
                "example_host": "167.5.20.9",
                "example_port": "3145",
                "example_name": "the_end_of_summer",
            },
            "postgresql://fred:duplamub@167.5.20.9:3145/the_end_of_summer",
        ],
    ],
)
@pytest.mark.unit
def test_doit(parameters, expected):
    # test it with the split up string
    actual = args.parse_connector_string("example", parameters)
    assert expected == actual

    blank = {}
    for k in parameters:
        if "_" not in k:
            blank[k] = expected
        else:
            blank[k] = ""

    # test it with a normla string
    actual = args.parse_connector_string("example", parameters)
    assert expected == actual
