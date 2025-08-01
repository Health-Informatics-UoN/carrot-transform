
import pytest

from pathlib import Path


import click_tools as clicktools
import csvrow


import re































import carrottransform.tools.sources as sources






@pytest.mark.integration
def test_measure_weight_height(tmp_path: Path):
    """
    this test checks to be sure that two measurements (width and height) don't "collide" or interfere with eachother
    """

    (result, output, person_id_source2target, person_id_target2source) = (
        clicktools.click_test(
            tmp_path,
            "measure_weight_height/persons.csv",

            engine = True,
        )
    )

    raise Exception(
        f"sit thing {str(output).replace('\\', '/')=}"
    )

    assert 4 == len(person_id_source2target)
    assert 4 == len(person_id_target2source)

    # this is the data we're expecting to see
    height = 903133
    weight = 903121
    expect = {
        21: {
            "2021-12-02": {height: 123, weight: 31},
            "2021-12-01": {height: 122},
            "2021-12-03": {height: 12, weight: 12},
            "2022-12-01": {weight: 28},
        },
        81: {
            "2022-12-02": {height: 23, weight: 27},
            "2021-03-01": {height: 92},
            "2020-03-01": {weight: 92},
        },
        91: {
            "2021-02-03": {height: 72, weight: 12},
            "2021-02-01": {weight: 1},
        },
    }

    # validate the results
    measurements: int = 0
    for measurement in csvrow.csv_rows(output / "measurement.tsv", "\t"):
        measurements += 1

        src_person_id = int(person_id_target2source[measurement.person_id])
        date = measurement.measurement_date
        concept = int(measurement.measurement_concept_id)
        value = int(measurement.value_as_number)

        assert src_person_id in expect, f"{src_person_id=} {measurement=} "
        assert date in expect[src_person_id], f"{src_person_id=} {date=} {measurement=}"
        assert concept in expect[src_person_id][date], (
            f"{src_person_id=} {date=} {concept=} {measurement=}"
        )

        assert value == expect[src_person_id][date][concept], (
            f"{date=} {concept=} {measurement=}"
        )

    assert 13 == measurements


