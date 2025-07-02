"""
these test using carrot-transform end-to-end using fake spreadsheets and a rules.json

currently it's only checking birthdate and gender

to add more fields;
- add them to the existing .csv or create new ones
- produce a scan report
- upload the scan report to carrot-mapper and set rules to map the data
- download the rules.json, update this test, ensure that the fields all still match
"""

import pytest
import importlib.resources
import re

from pathlib import Path


import clicktools
import csvrow

print(
    'should this test people with birthdate on separte line?'
)

@pytest.mark.unit
def test_integration_test1(tmp_path: Path):

    arg__input_dir = Path(__file__).parent / "multi_mapping"
    arg__rules_file = arg__input_dir / "multi_mapping.json"


    ##
    #
    (result, output) = clicktools.click_mapstream(
        tmp_path,
        ["demos.csv"],
        arg__input_dir,
        arg__rules_file,
    )

    assert 0 == result.exit_code

    [s2t, t2s] = csvrow.back_get(output / "person_ids.tsv")
    assert(2 == len(s2t))
    assert(2 == len(t2s))

    # check the birthdateime and gender
    seen_ids: list[int] = []
    for person in csvrow.csv_rows(output / "person.tsv", "\t"):
        # check the ids
        assert person.person_id in t2s
        src_person_id = int(t2s[person.person_id])
        assert src_person_id not in seen_ids
        seen_ids.append(src_person_id)

        # dataset is small so just check it like this
        if 28 == src_person_id:
            assert '8532' == person.gender_concept_id
            assert '2020-09-12 00:00:00' == person.birth_datetime
        elif 82 == src_person_id:
            assert '8507' == person.gender_concept_id
            assert '2020-09-11 00:00:00' == person.birth_datetime
        else:
            raise Exception(f"unexpected {src_person_id=} in {person=}")
    assert 28 in seen_ids
    assert 82 in seen_ids

    # ethnicity is an observation
    for observation in csvrow.csv_rows(output / "observation.tsv", "\t"):
        assert observation.person_id in t2s
        src_person_id = int(t2s[observation.person_id])
        if 28 == src_person_id:
            assert '40618782' == observation.observation_concept_id
            assert '2020-09-12' == observation.observation_date
        elif 82 == src_person_id:
            assert '40637268' == observation.observation_concept_id
            assert '2020-09-11' == observation.observation_date
        else:
            raise Exception(f"unexpected {src_person_id=} in {observation=}")
