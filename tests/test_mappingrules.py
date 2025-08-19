import importlib.resources
import json
import re
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from carrottransform.tools.mappingrules import MappingRules
from carrottransform.tools.omopcdm import OmopCDM


def get_latest_omop_ddl():
    """Find the latest version of OMOP DDL file in the config directory."""

    config_dir = importlib.resources.files("carrottransform.config")
    sql_files = [
        f
        for f in config_dir.iterdir()
        if f.name.startswith("OMOPCDM_postgresql_") and f.name.endswith("_ddl.sql")
    ]

    # Extract version numbers and find the highest
    versions = []
    for file in sql_files:
        match = re.search(r"OMOPCDM_postgresql_(\d+\.\d+)_ddl\.sql", file.name)
        if match:
            # versions.append(match.group(1).split('.'), file)
            version = list(map(int, (match.group(1).split("."))))
            version.append(file)
            versions.append(version)
    # Return the file with highest version
    max_version = max(versions, key=lambda x: (x[0], x[1]))

    return max_version[2]


@pytest.fixture
def test_rules():
    """Fixture providing test rules"""
    return {
        "metadata": {
            "date_created": "2025-04-02T15:19:32.771599+00:00",
            "dataset": "test dataset",
        },
        "cdm": {
            "person": {
                "MALE": {
                    "person_id": {
                        "source_table": "demographic.csv",
                        "source_field": "person_id",
                    },
                    "gender_concept_id": {
                        "source_table": "demographic.csv",
                        "source_field": "sex",
                        "term_mapping": {"M": 8507},
                    },
                    "gender_source_value": {
                        "source_table": "demographic.csv",
                        "source_field": "sex",
                    },
                },
                "FEMALE": {
                    "person_id": {
                        "source_table": "demographic.csv",
                        "source_field": "person_id",
                    },
                    "gender_concept_id": {
                        "source_table": "demographic.csv",
                        "source_field": "sex",
                        "term_mapping": {"F": 8532},
                    },
                    "gender_source_value": {
                        "source_table": "demographic.csv",
                        "source_field": "sex",
                    },
                },
                "ETHNICITY": {
                    "race_concept_id": {
                        "source_table": "demographic.csv",
                        "source_field": "ethnicity",
                        "term_mapping": {"white": 8527, "asian": 8515, "black": 8516},
                    },
                    "race_source_value": {
                        "source_table": "demographic.csv",
                        "source_field": "ethnicity",
                    },
                },
            },
            "observation": {
                "SEX_OBS": {
                    "person_id": {
                        "source_table": "demographic.csv",
                        "source_field": "person_id",
                    },
                    "observation_concept_id": {
                        "source_table": "demographic.csv",
                        "source_field": "sex",
                        "term_mapping": 4135376,
                    },
                    "observation_source_value": {
                        "source_table": "demographic.csv",
                        "source_field": "sex",
                    },
                },
                "ETHNICITY_OBS": {
                    "person_id": {
                        "source_table": "demographic.csv",
                        "source_field": "person_id",
                    },
                    "observation_concept_id": {
                        "source_table": "demographic.csv",
                        "source_field": "ethnicity",
                        "term_mapping": 44803968,
                    },
                    "observation_source_value": {
                        "source_table": "demographic.csv",
                        "source_field": "ethnicity",
                    },
                },
            },
        },
    }


@pytest.fixture
def input_data():
    """Fixture providing test input data"""
    return pd.DataFrame(
        {
            "person_id": [1, 2, 3],
            "sex": ["M", "F", "M"],
            "ethnicity": ["white", "asian", "black"],
            "date_of_birth": ["2023-01-01", "2023-01-02", "2023-01-03"],
        }
    )


@pytest.fixture
def test_rules_file(test_rules):
    """Create a temporary file with the test rules."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(test_rules, f)
        return Path(f.name)


@pytest.fixture
def omopcdm():
    """Fixture providing OmopCDM instance using the latest available OMOP version."""
    ddl_path = get_latest_omop_ddl()
    with importlib.resources.as_file(
        importlib.resources.files("carrottransform.config") / "omop.json"
    ) as config_path:
        return OmopCDM(ddl_path, config_path)


def test_person_table_no_duplicates(omopcdm, test_rules_file, input_data):
    """Test that person table doesn't create duplicate rows for multiple mappings"""
    mapping_rules = MappingRules(test_rules_file, omopcdm)
    outfilenames, outdata = mapping_rules.parse_rules_src_to_tgt("demographic.csv")

    # Verify we got person table
    assert "person" in outfilenames

    # Check no duplicates in person mappings
    person_key = "demographic.csv~person"
    assert person_key in outdata
    assert len(outdata[person_key]) == 1  # Should only have one entry for person

    # Check mappings for first person
    person_mappings = outdata[person_key][0]
    assert "sex" in person_mappings
    assert "ethnicity" in person_mappings

    # Verify correct term mappings exist
    sex_mappings = person_mappings["sex"]
    assert isinstance(sex_mappings, dict)
    assert "M" in sex_mappings
    assert set(sex_mappings["M"]) == {"gender_concept_id~8507", "gender_source_value"}


def test_observation_table_multiple_rows(omopcdm, test_rules_file, input_data):
    """Test that observation table creates multiple rows for multiple mappings"""
    mapping_rules = MappingRules(test_rules_file, omopcdm)
    outfilenames, outdata = mapping_rules.parse_rules_src_to_tgt("demographic.csv")

    # Verify we got observation table
    assert "observation" in outfilenames

    # Check multiple entries for observations
    obs_keys = [k for k in outdata.keys() if "observation" in k]
    assert len(obs_keys) > 1  # Should have multiple observation mappings

    # Check each observation type has correct mappings
    for key in obs_keys:
        obs_data = outdata[key]
        assert len(obs_data) > 0
        # Check that we have the expected source fields
        assert "person_id" in obs_data[0]
        assert "sex" in obs_data[0] or "ethnicity" in obs_data[0]

        # Check the mappings for each source field
        for field, mappings in obs_data[0].items():
            if field == "person_id":
                assert mappings == ["person_id"]
            elif field == "sex":
                assert set(mappings) == {
                    "observation_concept_id~4135376",
                    "observation_source_value",
                }
            elif field == "ethnicity":
                assert set(mappings) == {
                    "observation_concept_id~44803968",
                    "observation_source_value",
                }


def test_mixed_person_and_observation(omopcdm, test_rules_file, input_data):
    """Test that person and observation tables handle multiple mappings correctly"""
    mapping_rules = MappingRules(test_rules_file, omopcdm)
    outfilenames, outdata = mapping_rules.parse_rules_src_to_tgt("demographic.csv")

    # Check both tables present
    assert "person" in outfilenames
    assert "observation" in outfilenames

    # Check person has single entry
    person_key = "demographic.csv~person"
    assert person_key in outdata
    assert len(outdata[person_key]) == 1

    # Check observations have multiple entries
    obs_keys = [k for k in outdata.keys() if "observation" in k]
    assert len(obs_keys) > 1


def test_person_data_correctness(omopcdm, test_rules_file, input_data):
    """Test that person data is mapped correctly without duplication"""
    mapping_rules = MappingRules(test_rules_file, omopcdm)
    outfilenames, outdata = mapping_rules.parse_rules_src_to_tgt("demographic.csv")

    person_key = "demographic.csv~person"
    person_mappings = outdata[person_key][0]

    # Check first person (Male, White)
    assert set(person_mappings["sex"]["M"]) == {
        "gender_concept_id~8507",
        "gender_source_value",
    }
    assert set(person_mappings["ethnicity"]["white"]) == {"race_concept_id~8527"}

    # Check second person (Female, Asian)
    assert set(person_mappings["sex"]["F"]) == {
        "gender_concept_id~8532",
        "gender_source_value",
    }
    assert set(person_mappings["ethnicity"]["asian"]) == {"race_concept_id~8515"}

    # Check third person (Male, Black)
    assert set(person_mappings["sex"]["M"]) == {
        "gender_concept_id~8507",
        "gender_source_value",
    }
    assert set(person_mappings["ethnicity"]["black"]) == {
        "race_concept_id~8516",
        "race_source_value",
    }


def test_output_data_writing(omopcdm, test_rules_file, input_data):
    """Test that data is written correctly to output files without duplication"""
    mapping_rules = MappingRules(test_rules_file, omopcdm)
    outfilenames, outdata = mapping_rules.parse_rules_src_to_tgt("demographic.csv")

    # Check person table output
    person_key = "demographic.csv~person"
    person_mappings = outdata[person_key][0]

    # Verify that each person_id only appears once in the mappings
    person_ids = set()
    for field, mappings in person_mappings.items():
        if field == "person_id":
            for mapping in mappings:
                person_ids.add(mapping)
    assert len(person_ids) == 1  # Should only have one person_id mapping

    # Verify individual field mappings for person table
    for person_id, row in input_data.iterrows():
        # Check sex mappings
        sex_value = row["sex"]
        if sex_value == "M":
            assert set(person_mappings["sex"]["M"]) == {
                "gender_concept_id~8507",
                "gender_source_value",
            }
        elif sex_value == "F":
            assert set(person_mappings["sex"]["F"]) == {
                "gender_concept_id~8532",
                "gender_source_value",
            }

        # Check ethnicity mappings
        ethnicity_value = row["ethnicity"]
        if ethnicity_value == "white":
            assert set(person_mappings["ethnicity"]["white"]) == {
                "race_concept_id~8527"
            }
        elif ethnicity_value == "asian":
            assert set(person_mappings["ethnicity"]["asian"]) == {
                "race_concept_id~8515"
            }
        elif ethnicity_value == "black":
            assert set(person_mappings["ethnicity"]["black"]) == {
                "race_concept_id~8516",
                "race_source_value",
            }

    # Check observation table output
    obs_keys = [k for k in outdata.keys() if "observation" in k]
    for key in obs_keys:
        obs_data = outdata[key]
        # Verify that each person_id only appears once per observation type
        person_ids = set()
        for field, mappings in obs_data[0].items():
            if field == "person_id":
                person_ids.add(mappings[0])
        assert (
            len(person_ids) == 1
        )  # Should only have one person_id mapping per observation type

        # Verify individual field mappings for observations
        for person_id, row in input_data.iterrows():
            sex_value = row["sex"]
            ethnicity_value = row["ethnicity"]

            # Check sex observation mappings
            if "sex" in key:
                # The mappings are stored in the 'sex' field of obs_data[0]
                assert "sex" in obs_data[0]
                sex_mappings = obs_data[0]["sex"]
                assert "observation_concept_id~4135376" in sex_mappings
                assert "observation_source_value" in sex_mappings

            # Check ethnicity observation mappings
            if "ethnicity" in key:
                # The mappings are stored in the 'ethnicity' field of obs_data[0]
                assert "ethnicity" in obs_data[0]
                ethnicity_mappings = obs_data[0]["ethnicity"]
                assert "observation_concept_id~44803968" in ethnicity_mappings
                assert "observation_source_value" in ethnicity_mappings

    # Verify that the total number of output records matches expected
    total_records = 0
    for key, data in outdata.items():
        if "person" in key:
            total_records += 1  # One record per person
        elif "observation" in key:
            total_records += len(
                input_data
            )  # One record per person per observation type

    expected_records = 1 + (
        2 * len(input_data)
    )  # 1 person record + 2 observation types per person
    assert total_records == expected_records


if __name__ == "__main__":
    pytest.main([__file__])
