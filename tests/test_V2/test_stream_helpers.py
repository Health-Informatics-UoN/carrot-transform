"""
Tests for stream helpers module
"""

from unittest.mock import Mock

import pytest

from carrottransform.tools.mapping_types import ConceptMapping, V2RuleSet, V2TableMapping
from carrottransform.tools.stream_helpers import StreamingLookupCache


class TestStreamingLookupCache:
    """Test cases for StreamingLookupCache"""

    @pytest.fixture
    def mock_mappingrules(self):
        """Create a mock MappingRules object"""
        mappingrules = Mock()

        # Mock v2_mappings structure
        mappingrules.rules_data = V2RuleSet(
            metadata=None,
            cdm={
                "person": {
                    "Demographics.csv": V2TableMapping(
                        concept_mappings={
                            'ethnicity': ConceptMapping.model_validate({
                                'Black': {
                                    'race_source_concept_id': [38003600],
                                    'race_concept_id': [38003600]
                                },
                                'White': {
                                    'race_concept_id': [8527],
                                    'race_source_concept_id': [8527]
                                },
                                'original_value': ['race_source_value']
                            })
                        }
                    ),
                    "Persons.csv": V2TableMapping(
                        concept_mappings={
                            'ethnicity': ConceptMapping.model_validate({
                                'Black': {
                                    'race_source_concept_id': [38003600],
                                    'race_concept_id': [38003600]
                                },
                                'White': {
                                    'race_concept_id': [8527],
                                    'race_source_concept_id': [8527]
                                },
                                'original_value': ['race_source_value']
                            })
                        }
                    ),
                },
                "observation": {
                    "Demographics.csv": V2TableMapping(
                        concept_mappings={
                            "ethnicity": ConceptMapping.model_validate({
                                "White and Asian": {"observation_concept_id": [8507]},
                                "original_value": ["value"]
                            })
                        }
                    ),
                    "Surveys.csv": V2TableMapping(
                        concept_mappings={
                            "Y": ConceptMapping.model_validate({
                                "gender_source_value": {"gender_concept_id": [8507]},
                                "original_value": ["value"]
                            })
                        }
                    ),
                },
                "measurement": {
                    "LabResults.csv": V2TableMapping(
                        concept_mappings={
                            "Y": ConceptMapping.model_validate({
                                "gender_source_value": {"gender_concept_id": [8507]},
                                "original_value": ["value"]
                            })
                        }
                    ),
                }
            }
        )

        # Mock method returns
        mappingrules.get_all_infile_names.return_value = [
            "Demographics.csv",
            "Persons.csv",
            "Surveys.csv",
            "LabResults.csv",
        ]
        mappingrules.get_all_outfile_names.return_value = [
            "person",
            "observation",
            "measurement",
        ]

        # Mock metadata methods
        def mock_get_infile_date_person_id(filename):
            metadata_map = {
                "Demographics.csv": ("birth_date", "person_id"),
                "Persons.csv": ("date_of_birth", "id"),
                "Surveys.csv": ("survey_date", "participant_id"),
                "LabResults.csv": ("test_date", "patient_id"),
            }
            return metadata_map.get(filename, (None, None))

        def mock_get_infile_data_fields(filename):
            data_fields_map = {
                "Demographics.csv": {
                    "person": ["gender", "ethnicity"],
                    "observation": ["smoking_status"],
                },
                "Persons.csv": {"person": ["gender"]},
                "Surveys.csv": {"observation": ["mood", "anxiety"]},
                "LabResults.csv": {"measurement": ["glucose", "cholesterol"]},
            }
            return data_fields_map.get(filename, {})

        mappingrules.get_infile_date_person_id = mock_get_infile_date_person_id
        mappingrules.get_infile_data_fields = mock_get_infile_data_fields

        return mappingrules

    @pytest.fixture
    def mock_omopcdm(self):
        """Create a mock OmopCDM object"""
        omopcdm = Mock()

        # Mock OMOP metadata methods
        def mock_get_auto_number_field(table_name):
            auto_fields = {
                "person": "person_id",
                "observation": "observation_id",
                "measurement": "measurement_id",
            }
            return auto_fields.get(table_name)

        def mock_get_person_id_field(table_name):
            return "person_id"  # Most tables have person_id

        def mock_get_datetime_linked_fields(table_name):
            datetime_fields = {
                "person": {},
                "observation": {"observation_datetime": "observation_date"},
                "measurement": {"measurement_datetime": "measurement_date"},
            }
            return datetime_fields.get(table_name, {})

        def mock_get_date_field_components(table_name):
            component_fields = {
                "person": {
                    "birth_datetime": {
                        "year": "year_of_birth",
                        "month": "month_of_birth",
                        "day": "day_of_birth",
                    }
                },
                "observation": {},
                "measurement": {},
            }
            return component_fields.get(table_name, {})

        def mock_get_notnull_numeric_fields(table_name):
            numeric_fields = {
                "person": [
                    "person_id",
                    "gender_concept_id",
                    "year_of_birth",
                    "race_concept_id",
                    "ethnicity_concept_id",
                ],
                "observation": [
                    "observation_id",
                    "person_id",
                    "observation_concept_id",
                    "observation_type_concept_id",
                ],
                "measurement": [
                    "measurement_id",
                    "person_id",
                    "measurement_concept_id",
                    "measurement_type_concept_id",
                ],
            }
            return numeric_fields.get(table_name, [])

        omopcdm.get_omop_auto_number_field = mock_get_auto_number_field
        omopcdm.get_omop_person_id_field = mock_get_person_id_field
        omopcdm.get_omop_datetime_linked_fields = mock_get_datetime_linked_fields
        omopcdm.get_omop_date_field_components = mock_get_date_field_components
        omopcdm.get_omop_notnull_numeric_fields = mock_get_notnull_numeric_fields

        return omopcdm

    def test_initialization(self, mock_mappingrules, mock_omopcdm):
        """Test StreamingLookupCache initialization"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        assert cache.mappingrules == mock_mappingrules
        assert cache.omopcdm == mock_omopcdm
        assert hasattr(cache, "input_to_outputs")
        assert hasattr(cache, "file_metadata_cache")
        assert hasattr(cache, "target_metadata_cache")

    def test_build_input_to_output_lookup(self, mock_mappingrules, mock_omopcdm):
        """Test building input to output lookup table"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        expected_lookup = {
            "Demographics.csv": {"person", "observation"},
            "Persons.csv": {"person"},
            "Surveys.csv": {"observation"},
            "LabResults.csv": {"measurement"},
        }

        assert cache.input_to_outputs == expected_lookup

    def test_metadata_cache_building(self, mock_mappingrules, mock_omopcdm):
        """Test metadata cache building"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        # Check that all input files are in the cache
        assert "Demographics.csv" in cache.file_metadata_cache
        assert "Persons.csv" in cache.file_metadata_cache
        assert "Surveys.csv" in cache.file_metadata_cache
        assert "LabResults.csv" in cache.file_metadata_cache

        # Check that all output files are in the cache
        assert "person" in cache.target_metadata_cache
        assert "observation" in cache.target_metadata_cache
        assert "measurement" in cache.target_metadata_cache

    def test_file_metadata_cache(self, mock_mappingrules, mock_omopcdm):
        """Test file metadata cache building"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        # Check specific metadata for Demographics.csv
        demo_meta = cache.file_metadata_cache["Demographics.csv"]
        assert demo_meta["datetime_source"] == "birth_date"
        assert demo_meta["person_id_source"] == "person_id"
        assert "person" in demo_meta["data_fields"]
        assert "observation" in demo_meta["data_fields"]

    def test_target_metadata_cache_person(self, mock_mappingrules, mock_omopcdm):
        """Test target metadata cache building"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        # Check specific metadata for person
        person_meta = cache.target_metadata_cache["person"]
        assert person_meta["auto_num_col"] == "person_id"
        assert person_meta["person_id_col"] == "person_id"
        assert "birth_datetime" in person_meta["date_component_data"]

    def test_target_metadata_cache_measurement(self, mock_mappingrules, mock_omopcdm):
        """Test target metadata cache building"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        # Check specific metadata for measurement
        measurement_meta = cache.target_metadata_cache["measurement"]
        assert measurement_meta["auto_num_col"] == "measurement_id"
        assert "measurement_id" in measurement_meta["notnull_numeric_fields"]
        assert "person_id" in measurement_meta["notnull_numeric_fields"]
        assert "measurement_concept_id" in measurement_meta["notnull_numeric_fields"]
        assert (
            "measurement_type_concept_id" in measurement_meta["notnull_numeric_fields"]
        )
        assert "measurement_datetime" in measurement_meta["date_col_data"]

    def test_target_metadata_cache_observation(self, mock_mappingrules, mock_omopcdm):
        """Test target metadata cache building"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        # Check specific metadata for observation
        observation_meta = cache.target_metadata_cache["observation"]
        assert observation_meta["auto_num_col"] == "observation_id"
        assert "observation_datetime" in observation_meta["date_col_data"]
        assert "observation_concept_id" in observation_meta["notnull_numeric_fields"]
        assert "observation_id" in observation_meta["notnull_numeric_fields"]
        assert (
            "observation_type_concept_id" in observation_meta["notnull_numeric_fields"]
        )
        assert "person_id" in observation_meta["notnull_numeric_fields"]

    # Should anyone pass in empty mappings? shouldn't that fail?
    # def test_empty_mappings(self):
    #     """Test cache with empty mappings"""
    #     empty_mappingrules = Mock()
    #     empty_mappingrules.v2_mappings = {}
    #     empty_mappingrules.get_all_infile_names.return_value = []
    #     empty_mappingrules.get_all_outfile_names.return_value = []
    #
    #     empty_omopcdm = Mock()
    #
    #     cache = StreamingLookupCache(empty_mappingrules, empty_omopcdm)
    #
    #     assert cache.input_to_outputs == {}
    #     assert cache.file_metadata_cache == {}
    #     assert cache.target_metadata_cache == {}

    def test_cache_contains_all_required_fields(self, mock_mappingrules, mock_omopcdm):
        """Test that cache contains all required fields for processing"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        # Check file metadata has required fields
        for filename, metadata in cache.file_metadata_cache.items():
            assert "datetime_source" in metadata
            assert "person_id_source" in metadata
            assert "data_fields" in metadata
            assert isinstance(metadata["data_fields"], dict)

        # Check target metadata has required fields
        for target_file, metadata in cache.target_metadata_cache.items():
            assert "auto_num_col" in metadata
            assert "person_id_col" in metadata
            assert "date_col_data" in metadata
            assert "date_component_data" in metadata
            assert "notnull_numeric_fields" in metadata

    def test_input_to_outputs_handles_multiple_targets(
        self, mock_mappingrules, mock_omopcdm
    ):
        """Test that input files mapping to multiple outputs are handled correctly"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        # Demographics.csv should map to both person and observation
        demo_outputs = cache.input_to_outputs["Demographics.csv"]
        assert len(demo_outputs) == 2
        assert "person" in demo_outputs
        assert "observation" in demo_outputs

        # LabResults.csv should only map to measurement
        lab_outputs = cache.input_to_outputs["LabResults.csv"]
        assert len(lab_outputs) == 1
        assert "measurement" in lab_outputs

    def test_cache_types(self, mock_mappingrules, mock_omopcdm):
        """Test that cache has properties with correct types"""
        cache = StreamingLookupCache(mock_mappingrules, mock_omopcdm)

        # All lookups should be dictionaries
        assert isinstance(cache.input_to_outputs, dict)
        assert isinstance(cache.file_metadata_cache, dict)
        assert isinstance(cache.target_metadata_cache, dict)

        # Sets should be used for target lookups
        for target_set in cache.input_to_outputs.values():
            assert isinstance(target_set, set)
