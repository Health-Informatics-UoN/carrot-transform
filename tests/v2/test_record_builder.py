import pytest
from unittest.mock import Mock, patch

from carrottransform.tools.record_builder import (
    TargetRecordBuilder,
    PersonRecordBuilder,
    StandardRecordBuilder,
    RecordBuilderFactory,
)
from carrottransform.tools.types import RecordContext
from carrottransform.tools.mapping_types import (
    V2TableMapping,
    ConceptMapping,
    PersonIdMapping,
    DateMapping,
)
from carrottransform.tools.omopcdm import OmopCDM
from carrottransform.tools.metrics import Metrics


@pytest.fixture
def mock_omopcdm():
    """Mock OMOP CDM for testing"""
    omop = Mock(spec=OmopCDM)
    omop.get_omop_datetime_linked_fields.return_value = {
        "observation_datetime": "observation_date"
    }
    omop.get_omop_date_field_components.return_value = {
        "birth_datetime": {
            "year": "year_of_birth",
            "month": "month_of_birth",
            "day": "day_of_birth",
        }
    }
    omop.get_omop_notnull_numeric_fields.return_value = [
        "person_id",
        "observation_concept_id",
    ]
    return omop


@pytest.fixture
def mock_metrics():
    """Mock metrics for testing"""
    metrics = Mock(spec=Metrics)
    metrics.increment_key_count = Mock()
    return metrics


@pytest.fixture
def sample_concept_mapping():
    """Sample concept mapping for testing"""
    return ConceptMapping(
        source_field="gender",
        value_mappings={
            "MALE": {"gender_concept_id": [8507]},
            "FEMALE": {"gender_concept_id": [8532]},
            "*": {"gender_concept_id": [0]},  # Unknown gender
        },
        original_value_fields=["gender_source_value"],
    )


@pytest.fixture
def sample_v2_mapping(sample_concept_mapping):
    """Sample V2 table mapping for testing"""
    return V2TableMapping(
        source_table="demographics.csv",
        concept_mappings={"gender": sample_concept_mapping},
        person_id_mapping=PersonIdMapping(
            source_field="patient_id", dest_field="person_id"
        ),
        date_mapping=DateMapping(
            source_field="birth_date", dest_fields=["birth_datetime"]
        ),
    )


@pytest.fixture
def sample_context(mock_omopcdm, mock_metrics, sample_v2_mapping):
    """Sample RecordContext for testing"""
    return RecordContext(
        tgtfilename="person",
        tgtcolmap={"person_id": 0, "gender_concept_id": 1, "gender_source_value": 2},
        v2_mapping=sample_v2_mapping,
        srcfield="gender",
        srcdata=["123", "MALE", "1990-01-01"],
        srccolmap={"patient_id": 0, "gender": 1, "birth_date": 2},
        srcfilename="demographics.csv",
        omopcdm=mock_omopcdm,
        metrics=mock_metrics,
    )


# Unit Tests for Abstract Base Class
class TestTargetRecordBuilder:
    def test_cannot_instantiate_abstract_class(self):
        """Test that abstract base class cannot be instantiated"""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            TargetRecordBuilder(Mock())

    def test_create_empty_record(self, sample_context):
        """Test empty record creation"""
        builder = StandardRecordBuilder(sample_context)
        record = builder.create_empty_record()

        assert len(record) == 3  # Based on tgtcolmap
        assert record[0] == "0"  # person_id (numeric field)
        assert record[1] == ""  # gender_concept_id
        assert record[2] == ""  # gender_source_value

    def test_apply_concept_mapping(self, sample_context):
        """Test concept mapping application"""
        builder = StandardRecordBuilder(sample_context)
        record = builder.create_empty_record()

        concept_combo = {"gender_concept_id": 8507}
        builder.apply_concept_mapping(record, concept_combo)

        assert record[1] == "8507"  # gender_concept_id position

    def test_apply_original_value_mappings(self, sample_context):
        """Test original value mappings"""
        builder = StandardRecordBuilder(sample_context)
        record = builder.create_empty_record()

        builder.apply_original_value_mappings(record, ["gender_source_value"], "MALE")

        assert record[2] == "MALE"  # gender_source_value position


# Unit Tests for StandardRecordBuilder
class TestStandardRecordBuilder:
    @pytest.mark.parametrize(
        "source_value,expected_concept",
        [
            ("MALE", 8507),
            ("FEMALE", 8532),
            ("OTHER", 0),  # Should use wildcard mapping
        ],
    )
    def test_build_records_with_valid_values(
        self, sample_context, source_value, expected_concept
    ):
        """Test building records with various valid source values"""
        # Update source data
        sample_context.srcdata[1] = source_value

        builder = StandardRecordBuilder(sample_context)
        result = builder.build_records()

        assert result.build_records is True
        assert len(result.records) == 1
        assert result.records[0][1] == str(expected_concept)  # gender_concept_id
        assert result.records[0][2] == source_value  # gender_source_value

    def test_build_records_with_invalid_value(self, sample_context):
        """Test building records with invalid source value"""
        sample_context.srcdata[1] = ""  # Empty value

        builder = StandardRecordBuilder(sample_context)
        result = builder.build_records()

        assert result.build_records is False
        assert len(result.records) == 0

    def test_build_records_with_no_mapping(self, sample_context):
        """Test building records when no concept mapping exists"""
        sample_context.srcfield = "unmapped_field"

        builder = StandardRecordBuilder(sample_context)
        result = builder.build_records()

        assert result.build_records is False
        assert len(result.records) == 0

    def test_multiple_concept_combinations(self, sample_context):
        """Test building records with multiple concept IDs"""
        # Create mapping with multiple concepts
        multi_concept_mapping = ConceptMapping(
            source_field="smoking",
            value_mappings={
                "SMOKER": {
                    "observation_concept_id": [3025315, 3021414],  # Multiple concepts
                    "value_as_concept_id": [4188539],  # Single concept
                }
            },
            original_value_fields=["observation_source_value"],
        )

        sample_context.v2_mapping.concept_mappings["smoking"] = multi_concept_mapping
        sample_context.srcfield = "smoking"
        sample_context.srcdata[1] = "SMOKER"
        sample_context.tgtcolmap = {
            "observation_concept_id": 0,
            "value_as_concept_id": 1,
            "observation_source_value": 2,
        }

        builder = StandardRecordBuilder(sample_context)
        result = builder.build_records()

        assert result.build_records is True
        assert len(result.records) == 2  # Two combinations

        # First combination
        assert result.records[0][0] == "3025315"  # First observation_concept_id
        assert result.records[0][1] == "4188539"  # value_as_concept_id (reused)

        # Second combination
        assert result.records[1][0] == "3021414"  # Second observation_concept_id
        assert result.records[1][1] == "4188539"  # value_as_concept_id (reused)


# Unit Tests for PersonRecordBuilder
class TestPersonRecordBuilder:
    def test_person_record_deduplication(self, sample_context):
        """Test that person records are not duplicated"""
        builder = PersonRecordBuilder(sample_context)

        # First call should succeed
        result1 = builder.build_records()
        assert result1.build_records is True
        assert len(result1.records) == 1

        # Second call should be skipped (cached)
        result2 = builder.build_records()
        assert result2.build_records is False
        assert len(result2.records) == 0

    def test_person_record_merges_all_fields(self, sample_context):
        """Test that person records merge mappings from all fields"""
        # Add additional concept mappings
        race_mapping = ConceptMapping(
            source_field="race",
            value_mappings={"WHITE": {"race_concept_id": [8527]}},
            original_value_fields=["race_source_value"],
        )
        sample_context.v2_mapping.concept_mappings["race"] = race_mapping

        # Update column maps and source data
        sample_context.srccolmap["race"] = 3
        sample_context.srcdata.append("WHITE")
        sample_context.tgtcolmap["race_concept_id"] = 3
        sample_context.tgtcolmap["race_source_value"] = 4

        builder = PersonRecordBuilder(sample_context)
        result = builder.build_records()

        assert result.build_records is True
        assert len(result.records) == 1

        record = result.records[0]
        assert record[1] == "8507"  # gender_concept_id
        assert record[2] == "MALE"  # gender_source_value
        assert len(record) > 4  # Should have race fields too


# Unit Tests for RecordBuilderFactory
class TestRecordBuilderFactory:
    def test_creates_person_builder_for_person_table(self, sample_context):
        """Test factory creates PersonRecordBuilder for person table"""
        sample_context.tgtfilename = "person"

        builder = RecordBuilderFactory.create_builder(sample_context)

        assert isinstance(builder, PersonRecordBuilder)

    def test_creates_standard_builder_for_other_tables(self, sample_context):
        """Test factory creates StandardRecordBuilder for non-person tables"""
        sample_context.tgtfilename = "observation"

        builder = RecordBuilderFactory.create_builder(sample_context)

        assert isinstance(builder, StandardRecordBuilder)

    def test_person_cache_is_shared(self, sample_context):
        """Test that person builders share the same cache"""
        sample_context.tgtfilename = "person"

        builder1 = RecordBuilderFactory.create_builder(sample_context)
        builder2 = RecordBuilderFactory.create_builder(sample_context)

        assert builder1.processed_cache is builder2.processed_cache

    def test_clear_person_cache(self, sample_context):
        """Test clearing the person cache"""
        sample_context.tgtfilename = "person"

        builder = RecordBuilderFactory.create_builder(sample_context)
        builder.processed_cache.add("test_key")

        RecordBuilderFactory.clear_person_cache()

        assert len(builder.processed_cache) == 0


# Integration Tests
class TestRecordBuilderIntegration:
    def test_date_mapping_integration(self, sample_context):
        """Test complete date mapping flow"""
        # Mock the date helper function
        with patch(
            "carrottransform.tools.record_builder.get_datetime_value"
        ) as mock_date:
            from datetime import datetime

            mock_date.return_value = datetime(1990, 1, 15)

            # Update context for date field components
            sample_context.tgtcolmap.update(
                {
                    "birth_datetime": 3,
                    "year_of_birth": 4,
                    "month_of_birth": 5,
                    "day_of_birth": 6,
                }
            )

            builder = StandardRecordBuilder(sample_context)
            result = builder.build_records()

            assert result.build_records is True
            record = result.records[0]

            # Check date components were set
            assert record[4] == "1990"  # year_of_birth
            assert record[5] == "1"  # month_of_birth
            assert record[6] == "15"  # day_of_birth

    def test_invalid_date_handling(self, sample_context):
        """Test handling of invalid dates"""
        with patch(
            "carrottransform.tools.record_builder.get_datetime_value"
        ) as mock_date:
            mock_date.return_value = None  # Invalid date

            builder = StandardRecordBuilder(sample_context)
            result = builder.build_records()

            assert result.build_records is False
            sample_context.metrics.increment_key_count.assert_called()


# Error Handling Tests
class TestRecordBuilderErrorHandling:
    def test_missing_source_field_in_column_map(self, sample_context):
        """Test handling when source field is missing from column map"""
        del sample_context.srccolmap["gender"]

        builder = StandardRecordBuilder(sample_context)
        result = builder.build_records()

        assert result.build_records is False

    def test_missing_dest_field_in_column_map(self, sample_context):
        """Test handling when destination field is missing"""
        del sample_context.tgtcolmap["gender_concept_id"]

        builder = StandardRecordBuilder(sample_context)
        result = builder.build_records()

        # Should still work, just skip that field
        assert result.build_records is True


# Performance Tests
class TestRecordBuilderPerformance:
    def test_large_concept_combinations(self, sample_context):
        """Test performance with many concept combinations"""
        # Create mapping with many concepts
        large_mapping = ConceptMapping(
            source_field="test_field",
            value_mappings={
                "TEST": {
                    f"concept_field_{i}": [j for j in range(100)] for i in range(10)
                }
            },
            original_value_fields=["test_source_value"],
        )

        sample_context.v2_mapping.concept_mappings["test_field"] = large_mapping
        sample_context.srcfield = "test_field"
        sample_context.srcdata[1] = "TEST"

        builder = StandardRecordBuilder(sample_context)
        result = builder.build_records()

        # Should handle large combinations efficiently
        assert result.build_records is True
        assert len(result.records) == 100  # Max concepts across all fields


if __name__ == "__main__":
    pytest.main([__file__])
