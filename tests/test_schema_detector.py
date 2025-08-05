"""Tests for schema detection functionality."""

import pytest
from tic_mrf_scraper.schema.detector import SchemaDetector

@pytest.fixture
def detector():
    """Create schema detector fixture."""
    return SchemaDetector()

@pytest.fixture
def inline_provider_data():
    """Sample data with inline provider references."""
    return {
        "reporting_entity_name": "Anthem Blue Cross and Blue Shield Colorado",
        "provider_references": [
            {
                "provider_group_id": 51.0000003653,
                "provider_groups": [
                    {
                        "npi": [1457079634, 1174217079],
                        "tin": {"type": "npi", "value": "1063183572"}
                    }
                ]
            }
        ]
    }

@pytest.fixture
def url_provider_data():
    """Sample data with URL provider references."""
    return {
        "reporting_entity_name": "Cigna Health Life Insurance Company",
        "provider_references": [
            {
                "provider_group_id": 3015429,
                "location": "https://example.com/prov_reference_file/2025-08_3015429.json"
            }
        ]
    }

def test_detect_inline_provider_schema(detector, inline_provider_data):
    """Test detection of inline provider references."""
    schema_type = detector.detect_schema(inline_provider_data)
    assert schema_type == "prov_ref_infile"

def test_detect_url_provider_schema(detector, url_provider_data):
    """Test detection of URL provider references."""
    schema_type = detector.detect_schema(url_provider_data)
    assert schema_type == "prov_ref_url"

def test_validate_schema_inline(detector, inline_provider_data):
    """Test validation of inline provider schema."""
    assert detector.validate_schema(inline_provider_data, "prov_ref_infile")
    assert not detector.validate_schema(inline_provider_data, "prov_ref_url")

def test_validate_schema_url(detector, url_provider_data):
    """Test validation of URL provider schema."""
    assert detector.validate_schema(url_provider_data, "prov_ref_url")
    assert not detector.validate_schema(url_provider_data, "prov_ref_infile")

def test_detect_schema_empty_data(detector):
    """Test schema detection with empty data."""
    assert detector.detect_schema({}) is None
    assert detector.detect_schema({"provider_references": []}) is None

def test_detect_schema_invalid_data(detector):
    """Test schema detection with invalid data."""
    invalid_data = {
        "provider_references": [
            {
                "provider_group_id": 123,
                # Missing both location and provider_groups
            }
        ]
    }
    assert detector.detect_schema(invalid_data) is None