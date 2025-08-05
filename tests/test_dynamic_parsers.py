"""Tests for dynamic MRF parsers."""

import pytest
from typing import Dict, Any
from tic_mrf_scraper.parsers.prov_ref_infile import ProvRefInfileParser
from tic_mrf_scraper.parsers.prov_ref_url import ProvRefUrlParser
from tic_mrf_scraper.stream.dynamic_parser import DynamicStreamingParser
from io import StringIO

@pytest.fixture
def anthem_data() -> Dict[str, Any]:
    """Sample Anthem-style MRF data."""
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
        ],
        "in_network": [
            {
                "billing_code": "0011U",
                "billing_code_type": "HCPCS",
                "description": "PRESCRIPTION DRUG MONITORING",
                "negotiated_rates": [
                    {
                        "provider_references": [51.0000003653],
                        "negotiated_prices": [
                            {
                                "negotiated_rate": 125.87,
                                "service_code": ["81"],
                                "billing_class": "professional"
                            }
                        ]
                    }
                ]
            }
        ]
    }

@pytest.fixture
def cigna_data() -> Dict[str, Any]:
    """Sample Cigna-style MRF data."""
    return {
        "reporting_entity_name": "Cigna Health Life Insurance Company",
        "provider_references": [
            {
                "provider_group_id": 3015429,
                "location": "https://example.com/prov_ref/3015429.json"
            }
        ],
        "in_network": [
            {
                "billing_code": "E1008",
                "description": "Pwr seat combo pwr shear",
                "negotiated_rates": [
                    {
                        "provider_references": [3015429],
                        "negotiated_prices": [
                            {
                                "negotiated_rate": 429.52,
                                "service_code": ["01", "02"],
                                "billing_class": "professional",
                                "billing_code_modifier": ["RR"]
                            }
                        ]
                    }
                ]
            }
        ]
    }

@pytest.fixture
def mock_provider_data() -> Dict[str, Any]:
    """Mock provider reference data."""
    return {
        "provider_groups": [
            {
                "npi": [1234567890, 9876543210],
                "tin": {"type": "npi", "value": "1111111111"}
            }
        ]
    }

def test_prov_ref_infile_parser(anthem_data):
    """Test inline provider reference parser."""
    parser = ProvRefInfileParser("TEST_PAYER", {"0011U"})
    records = parser.parse(anthem_data)

    assert len(records) == 2  # One record per NPI
    
    record = records[0]
    assert record["service_code"] == "0011U"
    assert record["negotiated_rate"] == 125.87
    assert record["service_codes"] == ["81"]
    assert record["billing_class"] == "professional"
    assert record["provider_npi"] in ["1457079634", "1174217079"]
    assert record["provider_tin"] == "1063183572"
    assert record["payer"] == "TEST_PAYER"

def test_prov_ref_url_parser(cigna_data, mock_provider_data, mocker):
    """Test external provider reference parser."""
    # Mock fetcher
    mock_fetcher = mocker.Mock()
    mock_fetcher.fetch_all.return_value = {
        "https://example.com/prov_ref/3015429.json": mock_provider_data
    }

    parser = ProvRefUrlParser("TEST_PAYER", {"E1008"}, fetcher=mock_fetcher)
    records = parser.parse(cigna_data)

    assert len(records) == 2  # One record per NPI
    
    record = records[0]
    assert record["service_code"] == "E1008"
    assert record["negotiated_rate"] == 429.52
    assert record["service_codes"] == ["01", "02"]
    assert record["billing_class"] == "professional"
    assert record["provider_npi"] in ["1234567890", "9876543210"]
    assert record["provider_tin"] == "1111111111"
    assert record["payer"] == "TEST_PAYER"

def test_dynamic_streaming_parser(anthem_data):
    """Test dynamic streaming parser."""
    parser = DynamicStreamingParser("TEST_PAYER", {"0011U"})
    
    # Convert data to stream
    stream = StringIO(str(anthem_data))
    records = list(parser.parse_stream(stream))

    assert len(records) == 2  # One record per NPI
    
    record = records[0]
    assert record["service_code"] == "0011U"
    assert record["negotiated_rate"] == 125.87
    assert record["service_codes"] == ["81"]
    assert record["billing_class"] == "professional"
    assert record["provider_npi"] in ["1457079634", "1174217079"]
    assert record["provider_tin"] == "1063183572"
    assert record["payer"] == "TEST_PAYER"

def test_parser_cpt_whitelist_filtering(anthem_data):
    """Test CPT code whitelist filtering."""
    parser = ProvRefInfileParser("TEST_PAYER", {"99999"})  # Different code
    records = parser.parse(anthem_data)
    assert len(records) == 0  # No records due to filtering

def test_parser_error_handling(anthem_data):
    """Test parser error handling."""
    # Remove required fields
    del anthem_data["provider_references"]
    del anthem_data["in_network"]

    parser = ProvRefInfileParser("TEST_PAYER")
    records = parser.parse(anthem_data)
    assert len(records) == 0  # No records due to missing data