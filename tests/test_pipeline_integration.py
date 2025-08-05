"""Integration tests for dynamic MRF parsing pipeline."""

import pytest
import json
from typing import Dict, Any
from pathlib import Path
import tempfile
import os

from tic_mrf_scraper.schema.detector import SchemaDetector
from tic_mrf_scraper.parsers.factory import ParserFactory
from tic_mrf_scraper.stream.dynamic_parser import DynamicStreamingParser
from production_etl_pipeline import ProductionETLPipeline, ETLConfig

@pytest.fixture
def test_config():
    """Create test ETL configuration."""
    return ETLConfig(
        payer_endpoints={
            "TEST_PAYER": "http://example.com/test_index.json"
        },
        cpt_whitelist=["0011U", "E1008"],
        batch_size=100,
        parallel_workers=1,
        test_mode=True,
        local_output_dir=tempfile.mkdtemp()
    )

@pytest.fixture
def anthem_data():
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
def cigna_data():
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
def mock_provider_data():
    """Mock provider reference data."""
    return {
        "provider_groups": [
            {
                "npi": [1234567890, 9876543210],
                "tin": {"type": "npi", "value": "1111111111"}
            }
        ]
    }

def test_schema_detection_integration(anthem_data, cigna_data):
    """Test schema detection in pipeline context."""
    detector = SchemaDetector()
    
    # Test Anthem format detection
    schema_type = detector.detect_schema(anthem_data)
    assert schema_type == "prov_ref_infile"
    
    # Test Cigna format detection
    schema_type = detector.detect_schema(cigna_data)
    assert schema_type == "prov_ref_url"

def test_dynamic_parser_integration(anthem_data, test_config):
    """Test dynamic parser integration with pipeline."""
    # Save test data
    test_file = Path(test_config.local_output_dir) / "test_mrf.json"
    with open(test_file, "w") as f:
        json.dump(anthem_data, f)
    
    # Initialize pipeline
    pipeline = ProductionETLPipeline(test_config)
    
    # Process file
    file_info = {
        "url": str(test_file),
        "size": os.path.getsize(test_file)
    }
    
    stats = pipeline.process_mrf_file_enhanced(
        "test_uuid",
        "TEST_PAYER",
        file_info,
        1,
        1
    )
    
    # Verify results
    assert stats["schema_type"] == "prov_ref_infile"
    assert stats["records_processed"] > 0
    assert stats["rate_records"] > 0
    assert stats["provider_records"] > 0

def test_provider_resolution_integration(cigna_data, mock_provider_data, test_config, mocker):
    """Test provider reference resolution in pipeline."""
    # Mock URL fetcher
    mock_fetch = mocker.patch("tic_mrf_scraper.fetch.multi_url_fetcher.MultiUrlFetcher.fetch_all")
    mock_fetch.return_value = {
        "https://example.com/prov_ref/3015429.json": mock_provider_data
    }
    
    # Save test data
    test_file = Path(test_config.local_output_dir) / "test_mrf.json"
    with open(test_file, "w") as f:
        json.dump(cigna_data, f)
    
    # Initialize pipeline
    pipeline = ProductionETLPipeline(test_config)
    
    # Process file
    file_info = {
        "url": str(test_file),
        "size": os.path.getsize(test_file),
        "provider_reference_url": "https://example.com/prov_ref/3015429.json"
    }
    
    stats = pipeline.process_mrf_file_enhanced(
        "test_uuid",
        "TEST_PAYER",
        file_info,
        1,
        1
    )
    
    # Verify results
    assert stats["schema_type"] == "prov_ref_url"
    assert stats["records_processed"] > 0
    assert stats["rate_records"] > 0
    assert stats["provider_records"] > 0
    
    # Verify provider resolution
    mock_fetch.assert_called_once()

def test_record_normalization_integration(anthem_data, test_config):
    """Test record normalization in pipeline context."""
    # Save test data
    test_file = Path(test_config.local_output_dir) / "test_mrf.json"
    with open(test_file, "w") as f:
        json.dump(anthem_data, f)
    
    # Initialize pipeline
    pipeline = ProductionETLPipeline(test_config)
    
    # Process file
    file_info = {
        "url": str(test_file),
        "size": os.path.getsize(test_file)
    }
    
    stats = pipeline.process_mrf_file_enhanced(
        "test_uuid",
        "TEST_PAYER",
        file_info,
        1,
        1
    )
    
    # Verify rate records
    assert stats["rate_records"] == 2  # One per NPI
    
    # Check parquet output
    parquet_dir = Path(test_config.local_output_dir) / "parquet"
    assert (parquet_dir / "rates").exists()
    assert (parquet_dir / "providers").exists()
    assert (parquet_dir / "organizations").exists()

def test_error_handling_integration(test_config):
    """Test error handling in pipeline context."""
    # Create invalid MRF file
    invalid_data = {"invalid": "structure"}
    test_file = Path(test_config.local_output_dir) / "invalid_mrf.json"
    with open(test_file, "w") as f:
        json.dump(invalid_data, f)
    
    # Initialize pipeline
    pipeline = ProductionETLPipeline(test_config)
    
    # Process file
    file_info = {
        "url": str(test_file),
        "size": os.path.getsize(test_file)
    }
    
    stats = pipeline.process_mrf_file_enhanced(
        "test_uuid",
        "TEST_PAYER",
        file_info,
        1,
        1
    )
    
    # Verify error handling
    assert stats["schema_type"] == "unknown"
    assert len(stats["errors"]) > 0
    assert stats["records_processed"] == 0