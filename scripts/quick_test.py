#!/usr/bin/env python3
"""Quick test script for dynamic MRF parsing."""

import json
from pathlib import Path
from tic_mrf_scraper.schema.detector import SchemaDetector
from tic_mrf_scraper.parsers.factory import ParserFactory
from tic_mrf_scraper.stream.dynamic_parser import DynamicStreamingParser
from tic_mrf_scraper.fetch.multi_url_fetcher import MultiUrlFetcher

class MockUrlFetcher(MultiUrlFetcher):
    """Mock URL fetcher for testing."""
    
    def fetch_all(self, urls):
        """Return mock provider data."""
        return {
            url: {
                "provider_groups": [
                    {
                        "npi": ["1234567890", "9876543210"],
                        "tin": {"type": "ein", "value": "123456789"}
                    }
                ]
            }
            for url in urls
        }

def test_mrf_file(file_path: str):
    """Test parsing a single MRF file."""
    print(f"\nTesting file: {file_path}")
    print("-" * 50)
    
    # Load file
    with open(file_path) as f:
        data = json.load(f)
    
    # Detect schema
    detector = SchemaDetector()
    schema_type = detector.detect_schema(data)
    print(f"Detected schema type: {schema_type}")
    
    # Create parser with mock URL fetcher
    parser_factory = ParserFactory()
    parser = parser_factory.create_parser(data, payer_name="TEST_PAYER")
    if not parser:
        print("Failed to create parser")
        return
    
    # Set mock fetcher if needed
    if schema_type == "prov_ref_url":
        parser.fetcher = MockUrlFetcher()
    
    # Parse records
    print("\nParsing records...")
    record_count = 0
    provider_count = 0
    rate_count = 0
    
    dynamic_parser = DynamicStreamingParser(payer_name="TEST_PAYER")
    
    for record in dynamic_parser.parse_stream(file_path, schema_type=schema_type, parser=parser):
        if record_count == 0:
            print("\nSample record:")
            print(json.dumps(record, indent=2))
        
        record_count += 1
        if "provider_npi" in record:
            provider_count += 1
        if "negotiated_rate" in record:
            rate_count += 1
    
    print(f"\nResults:")
    print(f"Total records: {record_count}")
    print(f"Provider records: {provider_count}")
    print(f"Rate records: {rate_count}")

def main():
    """Test multiple sample files."""
    # Test smallest file first
    test_mrf_file("mrf_samples/mrf_sample_20250805_101518.json")
    
    # Test medium file
    test_mrf_file("mrf_samples/mrf_sample_20250805_100333.json")

if __name__ == "__main__":
    main()