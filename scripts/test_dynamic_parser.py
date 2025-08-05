#!/usr/bin/env python3
"""Quick test script for dynamic MRF parsing."""

import os
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from tic_mrf_scraper.schema.detector import SchemaDetector
from tic_mrf_scraper.parsers.factory import ParserFactory
from tic_mrf_scraper.stream.dynamic_parser import DynamicStreamingParser
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

logger = get_logger(__name__)

def analyze_mrf_file(file_path: str, payer_name: str = "TEST_PAYER"):
    """
    Analyze and parse a single MRF file.

    Args:
        file_path: Path to MRF JSON file
        payer_name: Name of payer for record creation
    """
    logger.info("analyzing_file", file=file_path)
    
    # Load file
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Detect schema
    detector = SchemaDetector()
    schema_type = detector.detect_schema(data)
    logger.info("detected_schema", schema_type=schema_type)
    
    # Create parser
    parser = ParserFactory().create_parser(data)
    if not parser:
        logger.error("failed_to_create_parser")
        return
    
    # Initialize streaming parser
    dynamic_parser = DynamicStreamingParser(payer_name=payer_name)
    
    # Process records
    record_count = 0
    provider_count = 0
    rate_count = 0
    
    start_time = datetime.now()
    
    try:
        for record in dynamic_parser.parse_stream(
            file_path,
            schema_type=schema_type,
            parser=parser
        ):
            record_count += 1
            
            # Count providers and rates
            if "provider_npi" in record:
                provider_count += 1
            if "negotiated_rate" in record:
                rate_count += 1
            
            # Print sample record
            if record_count == 1:
                logger.info("sample_record", record=record)
            
            # Progress update every 1000 records
            if record_count % 1000 == 0:
                logger.info("processing_progress", records=record_count)
    
    except Exception as e:
        logger.error("parsing_failed", error=str(e))
        return
    
    processing_time = (datetime.now() - start_time).total_seconds()
    
    # Print summary
    summary = {
        "file": file_path,
        "schema_type": schema_type,
        "total_records": record_count,
        "provider_records": provider_count,
        "rate_records": rate_count,
        "processing_time": f"{processing_time:.2f}s"
    }
    
    logger.info("processing_complete", **summary)
    
    # Save summary
    output_dir = Path("parser_test_results")
    output_dir.mkdir(exist_ok=True)
    
    summary_file = output_dir / f"summary_{Path(file_path).stem}.json"
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info("saved_summary", file=str(summary_file))

def main():
    """Run test script."""
    parser = argparse.ArgumentParser(description="Test dynamic MRF parsing")
    parser.add_argument("--file", required=True, help="MRF file to process")
    parser.add_argument("--payer", default="TEST_PAYER", help="Payer name")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Run analysis
    analyze_mrf_file(args.file, args.payer)

if __name__ == "__main__":
    main()