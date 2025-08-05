#!/usr/bin/env python3
"""Validation script for dynamic MRF parsing pipeline."""

import os
import json
import argparse
from typing import Dict, Any, List, Tuple
from dataclasses import dataclass
from datetime import datetime
import pandas as pd

from tic_mrf_scraper.schema.detector import SchemaDetector
from tic_mrf_scraper.parsers.factory import ParserFactory
from tic_mrf_scraper.stream.dynamic_parser import DynamicStreamingParser
from tic_mrf_scraper.payers import get_handler
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

logger = get_logger(__name__)

@dataclass
class ValidationResult:
    """Results from validation run."""
    file_url: str
    schema_type: str
    legacy_records: int
    dynamic_records: int
    matching_records: int
    mismatched_records: List[Tuple[Dict[str, Any], Dict[str, Any]]]
    processing_time_legacy: float
    processing_time_dynamic: float
    memory_usage_legacy: float
    memory_usage_dynamic: float

def validate_file(
    file_url: str,
    payer_name: str,
    provider_ref_url: str = None,
    cpt_whitelist: set = None
) -> ValidationResult:
    """
    Validate a single MRF file using both legacy and dynamic parsers.

    Args:
        file_url: URL to MRF file
        payer_name: Name of payer
        provider_ref_url: Optional URL to provider reference file
        cpt_whitelist: Optional set of allowed CPT codes

    Returns:
        ValidationResult containing comparison metrics
    """
    start_time = datetime.now()
    
    # Process with legacy parser
    legacy_records = []
    legacy_start = start_time
    handler = get_handler(payer_name)
    
    for record in stream_parse_enhanced(
        file_url,
        payer_name,
        provider_ref_url,
        handler
    ):
        legacy_records.append(record)
    
    legacy_time = (datetime.now() - legacy_start).total_seconds()
    legacy_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

    # Process with dynamic parser
    dynamic_records = []
    dynamic_start = datetime.now()
    
    # Detect schema
    detector = SchemaDetector()
    parser_factory = ParserFactory()
    
    with open(file_url, 'rb') as f:
        sample_data = json.load(f)
        schema_type = detector.detect_schema(sample_data)
        parser = parser_factory.create_parser(sample_data)

    dynamic_parser = DynamicStreamingParser(
        payer_name=payer_name,
        cpt_whitelist=cpt_whitelist
    )

    for record in dynamic_parser.parse_stream(
        file_url,
        schema_type=schema_type,
        parser=parser,
        provider_ref_url=provider_ref_url
    ):
        dynamic_records.append(record)

    dynamic_time = (datetime.now() - dynamic_start).total_seconds()
    dynamic_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

    # Compare results
    matching = 0
    mismatched = []
    
    for leg_rec, dyn_rec in zip(sorted(legacy_records), sorted(dynamic_records)):
        if _compare_records(leg_rec, dyn_rec):
            matching += 1
        else:
            mismatched.append((leg_rec, dyn_rec))

    return ValidationResult(
        file_url=file_url,
        schema_type=schema_type or "unknown",
        legacy_records=len(legacy_records),
        dynamic_records=len(dynamic_records),
        matching_records=matching,
        mismatched_records=mismatched,
        processing_time_legacy=legacy_time,
        processing_time_dynamic=dynamic_time,
        memory_usage_legacy=legacy_memory,
        memory_usage_dynamic=dynamic_memory
    )

def _compare_records(record1: Dict[str, Any], record2: Dict[str, Any]) -> bool:
    """Compare two records for functional equivalence."""
    keys_to_compare = [
        "billing_code",
        "negotiated_rate",
        "provider_npi",
        "provider_tin",
        "service_codes"
    ]
    
    try:
        return all(
            record1.get(key) == record2.get(key)
            for key in keys_to_compare
        )
    except Exception:
        return False

def generate_validation_report(results: List[ValidationResult], output_dir: str):
    """Generate validation report in multiple formats."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Summary stats
    summary = {
        "total_files": len(results),
        "schema_types": {},
        "total_legacy_records": sum(r.legacy_records for r in results),
        "total_dynamic_records": sum(r.dynamic_records for r in results),
        "total_matching": sum(r.matching_records for r in results),
        "avg_time_improvement": sum(
            r.processing_time_legacy - r.processing_time_dynamic 
            for r in results
        ) / len(results),
        "avg_memory_improvement": sum(
            r.memory_usage_legacy - r.memory_usage_dynamic
            for r in results
        ) / len(results)
    }
    
    # Count schema types
    for result in results:
        summary["schema_types"][result.schema_type] = \
            summary["schema_types"].get(result.schema_type, 0) + 1
    
    # Save summary
    with open(os.path.join(output_dir, "validation_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    
    # Create detailed DataFrame
    df = pd.DataFrame([{
        "file_url": r.file_url,
        "schema_type": r.schema_type,
        "legacy_records": r.legacy_records,
        "dynamic_records": r.dynamic_records,
        "matching_records": r.matching_records,
        "mismatch_count": len(r.mismatched_records),
        "time_improvement": r.processing_time_legacy - r.processing_time_dynamic,
        "memory_improvement": r.memory_usage_legacy - r.memory_usage_dynamic
    } for r in results])
    
    # Save detailed report
    df.to_csv(os.path.join(output_dir, "validation_details.csv"), index=False)
    
    # Generate HTML report
    html_report = f"""
    <html>
    <head>
        <title>MRF Parser Validation Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .summary {{ background: #f5f5f5; padding: 20px; margin-bottom: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background: #f0f0f0; }}
        </style>
    </head>
    <body>
        <h1>MRF Parser Validation Report</h1>
        <div class="summary">
            <h2>Summary</h2>
            <p>Total Files: {summary['total_files']}</p>
            <p>Schema Types: {json.dumps(summary['schema_types'], indent=2)}</p>
            <p>Total Records (Legacy): {summary['total_legacy_records']}</p>
            <p>Total Records (Dynamic): {summary['total_dynamic_records']}</p>
            <p>Matching Records: {summary['total_matching']}</p>
            <p>Average Time Improvement: {summary['avg_time_improvement']:.2f}s</p>
            <p>Average Memory Improvement: {summary['avg_memory_improvement']:.2f}MB</p>
        </div>
        {df.to_html()}
    </body>
    </html>
    """
    
    with open(os.path.join(output_dir, "validation_report.html"), "w") as f:
        f.write(html_report)

def main():
    """Run validation script."""
    parser = argparse.ArgumentParser(description="Validate dynamic MRF parsing")
    parser.add_argument("--input-file", required=True, help="Input MRF file to validate")
    parser.add_argument("--payer-name", required=True, help="Payer name")
    parser.add_argument("--provider-ref-url", help="Provider reference URL")
    parser.add_argument("--output-dir", default="validation_results",
                       help="Output directory for validation results")
    parser.add_argument("--cpt-codes", help="File containing CPT whitelist")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Load CPT whitelist if provided
    cpt_whitelist = None
    if args.cpt_codes:
        with open(args.cpt_codes) as f:
            cpt_whitelist = set(line.strip() for line in f)
    
    # Run validation
    result = validate_file(
        args.input_file,
        args.payer_name,
        args.provider_ref_url,
        cpt_whitelist
    )
    
    # Generate report
    generate_validation_report([result], args.output_dir)
    
    logger.info("validation_complete", 
                output_dir=args.output_dir,
                schema_type=result.schema_type,
                matching_records=result.matching_records)

if __name__ == "__main__":
    main()