#!/usr/bin/env python3
"""Helper script for migrating to dynamic MRF parsing."""

import os
import json
import argparse
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
from tqdm import tqdm

from tic_mrf_scraper.schema.detector import SchemaDetector
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

logger = get_logger(__name__)

@dataclass
class PayerAnalysis:
    """Analysis results for a payer's MRF files."""
    payer_name: str
    total_files: int
    schema_types: Dict[str, int]
    sample_files: Dict[str, List[str]]
    migration_confidence: float
    recommended_order: List[str]

def analyze_payer_files(
    payer_name: str,
    file_list: List[str],
    sample_size: int = 5
) -> PayerAnalysis:
    """
    Analyze MRF files for a payer to guide migration.

    Args:
        payer_name: Name of payer
        file_list: List of MRF file URLs
        sample_size: Number of sample files per schema type

    Returns:
        PayerAnalysis with migration recommendations
    """
    detector = SchemaDetector()
    schema_counts = {}
    schema_samples = {}
    total = len(file_list)
    
    logger.info("analyzing_payer", payer=payer_name, files=total)
    
    for file_url in tqdm(file_list, desc=f"Analyzing {payer_name}"):
        try:
            # Load and detect schema
            with open(file_url, 'rb') as f:
                data = json.load(f)
                schema_type = detector.detect_schema(data) or "unknown"
            
            # Update counts
            schema_counts[schema_type] = schema_counts.get(schema_type, 0) + 1
            
            # Store sample files
            if schema_type not in schema_samples:
                schema_samples[schema_type] = []
            if len(schema_samples[schema_type]) < sample_size:
                schema_samples[schema_type].append(file_url)
                
        except Exception as e:
            logger.error("file_analysis_failed", file=file_url, error=str(e))
            schema_counts["error"] = schema_counts.get("error", 0) + 1
    
    # Calculate migration confidence
    known_schemas = sum(
        count for schema, count in schema_counts.items()
        if schema not in ("unknown", "error")
    )
    confidence = known_schemas / total if total > 0 else 0
    
    # Determine recommended migration order
    ordered_schemas = sorted(
        [s for s in schema_counts.keys() if s not in ("unknown", "error")],
        key=lambda s: schema_counts[s],
        reverse=True
    )
    
    return PayerAnalysis(
        payer_name=payer_name,
        total_files=total,
        schema_types=schema_counts,
        sample_files=schema_samples,
        migration_confidence=confidence,
        recommended_order=ordered_schemas
    )

def generate_migration_plan(
    analysis: PayerAnalysis,
    output_dir: str,
    payer_config: Optional[Dict[str, Any]] = None
):
    """
    Generate migration plan and configuration updates.

    Args:
        analysis: PayerAnalysis results
        output_dir: Output directory for migration artifacts
        payer_config: Optional existing payer configuration
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Basic plan structure
    plan = {
        "payer_name": analysis.payer_name,
        "analysis_date": datetime.now().isoformat(),
        "total_files": analysis.total_files,
        "schema_distribution": analysis.schema_types,
        "migration_confidence": analysis.migration_confidence,
        "phases": []
    }
    
    # Generate phases based on schema types
    for schema in analysis.recommended_order:
        phase = {
            "schema_type": schema,
            "file_count": analysis.schema_types[schema],
            "sample_files": analysis.sample_files.get(schema, []),
            "validation_steps": [
                f"Validate sample files using validate_dynamic_pipeline.py",
                f"Compare output with legacy parser results",
                f"Verify provider reference handling",
                f"Check rate record normalization"
            ],
            "rollback_plan": [
                "Revert to legacy parser if validation fails",
                "Log issues for schema-specific fixes",
                "Update schema detection rules if needed"
            ]
        }
        plan["phases"].append(phase)
    
    # Add handling for unknown/error files
    if "unknown" in analysis.schema_types or "error" in analysis.schema_types:
        plan["cleanup_required"] = {
            "unknown_files": analysis.schema_types.get("unknown", 0),
            "error_files": analysis.schema_types.get("error", 0),
            "sample_files": {
                "unknown": analysis.sample_files.get("unknown", []),
                "error": analysis.sample_files.get("error", [])
            }
        }
    
    # Generate updated payer config
    if payer_config:
        updated_config = payer_config.copy()
        updated_config["schema_types"] = list(analysis.recommended_order)
        updated_config["requires_dynamic_parsing"] = analysis.migration_confidence > 0.8
        
        with open(os.path.join(output_dir, "updated_payer_config.json"), "w") as f:
            json.dump(updated_config, f, indent=2)
    
    # Save migration plan
    with open(os.path.join(output_dir, "migration_plan.json"), "w") as f:
        json.dump(plan, f, indent=2)
    
    # Generate summary report
    summary = f"""
    Migration Plan Summary for {analysis.payer_name}
    =============================================
    
    Analysis Results:
    - Total Files: {analysis.total_files}
    - Migration Confidence: {analysis.migration_confidence:.2%}
    - Schema Distribution:
    {json.dumps(analysis.schema_types, indent=4)}
    
    Recommended Migration Order:
    {chr(10).join(f"  {i+1}. {schema}" for i, schema in enumerate(analysis.recommended_order))}
    
    Migration Phases:
    {chr(10).join(f"  Phase {i+1}: Migrate {phase['file_count']} files with {phase['schema_type']} schema" for i, phase in enumerate(plan['phases']))}
    
    Validation Requirements:
    1. Run validation script on sample files
    2. Compare output with legacy parser
    3. Verify provider reference handling
    4. Check rate record normalization
    
    Rollback Plan:
    1. Revert to legacy parser if validation fails
    2. Log issues for schema-specific fixes
    3. Update schema detection rules if needed
    
    Next Steps:
    1. Review sample files for each schema type
    2. Run validation script on sample files
    3. Update payer configuration
    4. Begin phased migration
    """
    
    with open(os.path.join(output_dir, "migration_summary.txt"), "w") as f:
        f.write(summary)

def main():
    """Run migration helper script."""
    parser = argparse.ArgumentParser(description="MRF Parser Migration Helper")
    parser.add_argument("--payer-name", required=True, help="Payer name")
    parser.add_argument("--file-list", required=True, help="File containing MRF URLs")
    parser.add_argument("--output-dir", default="migration_plans",
                       help="Output directory for migration artifacts")
    parser.add_argument("--payer-config", help="Existing payer configuration file")
    parser.add_argument("--sample-size", type=int, default=5,
                       help="Number of sample files per schema type")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Load file list
    with open(args.file_list) as f:
        file_list = [line.strip() for line in f]
    
    # Load existing config if provided
    payer_config = None
    if args.payer_config:
        with open(args.payer_config) as f:
            payer_config = json.load(f)
    
    # Run analysis
    analysis = analyze_payer_files(
        args.payer_name,
        file_list,
        args.sample_size
    )
    
    # Generate migration plan
    generate_migration_plan(
        analysis,
        os.path.join(args.output_dir, args.payer_name),
        payer_config
    )
    
    logger.info("migration_plan_generated",
                payer=args.payer_name,
                output_dir=args.output_dir,
                confidence=analysis.migration_confidence)

if __name__ == "__main__":
    main()