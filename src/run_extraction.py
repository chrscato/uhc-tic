"""Main runner script for MRF data extraction."""

import os
from pathlib import Path
from datetime import datetime
from typing import Optional

from utils import (
    get_memory_usage,
    download_to_temp,
    setup_output_dir
)
from extract_providers import ProviderExtractor
from extract_rates import RateExtractor

def run_extraction(
    source_path: str,
    output_dir: Optional[str] = None,
    max_items: Optional[int] = None
) -> None:
    """
    Run complete MRF extraction pipeline.
    
    Args:
        source_path: URL or local path to MRF file
        output_dir: Optional custom output directory
        max_items: Optional limit on number of items to process
    """
    start_time = datetime.now()
    print(f"\n🚀 STARTING MRF EXTRACTION PIPELINE")
    print(f"=" * 60)
    
    # Setup
    output_dir = setup_output_dir(output_dir or "output")
    print(f"📁 Output directory: {output_dir}")
    print(f"🧠 Available RAM: {get_memory_usage():.1f} MB")
    
    # Download if URL
    if source_path.startswith(('http://', 'https://')):
        temp_path = download_to_temp(source_path)
        print(f"📦 Downloaded to: {temp_path}")
    else:
        temp_path = source_path
        print(f"📄 Using local file: {temp_path}")
    
    try:
        # Step 1: Extract Providers
        provider_extractor = ProviderExtractor(batch_size=100)
        provider_results = provider_extractor.process_file(temp_path, output_dir)
        
        # Step 2: Extract Rates
        rate_extractor = RateExtractor(batch_size=5)
        rate_results = rate_extractor.process_file(temp_path, output_dir, max_items)
        
        # Final Summary
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n✨ EXTRACTION PIPELINE COMPLETE")
        print(f"⏱️  Total time: {elapsed:.1f} seconds")
        print(f"\n📊 SUMMARY:")
        print(f"   Providers processed: {provider_results['stats']['providers_processed']:,}")
        print(f"   Items processed: {rate_results['stats']['items_processed']:,}")
        print(f"   Rates generated: {rate_results['stats']['rates_generated']:,}")
        print(f"\n📁 OUTPUT FILES:")
        print(f"   Providers: {provider_results['output_path']}")
        print(f"   Rates: {rate_results['output_path']}")
        
    finally:
        # Cleanup temp file if downloaded
        if source_path.startswith(('http://', 'https://')):
            try:
                os.unlink(temp_path)
                print(f"\n🧹 Cleaned up temporary file")
            except Exception:
                pass

if __name__ == "__main__":
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description="Extract data from MRF files")
    parser.add_argument("source", help="URL or path to MRF file")
    parser.add_argument("--output-dir", help="Custom output directory")
    parser.add_argument("--max-items", type=int, help="Limit number of items to process")
    
    args = parser.parse_args()
    
    run_extraction(
        args.source,
        output_dir=args.output_dir,
        max_items=args.max_items
    )