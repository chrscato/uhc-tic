#!/usr/bin/env python3
"""Process a sample of UHC GA MRF file to validate end-to-end pipeline."""

import os
import time
from pathlib import Path
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.write.parquet_writer import ParquetWriter
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger
from tic_mrf_scraper.payers.uhc_ga import UhcGaHandler

def process_sample():
    """Process a small sample to validate the pipeline."""
    
    # Setup logging
    setup_logging("INFO")
    logger = get_logger(__name__)
    
    # Configuration
    url = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-08-01/2025-08-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_PPO---NDC_PPO-NDC_in-network-rates.json.gz"
    
    # Sample size limit - process first 1000 records only
    MAX_RECORDS = 1000
    
    # Output setup - mirror production structure
    base_dir = Path("output/uhc_ga_sample")
    for subdir in ["analytics", "organizations", "payers", "providers", "rates"]:
        (base_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    # Initialize writers for each output type
    writers = {
        "analytics": ParquetWriter(str(base_dir / "analytics/analytics.parquet"), batch_size=100),
        "organizations": ParquetWriter(str(base_dir / "organizations/organizations.parquet"), batch_size=100),
        "payers": ParquetWriter(str(base_dir / "payers/payers.parquet"), batch_size=100),
        "providers": ParquetWriter(str(base_dir / "providers/providers.parquet"), batch_size=100),
        "rates": ParquetWriter(str(base_dir / "rates/rates.parquet"), batch_size=100)
    }
    
    # Processing stats
    stats = {
        "records_processed": 0,
        "records_by_type": {k: 0 for k in writers.keys()},
        "start_time": time.time()
    }
    
    logger.info("Starting sample processing...")
    logger.info(f"URL: {url}")
    logger.info(f"Output directory: {base_dir}")
    
    try:
        # Initialize UHC GA handler
        handler = UhcGaHandler()
        
        # Stream and process records
        for raw_record in stream_parse_enhanced(url, payer="uhc_ga", handler=handler):
            stats["records_processed"] += 1
            
            # Process record without filtering
            normalized = normalize_tic_record(raw_record, set(), "uhc_ga")  # Empty whitelist = no filtering
            if normalized:
                # Determine record type and write to appropriate file
                record_type = normalized.get("record_type", "rates")
                if record_type in writers:
                    writers[record_type].write(normalized)
                    stats["records_by_type"][record_type] += 1
            
            # Log progress
            if stats["records_processed"] % 100 == 0:
                elapsed = time.time() - stats["start_time"]
                rate = stats["records_processed"] / elapsed
                logger.info(f"Processed {stats['records_processed']:,} records in {elapsed:.1f}s ({rate:.0f} records/sec)")
                logger.info("Records written by type:")
                for rtype, count in stats["records_by_type"].items():
                    logger.info(f"  {rtype}: {count}")
                
            # Check if we've reached sample limit
            if stats["records_processed"] >= MAX_RECORDS:
                logger.info(f"Reached sample limit of {MAX_RECORDS} records")
                break
        
        # Close all writers
        for writer in writers.values():
            writer.close()
        
        # Final statistics
        elapsed = time.time() - stats["start_time"]
        logger.info("\nSample processing complete!")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info(f"Records processed: {stats['records_processed']:,}")
        logger.info("\nRecords written by type:")
        for record_type, count in stats["records_by_type"].items():
            logger.info(f"  {record_type}: {count}")
        
        logger.info(f"\nOutput files written to: {base_dir}")
        
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Clean up writers on error
        for writer in writers.values():
            try:
                writer.close()
            except:
                pass

if __name__ == "__main__":
    print("🚀 Processing UHC GA MRF file (SAMPLE MODE)...")
    print("📋 Processing first 1,000 records without filtering")
    print("📁 Creating full output structure for validation")
    print()
    
    process_sample()
    
    print("\n✅ Done! Check output/uhc_ga_sample/ directory for Parquet files")