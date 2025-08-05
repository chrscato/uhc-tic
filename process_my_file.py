#!/usr/bin/env python3
"""Process a single MRF file to Parquet format."""

import os
import time
from pathlib import Path
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.write.parquet_writer import ParquetWriter
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

def process_single_file():
    """Process a single in-network file."""
    
    # Setup logging
    setup_logging("INFO")
    logger = get_logger(__name__)
    
    # Configuration
    # EDIT THIS: Replace with your in-network file URL
    url = "YOUR_IN_NETWORK_FILE_URL"
    
    # EDIT THIS: Replace with your payer name (e.g., "bcbs_il", "bcbs_fl", "centene_fidelis", etc.)
    payer_name = "YOUR_PAYER_NAME"
    
    # EDIT THIS: Add your target CPT/HCPCS codes
    cpt_whitelist = {
        # Add your codes here, for example:
        # "70450",  # CT HEAD/BRAIN
        # "72148",  # MRI SPINAL
        # "99213",  # OFFICE VISIT
    }
    
    # Output setup
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / f"{payer_name}_rates.parquet"
    
    # Initialize writer
    writer = ParquetWriter(str(output_file), batch_size=1000)
    
    # Processing stats
    stats = {
        "records_processed": 0,
        "records_written": 0,
        "unique_codes_found": set(),
        "rates_by_code": {},
        "start_time": time.time()
    }
    
    logger.info("Starting single file processing...")
    logger.info(f"URL: {url}")
    logger.info(f"Output: {output_file}")
    
    try:
        # Stream and process records
        for raw_record in stream_parse_enhanced(url, payer=payer_name):
            stats["records_processed"] += 1
            
            # Track what codes we're seeing
            billing_code = raw_record.get("billing_code")
            if billing_code:
                stats["unique_codes_found"].add(billing_code)
            
            # Normalize the record
            normalized = normalize_tic_record(raw_record, cpt_whitelist, payer_name)
            if normalized:
                writer.write(normalized)
                stats["records_written"] += 1
                
                # Track rates by code
                code = normalized["service_code"]
                rate = normalized["negotiated_rate"]
                if code not in stats["rates_by_code"]:
                    stats["rates_by_code"][code] = []
                stats["rates_by_code"][code].append(rate)
                
                # Log progress for successful writes
                if stats["records_written"] % 100 == 0:
                    logger.info(f"Written {stats['records_written']} records...")
            
            # Log progress for processing
            if stats["records_processed"] % 10000 == 0:
                elapsed = time.time() - stats["start_time"]
                logger.info(f"Processed {stats['records_processed']} records in {elapsed:.1f}s")
                logger.info(f"Found codes: {sorted(list(stats['unique_codes_found']))[:10]}...")
        
        # Close writer
        writer.close()
        
        # Final statistics
        elapsed = time.time() - stats["start_time"]
        logger.info("Processing complete!")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info(f"Records processed: {stats['records_processed']:,}")
        logger.info(f"Records written: {stats['records_written']:,}")
        
        # Show rates by code
        logger.info("\nRates by code:")
        for code in sorted(stats["rates_by_code"].keys()):
            rates = stats["rates_by_code"][code]
            min_rate = min(rates)
            max_rate = max(rates)
            avg_rate = sum(rates) / len(rates)
            logger.info(f"{code}: {len(rates)} rates, min=${min_rate:.2f}, max=${max_rate:.2f}, avg=${avg_rate:.2f}")
            
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    print("🚀 Processing single MRF file...")
    print("📋 Edit the script to set your file URL, payer name, and CPT codes")
    print("⏱️  Processing time will vary based on file size...")
    print()
    
    process_single_file()
    
    print("\n✅ Done! Check the output/ directory for your Parquet file.")