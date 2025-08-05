#!/usr/bin/env python3
"""Process a single UHC GA MRF file to Parquet format."""

import os
import time
import gc
import psutil
from pathlib import Path
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.write.parquet_writer import ParquetWriter
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def process_single_file():
    """Process the UHC GA file with production CPT codes."""
    
    # Setup logging
    setup_logging("INFO")
    logger = get_logger(__name__)
    
    # Configuration
    url = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-08-01/2025-08-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_PPO---NDC_PPO-NDC_in-network-rates.json.gz"
    
    # Production CPT whitelist from config.yaml
    cpt_whitelist = {
        '99213', '99214', '72148', '73721', '70450', '0202U', '0240U', '0241U',
        '10005', '10006', '99500', '99502', '99503', '99505', '99506', '99509',
        '99510', '99511', '99512', '99600', '99602', '92521', '92522', '92523',
        '97162', '97163', '97164', '97165', '97166', '97167', '97168', 'G0008',
        'G0009', 'G0010', 'M0201', 'T1019', 'T1020', 'T1021', 'T1022', 'G0151',
        'G0152', 'G0153', 'G0156', 'G0299', 'G0300', 'S5125', 'S9131', 'A9272',
        '70030', '70100', '70110', '70120', '70130', '70134', '70140', '70150',
        '70160', '70190', '70200', '70210', '70220', '70240', '70250', '70260',
        '70328', '70330', '70336', '70360', '70370', '70450', '70460', '70470',
        '70480', '70481', '70482', '70486', '70487', '70488', '70490', '70491',
        '70492', '70540', '70542', '70543', '70544', '70551', '70552', '70553',
        '71010', '71020', '71021', '71022', '71023', '71030', '71034', '71035',
        '71046', '71100', '71101', '71110', '71111', '71120', '71130', '71250',
        '71260', '71270', '71275', '71550', '71551', '71552', '72020', '72040',
        '72050', '72052', '72070', '72072', '72074', '72080', '72082', '72083',
        '72084', '72100', '72110', '72114', '72120', '72125', '72126', '72127',
        '72128', '72129', '72130', '72131', '72132', '72133', '72141', '72142',
        '72146', '72147', '72148', '72149', '72156', '72157', '72158', '72170',
        '72190', '72192', '72193', '72194', '72195', '72196', '72197', '72200',
        '72202', '72220', '73000', '73010', '73020', '73030', '73040', '73050',
        '73060', '73070', '73080', '73090', '73092', '73100', '73110', '73120',
        '73130', '73140', '73200', '73201', '73202', '73218', '73219', '73220',
        '73221', '73222', '73223', '73501', '73502', '73503', '73521', '73522',
        '73523', '73525', '73551', '73552', '73560', '73561', '73562', '73564',
        '73565', '73580', '73590', '73592', '73600', '73610', '73620', '73630',
        '73650', '73660', '73700', '73701', '73702', '73718', '73719', '73720',
        '73721', '73722', '73723', '74000', '74010', '74020', '74150', '74160',
        '74170', '74176', '74177', '74178', '74181', '74182', '74183', '74220',
        '74240', '74241', '74245', '74246', '74247', '74249', '74250', '74251',
        '74270', '74280', '74400', '74410', '74740', '75561', '75571', '76010',
        '76080', '76376', '76377', '76536', '76604', '76642', '76700', '76705',
        '76770', '76775', '76830', '76856', '76857', '76870', '76882', '77002',
        '77003', '77012', '77072', '77073', '77074', '77075', '77076', '77077',
        '77080', '93971', '95870', '95885', '95886', '95887', '95909', '95910',
        '95911', '95912', '95913', '99202', '99203', '99204', '99214', '78306'
    }
    
    # Output setup
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "uhc_ga_rates.parquet"
    
    # Initialize writer with larger batch size for better performance
    writer = ParquetWriter(str(output_file), batch_size=5000)  # Increased from 1000
    
    # Processing stats
    stats = {
        "records_processed": 0,
        "records_written": 0,
        "unique_codes_found": set(),
        "rates_by_code": {},
        "start_time": time.time(),
        "last_gc_time": time.time(),
        "last_memory": get_memory_usage()
    }
    
    # Memory monitoring thresholds
    MEMORY_CHECK_INTERVAL = 50000  # Check memory every 50K records
    MEMORY_GROWTH_THRESHOLD_MB = 500  # Alert if memory grows by 500MB
    GC_INTERVAL_SECONDS = 300  # Force GC every 5 minutes
    
    logger.info("Starting single file processing...")
    logger.info(f"Initial memory usage: {stats['last_memory']:.1f} MB")
    logger.info(f"URL: {url}")
    logger.info(f"Output: {output_file}")
    
    try:
        # Stream and process records
        for raw_record in stream_parse_enhanced(url, payer="uhc_ga"):
            stats["records_processed"] += 1
            
            # Memory monitoring
            if stats["records_processed"] % MEMORY_CHECK_INTERVAL == 0:
                current_memory = get_memory_usage()
                memory_growth = current_memory - stats["last_memory"]
                current_time = time.time()
                
                # Log memory stats
                logger.info(f"Memory usage: {current_memory:.1f} MB (change: {memory_growth:+.1f} MB)")
                
                # Force garbage collection if needed
                if memory_growth > MEMORY_GROWTH_THRESHOLD_MB or \
                   current_time - stats["last_gc_time"] > GC_INTERVAL_SECONDS:
                    logger.info("Running garbage collection...")
                    gc.collect()
                    stats["last_gc_time"] = current_time
                    stats["last_memory"] = get_memory_usage()
                    logger.info(f"Memory after GC: {stats['last_memory']:.1f} MB")
                else:
                    stats["last_memory"] = current_memory
            
            # Early filtering - skip if code not in whitelist
            billing_code = raw_record.get("billing_code")
            if not billing_code or billing_code not in cpt_whitelist:
                continue
            
            # Track unique codes
            stats["unique_codes_found"].add(billing_code)
            
            # Normalize the record
            normalized = normalize_tic_record(raw_record, cpt_whitelist, "uhc_ga")
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
                if stats["records_written"] % 1000 == 0:  # Increased from 100
                    logger.info(f"Written {stats['records_written']} records...")
            
            # Log progress for processing
            if stats["records_processed"] % 50000 == 0:  # Increased from 10000
                elapsed = time.time() - stats["start_time"]
                rate = stats["records_processed"] / elapsed
                logger.info(f"Processed {stats['records_processed']:,} records in {elapsed:.1f}s ({rate:.0f} records/sec)")
                logger.info(f"Found codes: {sorted(list(stats['unique_codes_found']))[:10]}...")
        
        # Close writer
        writer.close()
        
        # Final statistics
        elapsed = time.time() - stats["start_time"]
        logger.info("\nProcessing complete!")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info(f"Records processed: {stats['records_processed']:,}")
        logger.info(f"Records written: {stats['records_written']:,}")
        logger.info(f"Processing rate: {stats['records_processed'] / elapsed:.0f} records/sec")
        logger.info(f"Final memory usage: {get_memory_usage():.1f} MB")
        
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
    print("🚀 Processing UHC GA MRF file...")
    print("📋 Using production CPT whitelist")
    print("⏱️  This may take several minutes depending on file size...")
    print()
    
    process_single_file()
    
    print("\n✅ Done! Check the output/ directory for your Parquet file.")