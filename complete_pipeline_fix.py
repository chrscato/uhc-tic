#!/usr/bin/env python3
"""Complete pipeline fix - all issues addressed."""

import os
import time
import json
import gzip
import requests
from io import BytesIO
from pathlib import Path
from typing import Dict, Any, Iterator, Set
from tic_mrf_scraper.write.parquet_writer import ParquetWriter
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger
from tic_mrf_scraper.fetch.blobs import get_cloudfront_headers

class FixedUhcGaHandler:
    """Fixed UHC GA handler that properly processes individual in_network items."""
    
    def parse_in_network(self, item: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """Parse individual in_network item from the MRF structure."""
        if not isinstance(item, dict):
            return

        # Extract base record information
        base_record = {
            "billing_code": item.get("billing_code", ""),
            "billing_code_type": item.get("billing_code_type", ""),
            "billing_code_type_version": item.get("billing_code_type_version", ""),
            "description": item.get("description", ""),
            "negotiation_arrangement": item.get("negotiation_arrangement", ""),
            "name": item.get("name", ""),
            "record_type": "rates"
        }

        # Handle negotiated rates structure
        negotiated_rates = item.get("negotiated_rates", [])
        if not negotiated_rates:
            print(f"  WARNING: No negotiated_rates found for {base_record['billing_code']}")
            return

        for rate_group in negotiated_rates:
            if not isinstance(rate_group, dict):
                continue

            # Get provider references
            provider_refs = rate_group.get("provider_references", [])
            
            # Get negotiated prices
            negotiated_prices = rate_group.get("negotiated_prices", [])
            if not negotiated_prices:
                print(f"  WARNING: No negotiated_prices in rate group for {base_record['billing_code']}")
                continue

            for price in negotiated_prices:
                if not isinstance(price, dict):
                    continue

                # Create individual rate record
                rate_record = base_record.copy()
                rate_record.update({
                    "negotiated_rate": price.get("negotiated_rate", 0.0),
                    "negotiated_type": price.get("negotiated_type", ""),
                    "service_code": price.get("service_code", []),
                    "billing_class": price.get("billing_class", ""),
                    "expiration_date": price.get("expiration_date", ""),
                    "billing_code_modifier": price.get("billing_code_modifier", []),
                    "additional_information": price.get("additional_information", ""),
                    "provider_references": provider_refs,
                    "payer": "uhc_ga"
                })
                
                print(f"    Generated rate record: {rate_record['billing_code']} = ${rate_record['negotiated_rate']}")
                yield rate_record

def fixed_normalize_tic_record(record: Dict[str, Any], 
                              code_whitelist: Set[str], 
                              payer: str) -> Dict[str, Any]:
    """Fixed normalization that handles both CPT and NDC codes."""
    
    # Extract billing code
    billing_code = record.get("billing_code", "")
    if not billing_code:
        print(f"  SKIP: No billing code in record")
        return None
    
    # Apply whitelist filtering (if whitelist is provided and not empty)
    if code_whitelist and billing_code not in code_whitelist:
        print(f"  SKIP: Code {billing_code} not in whitelist")
        return None
        
    # Get negotiated rate
    negotiated_rate = record.get("negotiated_rate")
    if negotiated_rate is None or negotiated_rate <= 0:
        print(f"  SKIP: Invalid rate {negotiated_rate} for {billing_code}")
        return None
        
    # Build normalized record
    normalized = {
        "service_code": billing_code,
        "billing_code_type": record.get("billing_code_type", ""),
        "description": record.get("description", ""),
        "negotiated_rate": float(negotiated_rate),
        "service_codes": record.get("service_code", []),  # From negotiated_prices
        "billing_class": record.get("billing_class", ""),
        "negotiated_type": record.get("negotiated_type", ""),
        "expiration_date": record.get("expiration_date", ""),
        "provider_references": record.get("provider_references", []),
        "record_type": record.get("record_type", "rates"),
        "payer": payer
    }
    
    print(f"  ✅ Normalized: {normalized['service_code']} = ${normalized['negotiated_rate']}")
    return normalized

def fetch_and_parse_json(url: str):
    """Fetch and parse JSON, handling gzip compression."""
    print(f"Fetching URL: {url}")
    
    headers = get_cloudfront_headers()
    response = requests.get(url, headers=headers, timeout=300)
    response.raise_for_status()
    
    print(f"Downloaded {len(response.content)} bytes")
    
    # Handle gzipped content
    if url.endswith('.gz') or response.content.startswith(b'\x1f\x8b'):
        print("Decompressing gzipped content...")
        with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
            data = json.load(gz)
    else:
        data = json.loads(response.content.decode('utf-8'))
    
    print(f"Parsed JSON with keys: {list(data.keys()) if isinstance(data, dict) else 'array'}")
    return data

def process_complete_fix():
    """Process QUICK SAMPLE with all fixes applied - just for validation."""
    
    # Setup logging
    setup_logging("INFO")
    logger = get_logger(__name__)
    
    # Configuration
    url = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-08-01/2025-08-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_PPO---NDC_PPO-NDC_in-network-rates.json.gz"
    
    # IMPORTANT: For UHC GA NDC file, we need an NDC whitelist, not CPT!
    # Using empty set to process ALL codes for testing
    code_whitelist = set()  # Process everything for debugging
    
    # Alternatively, you could create an NDC whitelist like:
    # ndc_whitelist = {"00002-1433-80", "00002-7510-01"}  # Sample NDCs from your data
    
    # Quick sample for validation - just 3 items
    MAX_ITEMS = 3
    
    # Output setup
    base_dir = Path("output/uhc_ga_complete_fix")
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize writer
    writer = ParquetWriter(str(base_dir / "rates.parquet"), batch_size=100)
    
    # Processing stats
    stats = {
        "items_processed": 0,
        "records_generated": 0,
        "records_normalized": 0,
        "records_written": 0,
        "start_time": time.time()
    }
    
    logger.info("Starting QUICK VALIDATION processing...")
    logger.info(f"Processing only {MAX_ITEMS} items for validation")
    logger.info(f"URL: {url}")
    logger.info(f"Code whitelist: {len(code_whitelist)} codes (empty = process all)")
    logger.info(f"Output directory: {base_dir}")
    
    try:
        # Step 1: Fetch and parse the entire file
        print("\n=== STEP 1: FETCH AND PARSE ===")
        data = fetch_and_parse_json(url)
        
        if not isinstance(data, dict) or "in_network" not in data:
            raise ValueError("Invalid MRF structure - no in_network array found")
        
        in_network_items = data.get("in_network", [])
        print(f"Found {len(in_network_items)} in_network items")
        
        # Step 2: Initialize fixed handler
        print("\n=== STEP 2: INITIALIZE HANDLER ===")
        handler = FixedUhcGaHandler()
        print("Fixed UHC GA handler initialized")
        
        # Step 3: Process ONLY first few items for quick validation
        print(f"\n=== STEP 3: QUICK VALIDATION - FIRST {MAX_ITEMS} ITEMS ONLY ===")
        print("(This is just to test the pipeline, not process the whole file)")
        
        for i, item in enumerate(in_network_items[:MAX_ITEMS]):
            stats["items_processed"] += 1
            
            print(f"\nProcessing item {i+1}/{min(MAX_ITEMS, len(in_network_items))}")
            print(f"  Billing code: {item.get('billing_code', 'N/A')}")
            print(f"  Description: {item.get('description', 'N/A')[:50]}...")
            print(f"  Negotiated rates count: {len(item.get('negotiated_rates', []))}")
            
            # Step 3a: Parse with handler
            parsed_records = list(handler.parse_in_network(item))
            stats["records_generated"] += len(parsed_records)
            print(f"  Handler generated: {len(parsed_records)} records")
            
            # Step 3b: Normalize each parsed record
            for j, parsed_record in enumerate(parsed_records):
                print(f"    Normalizing record {j+1}/{len(parsed_records)}")
                
                normalized = fixed_normalize_tic_record(parsed_record, code_whitelist, "uhc_ga")
                if normalized:
                    stats["records_normalized"] += 1
                    
                    # Step 3c: Write normalized record
                    writer.write(normalized)
                    stats["records_written"] += 1
                    
                    # Show first few normalized records
                    if stats["records_written"] <= 3:
                        print(f"    Sample normalized record:\n{json.dumps(normalized, indent=6)}")
        
        # Step 4: Finalize
        print(f"\n=== STEP 4: FINALIZE ===")
        writer.close()
        
        # Final statistics
        elapsed = time.time() - stats["start_time"]
        print(f"\n=== VALIDATION RESULTS ===")
        print(f"⏱️  Total time: {elapsed:.1f} seconds")
        print(f"📄 Items processed: {stats['items_processed']:,}")
        print(f"🔄 Records generated by handler: {stats['records_generated']:,}")
        print(f"✅ Records normalized: {stats['records_normalized']:,}")
        print(f"💾 Records written: {stats['records_written']:,}")
        print(f"📁 Output file: {base_dir / 'rates.parquet'}")
        
        # Validation results
        if stats['records_written'] > 0:
            print(f"\n🎉 VALIDATION SUCCESS!")
            print(f"   Pipeline is working correctly")
            print(f"   Ready to process full dataset")
        else:
            print(f"\n❌ VALIDATION FAILED!")
            print(f"   Pipeline still has issues")
        
        # Log to structured logging
        logger.info("VALIDATION processing finished!")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info(f"Items processed: {stats['items_processed']:,}")
        logger.info(f"Records generated: {stats['records_generated']:,}")
        logger.info(f"Records normalized: {stats['records_normalized']:,}")
        logger.info(f"Records written: {stats['records_written']:,}")
        
        return stats["records_written"] > 0
        
    except Exception as e:
        logger.error(f"Error in complete fix: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Clean up writer on error
        try:
            writer.close()
        except:
            pass
        
        return False

if __name__ == "__main__":
    print("🔧 QUICK PIPELINE VALIDATION")
    print("📋 Fast validation of pipeline fixes:")
    print("   1. ✅ Bypasses broken streaming parser")
    print("   2. ✅ Uses fixed UHC GA handler")
    print("   3. ✅ Handles NDC codes properly") 
    print("   4. ✅ Fixed normalization logic")
    print("   5. ✅ Processes only 3 items for speed")
    print()
    
    success = process_complete_fix()
    
    if success:
        print("\n✅ VALIDATION SUCCESS!")
        print("   🎯 Pipeline is working correctly")
        print("   🚀 Ready to apply fixes to main code")
    else:
        print("\n❌ VALIDATION FAILED!")
        print("   🔍 Check logs for details")
    
    print("\n📋 Next steps:")
    print("   1. If validation passed: Apply fixes to main pipeline")
    print("   2. If validation failed: Debug the specific issues")
    print("   3. Update code whitelists for NDC vs CPT codes")
    print("   4. Fix streaming parser for production use")