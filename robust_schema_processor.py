#!/usr/bin/env python3
"""
Memory-efficient MRF processor with clean progress bars and laptop-friendly design.
"""

import uuid
import pandas as pd
import json
import gzip
import requests
import gc
import psutil
import os
import ijson
import tempfile
from typing import Dict, Any, List, Set, Optional
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from io import BytesIO
from tic_mrf_scraper.fetch.blobs import get_cloudfront_headers

# Try to import tqdm for progress bars
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("📋 Note: Install tqdm for better progress bars: pip install tqdm")

class MemoryEfficientProcessor:
    """Memory-efficient processor with progress tracking."""
    
    def __init__(self, billing_code_whitelist: Optional[Set[str]] = None, 
                 tin_value_whitelist: Optional[Set[str]] = None):
        self.billing_code_whitelist = billing_code_whitelist or set()
        self.tin_value_whitelist = tin_value_whitelist or set()
        
        # Memory-efficient storage with separate batch sizes
        self.rates_batch = []
        self.providers_batch = []
        self.provider_reference_cache = {}
        self.provider_batch_size = 100  # Process all providers in one larger batch
        self.rates_batch_size = 5       # Process rates in very small batches for more frequent writes
        
        # Progress tracking
        self.stats = {
            "items_processed": 0,
            "items_passed_billing": 0,
            "rates_generated": 0,
            "rates_passed_tin": 0,
            "providers_created": 0,
            "memory_peak_mb": 0,
            "start_time": datetime.now()
        }
    
    def get_memory_usage(self):
        """Get current memory usage in MB."""
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        self.stats["memory_peak_mb"] = max(self.stats["memory_peak_mb"], memory_mb)
        return memory_mb
    
    def process_mrf_streaming(self, data: Dict[str, Any], output_dir: Path, 
                            max_items: Optional[int] = None) -> Dict[str, Any]:
        """
        Process MRF data with streaming and memory management.
        """
        print(f"\n🚀 STARTING MEMORY-EFFICIENT PROCESSING")
        print(f"📊 Initial memory: {self.get_memory_usage():.1f} MB")
        
        # Setup output files
        rates_path = output_dir / "rates_normalized.parquet"
        providers_path = output_dir / "providers_normalized.parquet"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Extract metadata
        file_metadata = self._extract_metadata(data)
        
        # Get in_network items
        in_network_items = data.get("in_network", [])
        total_items = len(in_network_items)
        
        if max_items and max_items < total_items:
            in_network_items = in_network_items[:max_items]
            total_items = max_items
        
        print(f"📄 Processing {total_items:,} items from {len(data.get('in_network', [])):,} total")
        
        # Process provider references first (memory efficient)
        print(f"👥 Processing provider references...")
        self._process_providers_streaming(data.get("provider_references", []), file_metadata)
        
        # Process in_network items with progress bar
        print(f"💰 Processing rate records...")
        
        if TQDM_AVAILABLE:
            items_pbar = tqdm(in_network_items, desc="Items", unit="item", 
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]')
        else:
            items_pbar = in_network_items
            print(f"Processing {total_items} items... (install tqdm for progress bar)")
        
        rates_written = 0
        
        for item_idx, item in enumerate(items_pbar):
            self.stats["items_processed"] += 1
            
            # Memory check every 100 items
            if item_idx % 100 == 0:
                current_memory = self.get_memory_usage()
                if current_memory > 2000:  # 2GB threshold for laptops
                    print(f"\n⚠️  High memory usage: {current_memory:.1f} MB - forcing garbage collection")
                    gc.collect()
            
            # Process this item
            rates_added = self._process_single_item(item, file_metadata)
            rates_written += rates_added
            
            # Update progress description
            if TQDM_AVAILABLE:
                items_pbar.set_postfix({
                    'Rates': rates_written,
                    'Mem': f"{self.get_memory_usage():.0f}MB"
                })
            elif item_idx % 50 == 0:  # Progress update for non-tqdm users
                print(f"  Processed {item_idx+1}/{total_items} items, {rates_written} rates, {self.get_memory_usage():.0f}MB")
            
            # Write when rates batch size is reached
            if len(self.rates_batch) >= self.rates_batch_size:
                self._write_batch(rates_path, providers_path)
        
        # Final statistics
        elapsed = (datetime.now() - self.stats["start_time"]).total_seconds()
        final_memory = self.get_memory_usage()
        
        print(f"\n✅ PROCESSING COMPLETE!")
        print(f"⏱️  Total time: {elapsed:.1f} seconds")
        print(f"📊 Final stats:")
        print(f"   📄 Items processed: {self.stats['items_processed']:,}")
        print(f"   ✅ Items passed billing filter: {self.stats['items_passed_billing']:,}")
        print(f"   💰 Rates generated: {self.stats['rates_generated']:,}")
        print(f"   ✅ Rates passed TIN filter: {self.stats['rates_passed_tin']:,}")
        print(f"   👥 Providers created: {self.stats['providers_created']:,}")
        print(f"   🧠 Peak memory: {self.stats['memory_peak_mb']:.1f} MB")
        print(f"   📁 Output: {output_dir}")
        
        return self.stats
    
    def _extract_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract file metadata efficiently."""
        return {
            "reporting_entity_name": data.get("reporting_entity_name", ""),
            "reporting_entity_type": data.get("reporting_entity_type", ""),
            "last_updated_on": data.get("last_updated_on", ""),
            "mrf_version": data.get("version", ""),
            "processing_timestamp": datetime.now().isoformat()
        }
    
    def _process_providers_streaming(self, provider_refs: List[Dict], file_metadata: Dict, providers_path: Path):
        """Process provider references in memory-efficient way."""
        if TQDM_AVAILABLE:
            pbar = tqdm(provider_refs, desc="Providers", unit="ref", leave=False)
        else:
            pbar = provider_refs
        
        for provider_ref in pbar:
            provider_reference_id = provider_ref.get("provider_group_id")
            if provider_reference_id is None:
                continue
            
            for group in provider_ref.get("provider_groups", []):
                tin_info = group.get("tin", {})
                tin_value = str(tin_info.get("value", ""))
                
                # Apply TIN filter early
                if self.tin_value_whitelist and tin_value not in self.tin_value_whitelist:
                    continue
                
                # Generate UUID and cache
                provider_group_uuid = str(uuid.uuid4())
                cache_key = (provider_reference_id, id(group))
                self.provider_reference_cache[cache_key] = provider_group_uuid
                
                # Create provider records
                for npi in group.get("npi", []):
                    provider_record = {
                        "provider_group_uuid": provider_group_uuid,
                        "provider_reference_id": provider_reference_id,
                        "npi": str(npi),
                        "tin_type": tin_info.get("type", ""),
                        "tin_value": tin_value,
                        "payer": "uhc_ga",
                        **file_metadata
                    }
                    self.providers_batch.append(provider_record)
                    self.stats["providers_created"] += 1
                    # Write when provider batch size is reached
                    if len(self.providers_batch) >= self.provider_batch_size:
                        self._write_batch(None, providers_path)
    
    def _process_single_item(self, item: Dict[str, Any], file_metadata: Dict) -> int:
        """Process a single in_network item efficiently."""
        billing_code = item.get("billing_code", "")
        
        # Apply billing code filter
        if self.billing_code_whitelist and billing_code not in self.billing_code_whitelist:
            return 0
        
        self.stats["items_passed_billing"] += 1
        
        # Extract base info
        base_info = {
            "billing_code": billing_code,
            "billing_code_type": item.get("billing_code_type", ""),
            "description": item.get("description", ""),
            "name": item.get("name", ""),
            "negotiation_arrangement": item.get("negotiation_arrangement", ""),
            **file_metadata
        }
        
        rates_added = 0
        
        # Process rates
        for rate_group in item.get("negotiated_rates", []):
            for price in rate_group.get("negotiated_prices", []):
                self.stats["rates_generated"] += 1
                
                # Check each provider reference
                for provider_ref_id in rate_group.get("provider_references", []):
                    if self._provider_passes_filter(provider_ref_id):
                        self.stats["rates_passed_tin"] += 1
                        
                        # Create rate record
                        rate_record = {
                            "rate_uuid": str(uuid.uuid4()),
                            "provider_group_uuid": self._find_provider_uuid(provider_ref_id),
                            "negotiated_rate": float(price.get("negotiated_rate", 0)),
                            "negotiated_type": price.get("negotiated_type", ""),
                            "billing_class": price.get("billing_class", ""),
                            "expiration_date": price.get("expiration_date", ""),
                            "service_codes": str(price.get("service_code", [])),
                            "provider_reference_id": provider_ref_id,
                            "payer": "uhc_ga",
                            **base_info
                        }
                        self.rates_batch.append(rate_record)
                        rates_added += 1
        
        return rates_added
    
    def _provider_passes_filter(self, provider_ref_id: int) -> bool:
        """Check if provider passes TIN filter."""
        if not self.tin_value_whitelist:
            return True
        
        # Check cached providers
        for (cached_ref_id, _), _ in self.provider_reference_cache.items():
            if cached_ref_id == provider_ref_id:
                return True
        
        return False
    
    def _find_provider_uuid(self, provider_ref_id: int) -> str:
        """Find provider UUID efficiently."""
        for (cached_ref_id, group_obj_id), uuid_val in self.provider_reference_cache.items():
            if cached_ref_id == provider_ref_id:
                return uuid_val
        return str(uuid.uuid4())  # Fallback
    
    def _write_batch(self, rates_path: Optional[Path], providers_path: Optional[Path]):
        """Write batches to parquet and clear memory."""
        if rates_path and self.rates_batch:
            # Write entire batch of rate records
            rate_df = pd.DataFrame(self.rates_batch)  # Convert batch to DataFrame
            try:
                if rates_path.exists() and rates_path.stat().st_size > 0:
                    # Read existing and append batch efficiently
                    existing_df = pd.read_parquet(rates_path)
                    rate_df = pd.concat([existing_df, rate_df], ignore_index=True)
            except (OSError, pd.errors.EmptyDataError):
                pass  # Handle first write or empty file
            
            # Write and clear memory
            rate_df.to_parquet(rates_path, index=False)
            self.rates_batch.clear()
            del rate_df  # Explicitly remove DataFrame from memory
        
        if providers_path and self.providers_batch:
            # Write entire batch of provider records
            provider_df = pd.DataFrame(self.providers_batch)  # Convert batch to DataFrame
            try:
                if providers_path.exists() and providers_path.stat().st_size > 0:
                    # Read existing and append batch efficiently
                    existing_df = pd.read_parquet(providers_path)
                    provider_df = pd.concat([existing_df, provider_df], ignore_index=True)
            except (OSError, pd.errors.EmptyDataError):
                pass  # Handle first write or empty file
            
            # Write and clear memory
            provider_df.to_parquet(providers_path, index=False)
            self.providers_batch.clear()
            del provider_df  # Explicitly remove DataFrame from memory
        
        # Force garbage collection after batch write
        gc.collect()

def process_with_progress_bars(source_url: str, max_items: Optional[int] = 100,
                              billing_code_whitelist: Optional[Set[str]] = None,
                              tin_value_whitelist: Optional[Set[str]] = None):
    """
    Memory-efficient processing with clean progress bars.
    """
    print("🔧 MEMORY-EFFICIENT MRF PROCESSOR")
    print("=" * 60)
    
    # Setup output directory
    output_dir = Path("output/memory_efficient_processing")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Configuration display
    print("🔍 CONFIGURATION:")
    print(f"   📋 Billing codes: {'ALL' if not billing_code_whitelist else f'{len(billing_code_whitelist)} codes'}")
    print(f"   🏢 TIN values: {'ALL' if not tin_value_whitelist else f'{len(tin_value_whitelist)} TINs'}")
    print(f"   📄 Max items: {'ALL' if max_items is None else f'{max_items:,}'}")
    print(f"   🧠 Available RAM: {psutil.virtual_memory().total / 1024**3:.1f} GB")
    
    # Fetch data
    print(f"\n📥 Fetching MRF data...")
    headers = get_cloudfront_headers()
    response = requests.get(source_url, headers=headers, timeout=300)
    response.raise_for_status()
    
    print(f"📦 Downloaded {len(response.content) / 1024**2:.1f} MB")
    
    # Stream and parse JSON using temporary file to avoid memory issues
    print(f"🔓 Decompressing and parsing...")
    
    # Save response content to temporary file to enable streaming
    with tempfile.NamedTemporaryFile(delete=False, suffix='.gz' if source_url.endswith('.gz') else '') as temp_file:
        temp_file.write(response.content)
        temp_path = temp_file.name
    
    try:
        # Process the file in streaming mode
        if source_url.endswith('.gz'):
            with gzip.open(temp_path, 'rb') as gz_file:
                # Extract metadata first (small memory footprint)
                metadata = {}
                parser = ijson.parse(gz_file)
                for prefix, event, value in parser:
                    if prefix in ['reporting_entity_name', 'reporting_entity_type', 'last_updated_on', 'version']:
                        metadata[prefix] = value
                    elif prefix == 'in_network':
                        break
                
                # Reset file pointer for main processing
                gz_file.seek(0)
                
                # Create processor first
                processor = MemoryEfficientProcessor(
                    billing_code_whitelist=billing_code_whitelist,
                    tin_value_whitelist=tin_value_whitelist
                )

                # Stream the provider references
                print(f"👥 Processing provider references...")
                provider_refs = ijson.items(gz_file, 'provider_references.item')
                providers_path = output_dir / "providers_normalized.parquet"
                processor._process_providers_streaming(provider_refs, metadata, providers_path)
                
                # Reset file pointer for in_network processing
                gz_file.seek(0)
                
                # Stream the in_network items
                in_network_stream = ijson.items(gz_file, 'in_network.item')
                data = {'in_network': in_network_stream, **metadata}
        else:
            with open(temp_path, 'rb') as json_file:
                # Similar process for uncompressed JSON
                metadata = {}
                parser = ijson.parse(json_file)
                for prefix, event, value in parser:
                    if prefix in ['reporting_entity_name', 'reporting_entity_type', 'last_updated_on', 'version']:
                        metadata[prefix] = value
                    elif prefix == 'in_network':
                        break
                
                json_file.seek(0)
                
                # Create processor first
                processor = MemoryEfficientProcessor(
                    billing_code_whitelist=billing_code_whitelist,
                    tin_value_whitelist=tin_value_whitelist
                )
                
                provider_refs = ijson.items(json_file, 'provider_references.item')
                providers_path = output_dir / "providers_normalized.parquet"
                processor._process_providers_streaming(provider_refs, metadata, providers_path)
                
                json_file.seek(0)
                in_network_stream = ijson.items(json_file, 'in_network.item')
                data = {'in_network': in_network_stream, **metadata}
        
        # Process the streaming data
        stats = processor.process_mrf_streaming(data, output_dir, max_items)
    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_path)
        except Exception:
            pass  # Ignore cleanup errors
    
    # Load and return final results
    rates_df = pd.read_parquet(output_dir / "rates_normalized.parquet")
    providers_df = pd.read_parquet(output_dir / "providers_normalized.parquet")
    
    return rates_df, providers_df, stats

if __name__ == "__main__":
    # Configuration for testing
    url = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-08-01/2025-08-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_Choice-Plus-POS_8_in-network-rates.json.gz"
    
    # Test with real TINs from the data
    test_tins = {
        "300466706",
        "581646537"
    }
    
    print("🧪 TESTING SCENARIOS:")
    print("1. Memory-efficient processing with progress bars")
    print("2. Laptop-friendly batch processing")
    print("3. Clean, readable output")
    
    # Run test
    rates_df, providers_df, stats = process_with_progress_bars(
        url,
        max_items=100,  # Good balance for laptop testing
        billing_code_whitelist=None,  # No billing filter
        tin_value_whitelist=None  # TIN filter only
    )
    
    print(f"\n🎉 SUCCESS!")
    print(f"📊 Final results:")
    print(f"   💰 Rates: {len(rates_df):,}")
    print(f"   👥 Providers: {len(providers_df):,}")
    print(f"   🧠 Peak memory: {stats['memory_peak_mb']:.1f} MB")
    
    if len(rates_df) > 0:
        print(f"\n🔍 Sample rate record:")
        sample = rates_df.iloc[0]
        print(f"   Code: {sample['billing_code']}")
        print(f"   Rate: ${sample['negotiated_rate']:.2f}")
        print(f"   Class: {sample['billing_class']}")
    else:
        print(f"\n⚠️  No rates found - try different TIN values or disable TIN filter")