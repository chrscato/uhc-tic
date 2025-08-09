"""Extract provider references from MRF files with memory-efficient streaming."""

import gzip
import ijson
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Set

from utils import (
    get_memory_usage,
    force_garbage_collection,
    create_progress_bar,
    get_output_slug
)

def load_provider_group_whitelist(parquet_path: str) -> Set[int]:
    """
    Load unique provider_reference_id values from a Parquet file.
    
    Args:
        parquet_path: Path to Parquet file containing provider_reference_id column
        
    Returns:
        Set of unique provider group IDs
    """
    print(f"📋 Loading provider group whitelist from: {parquet_path}")
    
    try:
        df = pd.read_parquet(parquet_path)
        
        if 'provider_reference_id' not in df.columns:
            raise ValueError(f"Column 'provider_reference_id' not found in {parquet_path}")
        
        provider_groups = set(df['provider_reference_id'].dropna().unique())
        print(f"✅ Loaded {len(provider_groups):,} unique provider group IDs")
        
        return provider_groups
        
    except Exception as e:
        print(f"❌ Error loading provider group whitelist: {e}")
        return set()

def load_tin_whitelist(file_path: str) -> Set[str]:
    """
    Load TIN values from a text file (one TIN per line).
    
    Args:
        file_path: Path to text file with TIN values
        
    Returns:
        Set of TIN values to filter for
    """
    tin_values = set()
    try:
        with open(file_path, 'r') as f:
            for line in f:
                tin = line.strip()
                if tin:  # Skip empty lines
                    tin_values.add(tin)
        print(f"📋 Loaded {len(tin_values)} TIN values from {file_path}")
        return tin_values
    except FileNotFoundError:
        print(f"⚠️  TIN whitelist file not found: {file_path}")
        return set()

class ProviderExtractor:
    def __init__(self, batch_size: int = 1000, provider_group_whitelist: Optional[Set[int]] = None):
        self.batch_size = batch_size
        self.provider_group_whitelist = provider_group_whitelist or set()
        self.providers_batch: List[Dict[str, Any]] = []
        self.stats = {
            "providers_processed": 0,
            "providers_written": 0,
            "providers_filtered": 0,
            "providers_examined": 0,
            "peak_memory_mb": 0,
            "start_time": datetime.now()
        }
    
    def _update_memory_stats(self):
        """Update peak memory usage statistics."""
        current_memory = get_memory_usage()
        self.stats["peak_memory_mb"] = max(
            self.stats["peak_memory_mb"], 
            current_memory
        )
        return current_memory

    def _write_batch(self, output_path: Path) -> None:
        """Write current batch to parquet file."""
        if not self.providers_batch:
            return
            
        print(f"  💾 Writing batch of {len(self.providers_batch)} providers...")
        provider_df = pd.DataFrame(self.providers_batch)
        
        # Append to existing file or create new one
        if output_path.exists():
            existing_df = pd.read_parquet(output_path)
            provider_df = pd.concat([existing_df, provider_df], ignore_index=True)
        
        provider_df.to_parquet(output_path, index=False)
        self.stats["providers_written"] += len(self.providers_batch)
        print(f"  ✅ Batch written! Total written: {self.stats['providers_written']:,}")
        self.providers_batch.clear()
        force_garbage_collection()

    def _process_provider(self, provider: Dict[str, Any], file_metadata: Dict[str, Any]) -> None:
        """Process a single provider reference."""
        provider_group_id = provider.get("provider_group_id")
        self.stats["providers_examined"] += 1
        
        # Apply provider group whitelist filter if specified
        if self.provider_group_whitelist and provider_group_id not in self.provider_group_whitelist:
            self.stats["providers_filtered_by_group"] += 1
            return  # Skip this provider entirely
        
        # If we get here, this provider matches the group whitelist
        if self.stats["providers_processed"] % 10 == 0:  # Show every 10th match
            print(f"  ✅ Found matching provider group {provider_group_id} (total processed: {self.stats['providers_processed']:,})")
        
        for group in provider.get("provider_groups", []):
            tin_info = group.get("tin", {})
            tin_value = str(tin_info.get("value", ""))
            
            # Apply TIN whitelist filter if specified
            if self.tin_whitelist and tin_value not in self.tin_whitelist:
                self.stats["providers_filtered_by_tin"] += 1
                continue  # Skip this provider group but continue with other groups
            
            # Create provider record for each NPI
            for npi in group.get("npi", []):
                provider_record = {
                    "provider_group_id": provider_group_id,
                    "npi": str(npi),
                    "tin_type": tin_info.get("type", ""),
                    "tin_value": tin_value,
                    **file_metadata
                }
                self.providers_batch.append(provider_record)
                self.stats["providers_processed"] += 1
                
                # Write batch if size threshold reached
                if len(self.providers_batch) >= self.batch_size:
                    self._write_batch(self.output_path)

    def process_file(self, file_path: str, output_dir: Path, max_providers: Optional[int] = None) -> Dict[str, Any]:
        """
        Process provider references from MRF file.
        
        Args:
            file_path: Path to .json.gz file
            output_dir: Directory for output files
            max_providers: Optional limit on number of provider groups to process
        
        Returns:
            Processing statistics
        """
        print(f"\n🔍 EXTRACTING PROVIDERS")
        print(f"📊 Initial memory: {self._update_memory_stats():.1f} MB")
        
        # Setup output path
        slug = get_output_slug()
        self.output_path = output_dir / f"providers_{slug}.parquet"
        
        with gzip.open(file_path, 'rb') as gz_file:
            # Extract file metadata first
            parser = ijson.parse(gz_file)
            file_metadata = {}
            for prefix, event, value in parser:
                if prefix in ['reporting_entity_name', 'reporting_entity_type', 
                            'last_updated_on', 'version']:
                    file_metadata[prefix] = value
                elif prefix == 'provider_references':
                    break
            
            # Reset file pointer for provider processing
            gz_file.seek(0)
            
            # Stream process providers
            providers = ijson.items(gz_file, 'provider_references.item')
            
            # Apply max_providers limit if specified
            if max_providers:
                providers = create_progress_bar(providers, "Providers", "ref")
                for idx, provider in enumerate(providers):
                    if idx >= max_providers:
                        break
                    self._process_provider(provider, file_metadata)
                    
                    # Progress update every 1000 providers
                    if idx % 1000 == 0 and idx > 0:
                        print(f"  📊 Examined {self.stats['providers_examined']:,} providers, filtered {self.stats['providers_filtered']:,}, written {self.stats['providers_written']:,}")
                    
                    # Memory check every 100 providers
                    if self.stats["providers_processed"] % 100 == 0:
                        self._update_memory_stats()
            else:
                # Process all providers
                for idx, provider in enumerate(create_progress_bar(providers, "Providers", "ref")):
                    self._process_provider(provider, file_metadata)
                    
                    # Progress update every 1000 providers
                    if idx % 1000 == 0 and idx > 0:
                        print(f"  📊 Examined {self.stats['providers_examined']:,} providers, filtered by group: {self.stats['providers_filtered_by_group']:,}, filtered by TIN: {self.stats['providers_filtered_by_tin']:,}, written {self.stats['providers_written']:,}")
                    
                    # Memory check every 100 providers
                    if self.stats["providers_processed"] % 100 == 0:
                        self._update_memory_stats()
        
        # Write final batch
        if self.providers_batch:
            self._write_batch(self.output_path)
        
        # Final statistics
        elapsed = (datetime.now() - self.stats["start_time"]).total_seconds()
        final_memory = self._update_memory_stats()
        
        print(f"\n✅ PROVIDER EXTRACTION COMPLETE")
        print(f"⏱️  Time elapsed: {elapsed:.1f} seconds")
        print(f"📊 Providers examined: {self.stats['providers_examined']:,}")
        print(f"📊 Providers filtered by group: {self.stats['providers_filtered_by_group']:,}")
        print(f"📊 Providers filtered by TIN: {self.stats['providers_filtered_by_tin']:,}")
        print(f"📊 Providers written: {self.stats['providers_written']:,}")
        print(f"🧠 Peak memory: {self.stats['peak_memory_mb']:.1f} MB")
        print(f"📁 Output: {self.output_path}")
        
        return {
            "output_path": str(self.output_path),
            "stats": self.stats
        }

if __name__ == "__main__":
    import sys
    import argparse
    from utils import download_to_temp
    
    parser = argparse.ArgumentParser(description="Extract providers from MRF files")
    parser.add_argument("source", help="URL or path to MRF file")
    parser.add_argument("--max-providers", "-m", type=int, help="Maximum number of provider groups to process")
    parser.add_argument("--provider-whitelist", "-p", type=str, 
                        help="Path to Parquet file with provider_reference_id column to use as whitelist")
    parser.add_argument("--batch-size", "-b", type=int, default=1000,
                        help="Batch size for writing (default: 1000)")
    args = parser.parse_args()
    
    # Load provider group whitelist if specified
    provider_group_whitelist = None
    if args.provider_whitelist:
        provider_group_whitelist = load_provider_group_whitelist(args.provider_whitelist)
        if not provider_group_whitelist:
            print("⚠️  No provider groups loaded from whitelist file. Processing all providers.")
    
    # Load TIN whitelist if specified
    tin_whitelist = None
    if args.tin_whitelist:
        tin_whitelist = load_tin_whitelist(args.tin_whitelist)
        if not tin_whitelist:
            print("⚠️  No TIN values loaded from whitelist file. Processing all TINs.")
    
    try:
        # Download if URL
        if args.source.startswith(('http://', 'https://')):
            print(f"📥 Downloading from URL...")
            temp_path = download_to_temp(args.source)
            print(f"📦 Downloaded to: {temp_path}")
        else:
            temp_path = args.source
            print(f"📄 Using local file: {temp_path}")
        
        # Process the file
        extractor = ProviderExtractor(
            batch_size=args.batch_size,
            provider_group_whitelist=provider_group_whitelist,
            tin_whitelist=tin_whitelist
        )
        results = extractor.process_file(
            temp_path,
            output_dir=Path("output"),
            max_providers=args.max_providers
        )
        
    finally:
        # Cleanup temp file if downloaded
        if args.source.startswith(('http://', 'https://')) and 'temp_path' in locals():
            try:
                import os
                os.unlink(temp_path)
                print(f"\n🧹 Cleaned up temporary file")
            except Exception:
                pass