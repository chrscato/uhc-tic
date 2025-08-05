#!/usr/bin/env python3
"""Create a minimal representative sample from an in-network MRF file.

This script takes an in-network MRF file URL and creates a minimal sample that
preserves all unique data structures but removes repetition.
"""

import json
import gzip
import requests
import os
from io import BytesIO
from typing import Dict, Any, List, Set
from pathlib import Path
from datetime import datetime
import argparse

def is_local_file(path: str) -> bool:
    """Check if the path is a local file."""
    return os.path.exists(path) or path.startswith(('file://', 'C:', 'D:', '/', '\\'))

def load_local_file(file_path: str) -> Dict[str, Any]:
    """Load JSON from a local file, handling gzip compression."""
    try:
        # Remove file:// prefix if present
        if file_path.startswith('file://'):
            file_path = file_path[7:]
        
        # Handle gzip compression
        if file_path.endswith('.gz'):
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                return json.load(f)
        else:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading local file {file_path}: {str(e)}")
        return None

def fetch_json(url: str) -> Dict[str, Any]:
    """Fetch JSON from URL or local file."""
    try:
        if is_local_file(url):
            return load_local_file(url)
        
        print("  [*] Downloading file...")
        # Handle HTTP URLs
        resp = requests.get(url)
        resp.raise_for_status()
        
        print("  [*] Parsing content...")
        content = resp.content
        
        # Try to detect if content is gzipped
        is_gzipped = content.startswith(b'\x1f\x8b') or url.endswith('.gz')
        
        if is_gzipped:
            print("  [*] Decompressing gzipped content...")
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                content = gz.read()
        
        print("  [*] Parsing JSON...")
        return json.loads(content)
            
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return None

def make_hashable(obj: Any) -> Any:
    """Convert a value into a hashable type for deduplication."""
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    elif isinstance(obj, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
    elif isinstance(obj, (list, tuple)):
        return tuple(make_hashable(x) for x in obj)
    else:
        return str(obj)

def trim_arrays(obj: Any, max_items: int = 2, path: str = "") -> Any:
    """Recursively trim arrays to max_items while preserving unique values."""
    if isinstance(obj, list):
        # Special handling for various reference arrays
        if path == "provider_references" or path.endswith(".provider_references"):
            return obj[:2] if obj else []  # Just keep first 2 refs
            
        if path.endswith(".npi"):
            return obj[:2] if obj else []  # Just keep first 2 NPIs
            
        if path.endswith("provider_groups"):
            return obj[:2] if obj else []  # Just keep first 2 provider groups
            
        # Special handling for service_code arrays
        if path.endswith("service_code"):
            return obj[:2] if obj else []  # Just keep first 2 codes
            
        # For other arrays, keep unique items up to max_items
        seen = set()
        trimmed = []
        for item in obj:
            try:
                item_key = make_hashable(item)
                if item_key not in seen and len(trimmed) < max_items:
                    seen.add(item_key)
                    trimmed.append(trim_arrays(item, max_items, path))
            except Exception:
                # If we can't make it hashable, just add it if we have room
                if len(trimmed) < max_items:
                    trimmed.append(trim_arrays(item, max_items, path))
        return trimmed
    elif isinstance(obj, dict):
        return {k: trim_arrays(v, max_items, f"{path}.{k}" if path else k) for k, v in obj.items()}
    else:
        return obj

def get_structure_signature(item: Dict[str, Any]) -> str:
    """Get a unique signature for an item's structure."""
    sig = []
    if "billing_code_type" in item:
        sig.append(f"code:{item['billing_code_type']}:{item.get('billing_code_type_version', '')}")
    if "negotiation_arrangement" in item:
        sig.append(f"arrangement:{item['negotiation_arrangement']}")
    if "negotiated_rates" in item:
        for rate in item["negotiated_rates"]:
            if "negotiated_prices" in rate:
                for price in rate["negotiated_prices"]:
                    price_sig = []
                    if "negotiated_type" in price:
                        price_sig.append(f"type:{price['negotiated_type']}")
                    if "billing_class" in price:
                        price_sig.append(f"class:{price['billing_class']}")
                    if "billing_code_modifier" in price:
                        price_sig.append(f"mod:{','.join(sorted(price['billing_code_modifier']))}")
                    sig.append("price:" + ";".join(sorted(price_sig)))
    return "|".join(sorted(sig))

def get_unique_structure_samples(data: Dict[str, Any]) -> Dict[str, Any]:
    """Create minimal sample preserving all unique structures."""
    if not isinstance(data, dict):
        print("Invalid data format")
        return None
    
    # First, handle top-level provider_references if present
    if "provider_references" in data:
        print("[*] Trimming provider references...")
        print(f"  - Original count: {len(data['provider_references'])}")
        # Take just first 2 provider references
        data["provider_references"] = data["provider_references"][:2]
        print(f"  - Trimmed to: {len(data['provider_references'])}")
    
    # Track unique structure signatures
    seen_signatures = set()
    minimal_sample = []
    
    print("\n[*] Analyzing in_network structures...")
    
    # Analyze unique structures in in_network array
    print("  [*] Finding unique structures...")
    for item in data.get("in_network", []):
        if not isinstance(item, dict):
            continue
            
        sig = get_structure_signature(item)
        if sig not in seen_signatures:
            seen_signatures.add(sig)
            minimal_sample.append(item)
    
    print(f"\n[*] Found {len(seen_signatures)} unique structure combinations")
    print("\nUnique structures found:")
    for sig in sorted(seen_signatures):
        print(f"  - {sig}")
    
    print("\n[*] Creating minimal sample...")
    
    # Create minimal dataset with all top-level fields
    minimal_data = {k: v for k, v in data.items() if k != "in_network"}
    minimal_data["in_network"] = trim_arrays(minimal_sample, max_items=2)
    
    print("\n[*] Trimming arrays...")
    print(f"  - provider_references trimmed to max 2 items")
    print(f"  - service_codes trimmed to max 2 items")
    print(f"  - negotiated_prices trimmed to max 2 items")
    
    print(f"\n[+] Created minimal sample with {len(minimal_sample)} items")
    print(f"    (reduced from {len(data['in_network'])} original items)")
    
    return minimal_data

def main():
    parser = argparse.ArgumentParser(description="Create minimal MRF sample preserving all structures")
    parser.add_argument("url", help="URL or path to in-network MRF file")
    parser.add_argument("--output", default="mrf_samples", help="Output directory")
    args = parser.parse_args()
    
    # Create output directory
    output_path = Path(args.output)
    output_path.mkdir(exist_ok=True)
    
    # Fetch and process data
    print(f"\n[*] Fetching MRF file: {args.url}")
    data = fetch_json(args.url)
    if not data:
        print("[X] Failed to fetch MRF file")
        return
    
    # Create minimal sample
    minimal_data = get_unique_structure_samples(data)
    if not minimal_data:
        print("[X] Failed to create sample")
        return
    
    # Save sample
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"mrf_sample_{timestamp}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(minimal_data, f, indent=2)
    
    print(f"\n[+] Saved minimal sample to: {output_file}")

if __name__ == "__main__":
    main()