#!/usr/bin/env python3
"""Analyze a single in-network MRF file structure efficiently."""

import json
import gzip
import requests
import os
from io import BytesIO
from datetime import datetime
from pathlib import Path
import argparse
from tic_mrf_scraper.utils.http_headers import get_cloudfront_headers
import tempfile

def is_local_file(path: str) -> bool:
    """Check if the path is a local file."""
    return os.path.exists(path) or path.startswith(('file://', 'C:', 'D:', '/', '\\'))

def get_file_size(url: str):
    """Get file size in bytes without downloading."""
    try:
        if is_local_file(url):
            if os.path.exists(url):
                return os.path.getsize(url)
            return None
            
        headers = get_cloudfront_headers()
        resp = requests.head(url, allow_redirects=True, headers=headers, timeout=30)
        resp.raise_for_status()
        return int(resp.headers.get('content-length', 0))
    except Exception as e:
        print(f"Error getting file size for {url}: {str(e)}")
        return None

def load_local_file(file_path: str):
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

def fetch_json_extremely_large(url: str, resp) -> dict:
    """Handle extremely large JSON files (>5GB) with minimal memory usage."""
    try:
        print(f"Downloading extremely large file in chunks...")
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json.gz' if url.endswith('.gz') else '.json') as temp_file:
            temp_path = temp_file.name
            
            # Download in chunks
            chunk_size = 1024 * 1024  # 1MB chunks
            downloaded = 0
            total_size = int(resp.headers.get('content-length', 0))
            
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    temp_file.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"Download progress: {progress:.1f}% ({downloaded / 1024 / 1024:.1f} MB / {total_size / 1024 / 1024:.1f} MB)")
        
        print(f"File downloaded to temporary location: {temp_path}")
        
        try:
            # Parse the file
            if url.endswith('.gz'):
                with gzip.open(temp_path, 'rt', encoding='utf-8') as f:
                    return json.load(f)
            else:
                with open(temp_path, 'rb') as f:
                    return json.load(f)
        finally:
            # Clean up
            try:
                os.unlink(temp_path)
                print("Temporary file cleaned up")
            except Exception as e:
                print(f"Warning: Could not clean up temporary file {temp_path}: {e}")
        
    except Exception as e:
        print(f"Error handling extremely large file: {str(e)}")
        return None

def fetch_json_streaming(url: str) -> dict:
    """Fetch and parse JSON with streaming for large files."""
    try:
        # Handle local files
        if is_local_file(url):
            return load_local_file(url)
        
        # Get file size first
        size = get_file_size(url)
        if size:
            size_mb = size / 1024 / 1024
            print(f"File size: {size_mb:.1f} MB")
            
            # For extremely large files (> 5GB), use special handling
            if size_mb > 5000:
                print(f"Extremely large file detected ({size_mb:.1f} MB)")
                headers = get_cloudfront_headers()
                resp = requests.get(url, stream=True, headers=headers, timeout=300)
                resp.raise_for_status()
                return fetch_json_extremely_large(url, resp)
        
        # Handle HTTP URLs
        headers = get_cloudfront_headers()
        resp = requests.get(url, stream=True, headers=headers, timeout=300)
        resp.raise_for_status()
        
        # Handle gzipped content
        content = resp.content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                return json.load(gz)
        else:
            return json.loads(content.decode('utf-8'))
            
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return None

def get_raw_sample(data, max_items=2):
    """Get raw samples from the data."""
    if not isinstance(data, dict):
        return {"error": "Invalid data format - not a dictionary"}
        
    if "in_network" not in data:
        return {"error": "Invalid data format - no in_network key"}
    
    # Extract core metadata
    sample = {
        "reporting_entity_name": data.get("reporting_entity_name"),
        "reporting_entity_type": data.get("reporting_entity_type"),
        "last_updated_on": data.get("last_updated_on"),
        "version": data.get("version"),
    }
    
    # Extract provider references if present
    if "provider_references" in data:
        sample["provider_references"] = data["provider_references"][:2]  # First 2 only
        
    # Take limited samples from in_network
    in_network_samples = []
    for item in data["in_network"][:max_items]:
        sample_item = item.copy()
        if "negotiated_rates" in sample_item:
            # Only take first 2 negotiated rates per item
            sample_item["negotiated_rates"] = sample_item["negotiated_rates"][:2]
        in_network_samples.append(sample_item)
    
    sample["in_network"] = in_network_samples
    return sample

def main():
    parser = argparse.ArgumentParser(description="Analyze a single in-network MRF file")
    parser.add_argument("url", help="URL or path to the in-network file")
    parser.add_argument("--output", "-o", default="mrf_sample.json", help="Output file path")
    args = parser.parse_args()
    
    print(f"Analyzing MRF file: {args.url}")
    
    # Fetch and analyze the file using streaming for large files
    data = fetch_json_streaming(args.url)
    if not data:
        print("Failed to fetch or parse the file")
        return
    
    # Get sample structure
    sample = get_raw_sample(data)
    
    # Save to file
    output_path = Path(args.output)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(sample, f, indent=2)
    
    print(f"\nSample structure saved to: {output_path}")

if __name__ == "__main__":
    main()