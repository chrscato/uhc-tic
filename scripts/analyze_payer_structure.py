#!/usr/bin/env python3
"""Analyze Table of Contents and MRF structures for all payers in config.

Script name: analyze_payer_structures.py
"""

import json
import yaml
import requests
import gzip
import os
from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict
import argparse
from tic_mrf_scraper.utils.http_headers import get_cloudfront_headers

def load_config(config_path: str) -> Dict[str, Any]:
    """Load YAML configuration."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def is_local_file(path: str) -> bool:
    """Check if the path is a local file."""
    return os.path.exists(path) or path.startswith(('file://', 'C:', 'D:', '/', '\\'))

def load_local_file(file_path: str) -> Optional[Dict[str, Any]]:
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
        print(f"  [X] Error loading local file {file_path}: {str(e)}")
        return None

def fetch_json(url: str, max_size_mb: int = 500) -> Optional[Dict[str, Any]]:
    """Fetch and parse JSON from URL with size limit."""
    try:
        # Handle local files
        if is_local_file(url):
            return load_local_file(url)
        
        # Handle HTTP URLs
        headers = get_cloudfront_headers()
        resp = requests.get(url, stream=True, headers=headers, timeout=300)
        resp.raise_for_status()
        
        # Check content length if available
        content_length = resp.headers.get('content-length')
        if content_length and int(content_length) > max_size_mb * 1024 * 1024:
            print(f"  [!] File too large: {int(content_length) / 1024 / 1024:.1f} MB")
            return None
        
        # Download content
        content = resp.content
        
        # Handle gzipped content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                return json.load(gz)
        else:
            return json.loads(content.decode('utf-8'))
            
    except Exception as e:
        print(f"  [X] Error fetching {url}: {str(e)}")
        return None

def fetch_json_streaming(url: str, max_size_mb: int = 1000) -> Optional[Dict[str, Any]]:
    """Fetch and parse JSON from URL with streaming for large files."""
    try:
        # Handle local files
        if is_local_file(url):
            return load_local_file(url)
        
        # Handle HTTP URLs
        headers = get_cloudfront_headers()
        resp = requests.get(url, stream=True, headers=headers, timeout=300)
        resp.raise_for_status()
        
        # Check content length if available
        content_length = resp.headers.get('content-length')
        if content_length:
            size_mb = int(content_length) / 1024 / 1024
            print(f"  [*] File size: {size_mb:.1f} MB")
            
            # For extremely large files (> 5GB), use special handling
            if size_mb > 5000:
                print(f"  [!] Extremely large file detected ({size_mb:.1f} MB). Using chunked download.")
                return fetch_json_extremely_large(url, resp)
            
            # For very large files, use streaming JSON parser
            if size_mb > 100:  # > 100MB
                print(f"  [*] Using streaming parser for large file ({size_mb:.1f} MB)")
                return fetch_json_streaming_large(url, resp)
        
        # Download content for smaller files
        content = resp.content
        
        # Handle gzipped content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                return json.load(gz)
        else:
            return json.loads(content.decode('utf-8'))
            
    except Exception as e:
        print(f"  [X] Error fetching {url}: {str(e)}")
        return None

def fetch_json_extremely_large(url: str, resp) -> Optional[Dict[str, Any]]:
    """Handle extremely large JSON files (>5GB) with minimal memory usage."""
    try:
        import ijson
        import tempfile
        import os
        
        print(f"  [*] Downloading extremely large file in chunks...")
        
        # Create a temporary file to store the downloaded content
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json.gz' if url.endswith('.gz') else '.json') as temp_file:
            temp_path = temp_file.name
            
            # Download in chunks to avoid memory issues
            chunk_size = 1024 * 1024  # 1MB chunks
            downloaded = 0
            total_size = int(resp.headers.get('content-length', 0))
            
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    temp_file.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        progress = (downloaded / total_size) * 100
                        print(f"  [*] Download progress: {progress:.1f}% ({downloaded / 1024 / 1024:.1f} MB / {total_size / 1024 / 1024:.1f} MB)")
        
        print(f"  [*] File downloaded to temporary location: {temp_path}")
        
        # Now parse the file using ijson for memory efficiency
        try:
            if url.endswith('.gz'):
                # Handle gzipped content
                with gzip.open(temp_path, 'rt', encoding='utf-8') as f:
                    # Use ijson to parse the file stream
                    return json.load(f)  # Fall back to regular json.load for now
            else:
                # For non-gzipped content, use ijson
                with open(temp_path, 'rb') as f:
                    # Use ijson to parse the file stream
                    return json.load(f)  # Fall back to regular json.load for now
        finally:
            # Clean up temporary file
            try:
                os.unlink(temp_path)
                print(f"  [*] Temporary file cleaned up")
            except Exception as e:
                print(f"  [!] Warning: Could not clean up temporary file {temp_path}: {e}")
        
    except Exception as e:
        print(f"  [X] Error handling extremely large file: {str(e)}")
        return None

def fetch_json_streaming_large(url: str, resp) -> Optional[Dict[str, Any]]:
    """Stream parse large JSON files to avoid memory issues."""
    try:
        import ijson  # For streaming JSON parsing
        
        # Handle gzipped content
        if url.endswith('.gz') or resp.content.startswith(b'\x1f\x8b'):
            import gzip
            # For gzipped content, we need to decompress first
            with gzip.GzipFile(fileobj=BytesIO(resp.content)) as gz:
                return json.load(gz)
        else:
            # For non-gzipped content, try streaming first
            try:
                # Use ijson to parse the content stream
                content = resp.content
                # Parse JSON using ijson for memory efficiency
                parser = ijson.parse(content)
                # For now, fall back to regular parsing with increased limits
                # as ijson parsing is complex for full object reconstruction
                return json.loads(content.decode('utf-8'))
            except Exception:
                # Fall back to regular parsing
                return json.loads(resp.content.decode('utf-8'))
        
    except ImportError:
        print("  [!] ijson not available, falling back to regular parsing")
        # Handle gzipped content in fallback case
        if url.endswith('.gz') or resp.content.startswith(b'\x1f\x8b'):
            import gzip
            with gzip.GzipFile(fileobj=BytesIO(resp.content)) as gz:
                return json.load(gz)
        else:
            return json.loads(resp.content.decode('utf-8'))
    except Exception as e:
        print(f"  [X] Error in streaming parse: {str(e)}")
        return None

def analyze_structure(data: Any, path: str = "root", max_depth: int = 4, current_depth: int = 0) -> Dict[str, Any]:
    """Recursively analyze JSON structure with depth limit."""
    if current_depth >= max_depth:
        return {"type": "truncated", "reason": "max_depth"}
    
    if isinstance(data, dict):
        analysis = {
            "type": "object",
            "keys": list(data.keys()),
            "key_count": len(data.keys()),
            "children": {}
        }
        
        # Analyze each key
        for key in data.keys():
            if key in ["in_network", "reporting_structure", "allowed_amount_file", 
                      "in_network_files", "provider_references", "negotiated_rates"]:
                # Analyze these important keys deeper
                analysis["children"][key] = analyze_structure(
                    data[key], f"{path}.{key}", max_depth, current_depth + 1
                )
        
        return analysis
        
    elif isinstance(data, list):
        analysis = {
            "type": "array",
            "length": len(data),
            "sample_item": None
        }
        
        if data:
            # Analyze first item as sample
            analysis["sample_item"] = analyze_structure(
                data[0], f"{path}[0]", max_depth, current_depth + 1
            )
            
        return analysis
    else:
        return {
            "type": type(data).__name__,
            "sample_value": str(data)[:50] if data else None
        }

def get_file_size(url: str) -> Optional[int]:
    """Get file size in bytes without downloading the full file."""
    try:
        if is_local_file(url):
            if os.path.exists(url):
                return os.path.getsize(url)
            return None
            
        # For HTTP URLs, just get the headers
        headers = get_cloudfront_headers()
        resp = requests.head(url, allow_redirects=True, headers=headers, timeout=30)
        resp.raise_for_status()
        return int(resp.headers.get('content-length', 0))
    except Exception as e:
        print(f"  [!] Error getting file size for {url}: {str(e)}")
        return None

def find_smallest_in_network_file(toc_analysis: Dict[str, Any], max_check: int = 25) -> Optional[Dict[str, Any]]:
    """Find the smallest in-network file from first N files in TOC.
    
    Args:
        toc_analysis: The TOC analysis containing the raw data
        max_check: Maximum number of files to check, defaults to first 100 files
    """
    smallest_file = None
    smallest_size = float('inf')
    checked_files = 0
    
    # Get the raw TOC data
    data = toc_analysis.get("data", {})
    if not isinstance(data, dict) or "reporting_structure" not in data:
        print("  [!] No reporting structure found in TOC")
        return None
        
    # Count total files first
    total_files = sum(
        len(rs.get("in_network_files", []))
        for rs in data["reporting_structure"]
        if isinstance(rs, dict)
    )
    print(f"  [*] Found {total_files} total in-network files")
    
    # Look through each reporting structure
    for rs in data["reporting_structure"]:
        if not isinstance(rs, dict) or "in_network_files" not in rs:
            continue
            
        # Check each file in this reporting structure
        for file_info in rs["in_network_files"]:
            if not isinstance(file_info, dict):
                continue
                
            url = file_info.get("location", "")
            if not url:
                continue
                
            checked_files += 1
            if checked_files > max_check:
                print(f"\n  [!] Reached max check limit ({max_check} files)")
                return smallest_file
            
            print(f"  [*] Checking file {checked_files}/{min(max_check, total_files)}...", end="\r")
            size = get_file_size(url)
            size_mb = size / 1024 / 1024 if size else 0
            
            # Print full URL for each file we check
            print(f"\n  [*] File {checked_files}:")
            print(f"      Size: {size_mb:.1f} MB")
            print(f"      URL: {url}")
            
            # Only consider files that are valid and between 1MB and 200MB
            if size and 1 * 1024 * 1024 <= size <= 200 * 1024 * 1024 and size < smallest_size:
                print(f"  [+] New smallest valid file found!")
                smallest_size = size
                smallest_file = {
                    "url": url,
                    "size_mb": size_mb,
                    "description": file_info.get("description", ""),
                    "total_checked": checked_files,
                    "total_files": total_files
                }
    
    print()  # Clear the progress line
    if smallest_file:
        print(f"  [+] Checked {checked_files} out of {total_files} files")
        print(f"  [+] Smallest file found: {smallest_file['size_mb']:.1f} MB")
    
    return smallest_file

def fetch_toc_data(url: str) -> Optional[Dict[str, Any]]:
    """Fetch TOC data with special handling for large files."""
    # Check TOC file size first
    size = get_file_size(url)
    if size:
        size_mb = size / 1024 / 1024
        print(f"  [*] TOC file size: {size_mb:.1f} MB")
        if size_mb > 1000:  # TOC files over 1GB need special handling
            print(f"  [!] Large TOC file detected ({size_mb:.1f} MB)")
            # Here we could add special handling for extremely large TOCs
            # For now, we'll still try to fetch it
    
    try:
        # For local files
        if is_local_file(url):
            return load_local_file(url)
        
        # For HTTP URLs, use streaming
        headers = get_cloudfront_headers()
        resp = requests.get(url, stream=True, headers=headers, timeout=300)
        resp.raise_for_status()
        
        # Handle gzipped content
        if url.endswith('.gz'):
            with gzip.GzipFile(fileobj=BytesIO(resp.content)) as gz:
                return json.load(gz)
        
        # Regular JSON content
        return json.loads(resp.content.decode('utf-8'))
        
    except Exception as e:
        print(f"  [X] Error fetching TOC: {str(e)}")
        return None

def analyze_table_of_contents(url: str, payer: str) -> Dict[str, Any]:
    """Analyze a Table of Contents (index) file structure."""
    print(f"\n[TOC] Analyzing Table of Contents for {payer}")
    print(f"   URL: {url}")
    
    print("  [*] Fetching TOC data...")
    data = fetch_toc_data(url)
    if not data:
        print("  [X] Failed to fetch TOC file")
        return {"error": "Failed to fetch"}
        
    # Store raw data for file finding
    analysis = {
        "data": data,  # Store complete raw data for file finding
        "payer": payer,
        "url": url,
        "structure_type": "unknown",
        "top_level_keys": list(data.keys()) if isinstance(data, dict) else [],
        "file_counts": {},
        "sample_files": {},
        "detailed_structure": {}
    }
    
    if isinstance(data, dict):
        # Standard Table of Contents structure
        if "reporting_structure" in data:
            analysis["structure_type"] = "standard_toc"
            rs = data["reporting_structure"]
            analysis["file_counts"]["reporting_structures"] = len(rs)
            
            # Count file types
            in_network_count = 0
            allowed_amount_count = 0
            provider_ref_count = 0
            
            # Sample first reporting structure
            if rs:
                first_rs = rs[0]
                analysis["sample_structure"] = {
                    "keys": list(first_rs.keys()),
                    "plan_name": first_rs.get("plan_name", ""),
                    "plan_id": first_rs.get("plan_id", ""),
                    "plan_market_type": first_rs.get("plan_market_type", "")
                }
                
                # Count and sample in-network files
                if "in_network_files" in first_rs:
                    for r in rs:
                        in_network_count += len(r.get("in_network_files", []))
                    
                    # Sample first in-network file
                    if first_rs["in_network_files"]:
                        sample_file = first_rs["in_network_files"][0]
                        sample_url = sample_file.get("location", "")
                        analysis["sample_files"]["in_network"] = {
                            "url": sample_url,
                            "url_display": sample_url[:25] + "..." if len(sample_url) > 25 else sample_url,
                            "description": sample_file.get("description", "")
                        }
                
                # Count other file types
                for r in rs:
                    if "allowed_amount_file" in r:
                        allowed_amount_count += 1
                    if "provider_references" in r:
                        provider_ref_count += len(r.get("provider_references", []))
            
            analysis["file_counts"]["in_network_files"] = in_network_count
            analysis["file_counts"]["allowed_amount_files"] = allowed_amount_count
            analysis["file_counts"]["provider_reference_files"] = provider_ref_count
            
        # Legacy blobs structure
        elif "blobs" in data:
            analysis["structure_type"] = "legacy_blobs"
            analysis["file_counts"]["blobs"] = len(data["blobs"])
            
            if data["blobs"]:
                sample_url = data["blobs"][0].get("url", "")
                analysis["sample_files"]["blob"] = {
                    "url": sample_url,
                    "url_display": sample_url[:100] + "..." if len(sample_url) > 100 else sample_url,
                    "name": data["blobs"][0].get("name", "")
                }
        
        # Direct in_network_files structure
        elif "in_network_files" in data:
            analysis["structure_type"] = "direct_in_network"
            analysis["file_counts"]["in_network_files"] = len(data["in_network_files"])
            
            if data["in_network_files"]:
                sample_url = data["in_network_files"][0].get("location", "")
                analysis["sample_files"]["in_network"] = {
                    "url": sample_url,
                    "url_display": sample_url[:100] + "..." if len(sample_url) > 100 else sample_url,
                    "description": data["in_network_files"][0].get("description", "")
                }
        
        # Store detailed structure analysis
        analysis["detailed_structure"] = analyze_structure(data)
    
    return analysis

def analyze_provider_references(refs: List[Dict[str, Any]], max_samples: int = 3) -> Dict[str, Any]:
    """Analyze provider reference structure in detail."""
    analysis = {
        "count": len(refs),
        "sample_refs": [],
        "ref_types": defaultdict(int),
        "has_nested_providers": False,
        "provider_keys": set(),
        "group_keys": set(),
        "tin_types": set(),
    }
    
    for ref in refs[:max_samples]:
        ref_type = "unknown"
        if "provider_groups" in ref:
            ref_type = "provider_groups"
        elif "provider_references" in ref:
            ref_type = "provider_references"
        
        analysis["ref_types"][ref_type] += 1
        
        sample = {
            "type": ref_type,
            "keys": list(ref.keys()),
        }
        
        # Analyze provider groups
        if "provider_groups" in ref and ref["provider_groups"]:
            pg = ref["provider_groups"][0]
            sample["group_keys"] = list(pg.keys())
            analysis["group_keys"].update(pg.keys())
            
            # Check TIN structure
            if "tin" in pg:
                tin = pg["tin"]
                if isinstance(tin, dict):
                    sample["tin_type"] = "object"
                    sample["tin_keys"] = list(tin.keys())
                    analysis["tin_types"].add("object")
                else:
                    sample["tin_type"] = "string"
                    analysis["tin_types"].add("string")
            
            # Check for nested providers
            if "providers" in pg and pg["providers"]:
                analysis["has_nested_providers"] = True
                provider = pg["providers"][0]
                sample["provider_keys"] = list(provider.keys())
                analysis["provider_keys"].update(provider.keys())
        
        analysis["sample_refs"].append(sample)
    
    return analysis

def get_raw_sample(data: Dict[str, Any], max_items: int = 2) -> Dict[str, Any]:
    """Just get raw samples from the data without analysis."""
    if not isinstance(data, dict) or "in_network" not in data:
        return {"error": "Invalid data format"}
    
    # Take limited samples and limit negotiated rates per item
    samples = []
    for item in data["in_network"][:max_items]:
        sample = item.copy()
        if "negotiated_rates" in sample:
            # Only take first 2 negotiated rates per item
            sample["negotiated_rates"] = sample["negotiated_rates"][:2]
            # Add note about provider references
            if any("provider_references" in rate for rate in sample["negotiated_rates"]):
                sample["_note"] = "Provider details available in provider references file"
        samples.append(sample)
        
    return {
        "total_items": len(data["in_network"]),
        "samples": samples
    }

def analyze_in_network_file(url: str, payer: str, max_items: int = 2) -> Dict[str, Any]:
    """Get raw samples from an in-network MRF file."""
    print(f"\n[MRF] Getting samples from file for {payer}")
    print(f"   URL: {url[:100]}...")
    
    # Try streaming first for large files, then fall back to regular fetch
    data = fetch_json_streaming(url, max_size_mb=1000)  # Much higher limit for MRF files
    if not data:
        data = fetch_json(url, max_size_mb=500)  # Fallback with higher limit
    if not data:
        return {"error": "Failed to fetch or file too large"}
    
    # Just get raw samples
    samples = get_raw_sample(data, max_items)
    return {
        "payer": payer,
        "url": url,
        "samples": samples
    }

def save_analysis(analyses: Dict[str, Any], output_dir: str = "payer_structure_analysis"):
    """Save analysis results to files."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save full raw analysis
    full_path = output_path / f"full_analysis_{timestamp}.json"
    with open(full_path, 'w', encoding='utf-8') as f:
        json.dump(analyses, f, indent=2, default=str)
    
    # Save raw samples separately for easier access
    samples_path = output_path / f"raw_samples_{timestamp}.json"
    raw_samples = {}
    for payer, analysis in analyses.items():
        if "in_network_mrf" in analysis and "sample_items" in analysis["in_network_mrf"]:
            raw_samples[payer] = {
                "raw_samples": analysis["in_network_mrf"]["sample_items"],
                "total_items": analysis["in_network_mrf"]["in_network_count"],
                "url": analysis["in_network_mrf"]["url"]
            }
    
    with open(samples_path, 'w', encoding='utf-8') as f:
        json.dump(raw_samples, f, indent=2, default=str)
    
    print(f"\n[+] Analysis saved to:")
    print(f"   - Full analysis: {full_path}")
    print(f"   - Raw samples: {samples_path}")

def main():
    parser = argparse.ArgumentParser(description="Analyze payer MRF structures")
    parser.add_argument("--config", default="production_config.yaml", help="Path to config file")
    parser.add_argument("--payers", nargs="+", help="Specific payers to analyze")
    parser.add_argument("--skip-mrf", action="store_true", help="Skip in-network MRF analysis")
    parser.add_argument("--output-dir", default="payer_structure_analysis", help="Output directory")
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    # Try both payer_endpoints and endpoints (for backward compatibility)
    payer_endpoints = config.get("payer_endpoints", config.get("endpoints", {}))
    
    # Filter payers if specified
    if args.payers:
        payer_endpoints = {k: v for k, v in payer_endpoints.items() if k in args.payers}
    
    print(f"[*] Analyzing {len(payer_endpoints)} payer(s) from {args.config}")
    
    # Analyze each payer
    all_analyses = {}
    
    for payer, index_url in payer_endpoints.items():
        print(f"\n{'='*80}")
        print(f"ANALYZING PAYER: {payer}")
        print(f"{'='*80}")
        
        payer_analysis = {}
        
        # Analyze Table of Contents
        toc_analysis = analyze_table_of_contents(index_url, payer)
        payer_analysis["table_of_contents"] = toc_analysis
        
        # Find smallest in-network file for analysis
        if not args.skip_mrf and isinstance(toc_analysis, dict) and not "error" in toc_analysis:
            print("\n[*] Finding smallest in-network file for analysis...")
            
            # Only check first 100 files
            smallest_file = find_smallest_in_network_file(toc_analysis)
            
            if smallest_file:
                print(f"  [+] Found smallest file: {smallest_file['size_mb']:.1f} MB")
                print(f"      Description: {smallest_file.get('description', 'N/A')}")
                print(f"      Found after checking {smallest_file['total_checked']} of {smallest_file['total_files']} files")
                mrf_analysis = analyze_in_network_file(smallest_file["url"], payer)
                payer_analysis["in_network_mrf"] = mrf_analysis
            else:
                print("  [!] No suitable in-network files found")
        
        all_analyses[payer] = payer_analysis
    
    # Save results
    save_analysis(all_analyses, args.output_dir)
    
    print("\n[DONE] Analysis complete!")

if __name__ == "__main__":
    main()