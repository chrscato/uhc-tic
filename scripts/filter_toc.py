#!/usr/bin/env python3
"""Filter TOC file to only keep URLs matching a specific pattern using streaming.

This script processes a large TOC file in chunks and creates a filtered version that only
contains reporting structures with URLs matching the specified pattern.
"""

import json
import gzip
import zlib
import requests
import os
import re
from io import BytesIO
from typing import Dict, Any, List, Optional, Iterator
from pathlib import Path
from datetime import datetime
import argparse
from tic_mrf_scraper.utils.http_headers import get_cloudfront_headers

def get_file_size(url: str) -> Optional[int]:
    """Get file size in bytes without downloading the full file."""
    try:
        if os.path.exists(url):
            return os.path.getsize(url)
            
        headers = get_cloudfront_headers()
        resp = requests.head(url, allow_redirects=True, headers=headers, timeout=30)
        resp.raise_for_status()
        return int(resp.headers.get('content-length', 0))
    except Exception as e:
        print(f"  [!] Error getting file size for {url}: {str(e)}")
        return None

def stream_decompress_and_filter(url: str, url_pattern: str, chunk_size_mb: int = 100) -> Iterator[Dict[str, Any]]:
    """Stream, decompress and filter TOC file in chunks."""
    try:
        print(f"  [*] Starting streaming download and filter...")
        headers = get_cloudfront_headers()
        
        with requests.get(url, headers=headers, timeout=600, stream=True) as resp:
            resp.raise_for_status()
            
            # Set up decompressor for gzip stream
            if url.endswith('.gz'):
                decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)  # gzip format
            
            buffer = ""
            downloaded_mb = 0
            chunk_size = chunk_size_mb * 1024 * 1024
            structure_count = 0
            matched_count = 0
            in_structure = False
            structure_buffer = ""
            brace_count = 0
            
            print(f"  [*] Processing stream in {chunk_size_mb}MB chunks...")
            
            for chunk in resp.iter_content(chunk_size=8192):
                downloaded_mb += len(chunk) / 1024 / 1024
                
                if downloaded_mb % 100 < 0.1:  # Every ~100MB
                    print(f"  [*] Processed {downloaded_mb:.0f}MB, found {matched_count} matching structures...")
                
                # Decompress chunk
                if url.endswith('.gz'):
                    try:
                        text_chunk = decompressor.decompress(chunk).decode('utf-8', errors='ignore')
                    except:
                        continue
                else:
                    text_chunk = chunk.decode('utf-8', errors='ignore')
                
                buffer += text_chunk
                
                # Process complete structures from buffer
                while True:
                    if not in_structure:
                        # Look for start of a reporting structure
                        structure_start = buffer.find('{"reporting_plans"')
                        if structure_start == -1:
                            # Keep some buffer for incomplete patterns
                            if len(buffer) > 10000:
                                buffer = buffer[-5000:]
                            break
                        
                        # Start collecting structure
                        in_structure = True
                        structure_buffer = buffer[structure_start:]
                        buffer = ""
                        brace_count = 0
                    
                    # Count braces to find complete structure
                    for i, char in enumerate(structure_buffer[len(structure_buffer) - len(text_chunk):]):
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            
                            if brace_count == 0:
                                # Found complete structure
                                complete_structure = structure_buffer[:len(structure_buffer) - len(text_chunk) + i + 1]
                                
                                try:
                                    # Parse and check structure
                                    structure_data = json.loads(complete_structure)
                                    structure_count += 1
                                    
                                    # Check if structure has matching URLs
                                    if has_matching_urls(structure_data, url_pattern):
                                        filtered_structure = filter_structure_urls(structure_data, url_pattern)
                                        matched_count += 1
                                        yield filtered_structure
                                    
                                except json.JSONDecodeError:
                                    pass  # Skip invalid JSON
                                
                                # Reset for next structure
                                structure_buffer = structure_buffer[len(structure_buffer) - len(text_chunk) + i + 1:]
                                in_structure = False
                                brace_count = 0
                                break
                    
                    if in_structure:
                        break
                
                # Memory management - don't let buffers grow too large
                if len(buffer) > 50000:
                    buffer = buffer[-25000:]
                if len(structure_buffer) > 500000:  # Structure too large, skip it
                    in_structure = False
                    structure_buffer = ""
                    brace_count = 0
            
            print(f"\n  [+] Streaming complete:")
            print(f"      Total structures processed: {structure_count}")
            print(f"      Matching structures found: {matched_count}")
            
    except Exception as e:
        print(f"  [!] Error in streaming: {str(e)}")

def has_matching_urls(structure: Dict[str, Any], pattern: str) -> bool:
    """Check if structure has any URLs matching the pattern."""
    pattern_lower = pattern.lower()
    
    # Check in_network_files
    if "in_network_files" in structure:
        for file_info in structure["in_network_files"]:
            if isinstance(file_info, dict) and "location" in file_info:
                if pattern_lower in file_info["location"].lower():
                    return True
    
    # Check allowed_amount_file
    if "allowed_amount_file" in structure:
        allowed_file = structure["allowed_amount_file"]
        if isinstance(allowed_file, dict) and "location" in allowed_file:
            if pattern_lower in allowed_file["location"].lower():
                return True
    
    # Check provider_references
    if "provider_references" in structure:
        for ref_info in structure["provider_references"]:
            if isinstance(ref_info, dict) and "location" in ref_info:
                if pattern_lower in ref_info["location"].lower():
                    return True
    
    return False

def filter_structure_urls(structure: Dict[str, Any], pattern: str) -> Dict[str, Any]:
    """Filter URLs in a structure to only keep matching ones."""
    pattern_lower = pattern.lower()
    filtered_structure = structure.copy()
    
    # Filter in_network_files
    if "in_network_files" in structure:
        filtered_files = []
        for file_info in structure["in_network_files"]:
            if isinstance(file_info, dict) and "location" in file_info:
                if pattern_lower in file_info["location"].lower():
                    filtered_files.append(file_info)
        filtered_structure["in_network_files"] = filtered_files
    
    # Filter allowed_amount_file
    if "allowed_amount_file" in structure:
        allowed_file = structure["allowed_amount_file"]
        if isinstance(allowed_file, dict) and "location" in allowed_file:
            if pattern_lower not in allowed_file["location"].lower():
                del filtered_structure["allowed_amount_file"]
    
    # Filter provider_references
    if "provider_references" in structure:
        filtered_refs = []
        for ref_info in structure["provider_references"]:
            if isinstance(ref_info, dict) and "location" in ref_info:
                if pattern_lower in ref_info["location"].lower():
                    filtered_refs.append(ref_info)
        filtered_structure["provider_references"] = filtered_refs
    
    return filtered_structure

def extract_metadata(url: str) -> Dict[str, Any]:
    """Extract metadata from the beginning of the file."""
    try:
        print("  [*] Extracting metadata...")
        headers = get_cloudfront_headers()
        
        # Download first 10MB to get metadata
        headers['Range'] = 'bytes=0-10485760'  # 10MB
        
        with requests.get(url, headers=headers, timeout=60) as resp:
            resp.raise_for_status()
            content = resp.content
            
            # Decompress if needed
            if url.endswith('.gz'):
                try:
                    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
                    content = decompressor.decompress(content)
                except:
                    # Try different approach for partial gzip
                    decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
                    content = decompressor.decompress(content[10:])  # Skip gzip header
            
            content_str = content.decode('utf-8', errors='ignore')
            
            # Extract metadata before reporting_structure
            metadata = {}
            
            # Look for key-value pairs at the start
            lines = content_str.split('\n')[:50]  # First 50 lines
            for line in lines:
                line = line.strip()
                if line.startswith('"reporting_entity_name"'):
                    match = re.search(r'"reporting_entity_name":\s*"([^"]*)"', line)
                    if match:
                        metadata["reporting_entity_name"] = match.group(1)
                elif line.startswith('"reporting_entity_type"'):
                    match = re.search(r'"reporting_entity_type":\s*"([^"]*)"', line)
                    if match:
                        metadata["reporting_entity_type"] = match.group(1)
                elif '"reporting_structure"' in line:
                    break
            
            print(f"  [+] Metadata extracted: {list(metadata.keys())}")
            return metadata
            
    except Exception as e:
        print(f"  [!] Error extracting metadata: {str(e)}")
        return {"reporting_entity_name": "Unknown", "reporting_entity_type": "health insurance issuer"}

def main():
    parser = argparse.ArgumentParser(description="Filter TOC file for specific URL patterns using streaming")
    parser.add_argument("url", help="URL or path to TOC file")
    parser.add_argument("--pattern", required=True, help="URL pattern to match (e.g., 'bcbsga')")
    parser.add_argument("--output", default="filtered_toc", help="Output directory")
    parser.add_argument("--output-name", help="Output filename (without extension)")
    parser.add_argument("--chunk-size", type=int, default=100, help="Chunk size in MB for streaming")
    args = parser.parse_args()
    
    # Create output directory
    output_path = Path(args.output)
    output_path.mkdir(exist_ok=True)
    
    print(f"\n[*] Processing TOC file: {args.url}")
    print(f"[*] Filtering pattern: '{args.pattern}'")
    
    # Check file size
    size = get_file_size(args.url)
    if size:
        size_gb = size / 1024 / 1024 / 1024
        print(f"[*] File size: {size_gb:.1f} GB")
    
    # Extract metadata first
    metadata = extract_metadata(args.url)
    
    # Set up output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.output_name:
        output_filename = f"{args.output_name}.json"
    else:
        original_filename = os.path.basename(args.url).replace('.gz', '').replace('.json', '')
        output_filename = f"{original_filename}_filtered_{args.pattern}_{timestamp}.json"
    
    output_file = output_path / output_filename
    
    print(f"\n[*] Starting streaming filter...")
    print(f"[*] Output file: {output_file}")
    
    # Process file with streaming and write results
    matched_structures = []
    total_urls = 0
    
    try:
        for structure in stream_decompress_and_filter(args.url, args.pattern, args.chunk_size):
            matched_structures.append(structure)
            
            # Count URLs in this structure
            total_urls += len(structure.get("in_network_files", []))
            if "allowed_amount_file" in structure:
                total_urls += 1
            total_urls += len(structure.get("provider_references", []))
            
            # Save periodically to avoid memory issues
            if len(matched_structures) % 1000 == 0:
                print(f"  [*] Collected {len(matched_structures)} matching structures so far...")
        
        # Create final filtered data
        filtered_data = metadata.copy()
        filtered_data["reporting_structure"] = matched_structures
        
        # Save final result
        print(f"\n[*] Saving filtered data...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n[+] Filtering complete!")
        print(f"[+] Original file: {args.url}")
        print(f"[+] Filtered file: {output_file}")
        print(f"[+] Pattern matched: '{args.pattern}'")
        print(f"[+] Filtered result contains:")
        print(f"    - {len(matched_structures)} reporting structures")
        print(f"    - {total_urls} total URLs (all matching pattern)")
        
    except Exception as e:
        print(f"\n[X] Error during processing: {str(e)}")
        # Save partial results if any
        if matched_structures:
            partial_data = metadata.copy()
            partial_data["reporting_structure"] = matched_structures
            partial_file = output_path / f"partial_{output_filename}"
            with open(partial_file, 'w', encoding='utf-8') as f:
                json.dump(partial_data, f, indent=2, ensure_ascii=False)
            print(f"[*] Saved partial results to: {partial_file}")

if __name__ == "__main__":
    main()