#!/usr/bin/env python3
"""Create a simple sample from a large JSON/gzipped JSON file.

This script downloads a portion of a large file and extracts whatever valid JSON it can find.
"""

import json
import gzip
import zlib
import requests
import os
from io import BytesIO
from typing import Dict, Any, Optional
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

def download_and_decompress(url: str, chunk_size_mb: int = 50) -> Optional[str]:
    """Download and decompress a portion of the file."""
    try:
        # Check file size first
        size = get_file_size(url)
        if size:
            size_mb = size / 1024 / 1024
            print(f"  [*] File size: {size_mb:.1f} MB")
        
        print(f"  [*] Downloading first {chunk_size_mb}MB...")
        headers = get_cloudfront_headers()
        
        # Download a chunk
        download_size = chunk_size_mb * 1024 * 1024
        if size and download_size > size:
            download_size = size
        else:
            headers['Range'] = f'bytes=0-{download_size - 1}'
        
        with requests.get(url, headers=headers, timeout=300, stream=True) as resp:
            resp.raise_for_status()
            
            content = b''
            downloaded = 0
            for chunk in resp.iter_content(chunk_size=8192):
                content += chunk
                downloaded += len(chunk)
                if downloaded >= download_size:
                    break
            
            print(f"  [*] Downloaded {len(content) / 1024 / 1024:.1f} MB")
            
            # Decompress if needed
            if url.endswith('.gz'):
                print("  [*] Decompressing...")
                try:
                    # Try simple decompression first
                    if not headers.get('Range'):
                        decompressed = gzip.decompress(content)
                    else:
                        # For partial files, try zlib decompression
                        # Skip gzip header (usually 10 bytes, but can vary)
                        for skip in [10, 12, 18]:  # Try different header lengths
                            try:
                                decompressor = zlib.decompressobj(-zlib.MAX_WBITS)
                                decompressed = decompressor.decompress(content[skip:])
                                break
                            except:
                                continue
                        else:
                            raise Exception("Could not decompress with any header skip length")
                    
                    content_str = decompressed.decode('utf-8')
                    print(f"  [*] Decompressed to {len(content_str) / 1024 / 1024:.1f} MB text")
                    return content_str
                    
                except Exception as e:
                    print(f"  [!] Decompression failed: {str(e)}")
                    return None
            else:
                return content.decode('utf-8')
                
    except Exception as e:
        print(f"  [!] Error downloading: {str(e)}")
        return None

def extract_json_sample(content: str, max_items: int = 5) -> Optional[Dict[str, Any]]:
    """Extract any valid JSON sample from the content."""
    try:
        # Find the start of JSON
        json_start = content.find('{')
        if json_start == -1:
            print("  [!] No JSON object found")
            return None
        
        content = content[json_start:]
        
        # Try to parse the entire content as JSON first
        try:
            data = json.loads(content)
            print("  [+] Successfully parsed complete JSON")
            return limit_sample_size(data, max_items)
        except json.JSONDecodeError:
            print("  [*] Content is not complete JSON, extracting partial sample...")
        
        # If that fails, try to extract a partial but valid JSON object
        # Look for key patterns and try to build something valid
        
        # Try to find the end of the first complete object
        brace_count = 0
        pos = 0
        
        for i, char in enumerate(content):
            if char == '{':
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0:
                    # Found a complete object
                    try:
                        sample_json = content[:i + 1]
                        data = json.loads(sample_json)
                        print("  [+] Extracted complete JSON object")
                        return limit_sample_size(data, max_items)
                    except:
                        continue
        
        # If we still can't find a complete object, try to extract key-value pairs
        print("  [*] Attempting to extract metadata...")
        result = {}
        
        # Look for simple key-value pairs at the start
        lines = content[:10000].split('\n')  # Just look at first 10k chars
        for line in lines:
            line = line.strip()
            if ':' in line and not line.startswith('{') and not line.startswith('['):
                try:
                    # Try to extract key-value pairs
                    if line.startswith('"') and '":' in line:
                        key_end = line.find('":')
                        key = line[1:key_end]
                        value_part = line[key_end + 2:].strip().rstrip(',')
                        
                        # Try to parse the value
                        try:
                            if value_part.startswith('"') and value_part.endswith('"'):
                                result[key] = value_part[1:-1]
                            elif value_part.lower() in ['true', 'false']:
                                result[key] = value_part.lower() == 'true'
                            elif value_part.isdigit():
                                result[key] = int(value_part)
                            else:
                                result[key] = value_part
                        except:
                            result[key] = value_part
                except:
                    continue
        
        if result:
            print(f"  [+] Extracted {len(result)} metadata fields")
            return result
        
        print("  [!] Could not extract any valid JSON")
        return None
        
    except Exception as e:
        print(f"  [!] Error extracting JSON: {str(e)}")
        return None

def limit_sample_size(data: Any, max_items: int = 5) -> Any:
    """Limit the size of arrays and objects to create a reasonable sample."""
    if isinstance(data, dict):
        result = {}
        for i, (key, value) in enumerate(data.items()):
            if i >= max_items * 10:  # Don't process too many keys
                result["..."] = f"truncated ({len(data) - i} more items)"
                break
            result[key] = limit_sample_size(value, max_items)
        return result
    elif isinstance(data, list):
        if len(data) <= max_items:
            return [limit_sample_size(item, max_items) for item in data]
        else:
            sample = [limit_sample_size(item, max_items) for item in data[:max_items]]
            sample.append(f"... truncated ({len(data) - max_items} more items)")
            return sample
    else:
        return data

def main():
    parser = argparse.ArgumentParser(description="Create a simple sample from a large JSON file")
    parser.add_argument("url", help="URL or path to JSON/gzipped file")
    parser.add_argument("--output", default="samples", help="Output directory")
    parser.add_argument("--chunk-size", type=int, default=50, help="Chunk size in MB to download")
    parser.add_argument("--max-items", type=int, default=5, help="Max items to keep in arrays")
    args = parser.parse_args()
    
    # Create output directory
    output_path = Path(args.output)
    output_path.mkdir(exist_ok=True)
    
    print(f"\n[*] Processing file: {args.url}")
    
    # Download and decompress
    content = download_and_decompress(args.url, args.chunk_size)
    if not content:
        print("[X] Failed to download/decompress file")
        return
    
    # Extract JSON sample
    sample = extract_json_sample(content, args.max_items)
    if not sample:
        print("[X] Failed to extract JSON sample, but will still save raw text")
        sample = {"error": "Could not extract valid JSON sample"}
    
    # Save both the JSON sample and raw text
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.basename(args.url).replace('.gz', '').replace('.json', '')
    
    # Save JSON sample
    json_output_file = output_path / f"{filename}_sample_{timestamp}.json"
    with open(json_output_file, 'w', encoding='utf-8') as f:
        json.dump(sample, f, indent=2, ensure_ascii=False)
    
    # Save raw text file (first chunk for inspection)
    txt_output_file = output_path / f"{filename}_raw_{timestamp}.txt"
    with open(txt_output_file, 'w', encoding='utf-8') as f:
        # Save first 1MB of the decompressed content for inspection
        f.write(content[:1024*1024] if len(content) > 1024*1024 else content)
    
    print(f"\n[+] Saved JSON sample to: {json_output_file}")
    print(f"[+] Saved raw text to: {txt_output_file}")
    print(f"[+] Sample contains {len(sample)} top-level fields")
    print(f"[+] Raw text contains {len(content) / 1024 / 1024:.1f} MB of decompressed data")
    
    # Show a preview
    print("\n[*] Sample preview:")
    preview = json.dumps(sample, indent=2, ensure_ascii=False)[:1000]
    print(preview)
    if len(preview) >= 1000:
        print("... (truncated)")

if __name__ == "__main__":
    main()