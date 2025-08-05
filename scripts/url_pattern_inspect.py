#!/usr/bin/env python3
"""Inspect URL patterns in TOC file to identify the right pattern for filtering.

This script samples URLs from the TOC file to help identify the correct pattern.
"""

import json
import gzip
import zlib
import requests
import os
import re
from collections import defaultdict
from typing import Dict, Any, List, Optional, Set
from pathlib import Path
from datetime import datetime
import argparse
from tic_mrf_scraper.utils.http_headers import get_cloudfront_headers

def extract_domain_patterns(url: str) -> Dict[str, str]:
    """Extract various patterns from a URL for analysis."""
    if not url:
        return {}
    
    try:
        # Basic parsing
        from urllib.parse import urlparse
        parsed = urlparse(url)
        
        patterns = {
            "domain": parsed.netloc,
            "path_start": "/".join(parsed.path.split("/")[:3]),
            "filename": parsed.path.split("/")[-1] if "/" in parsed.path else parsed.path
        }
        
        # Look for state codes or geographic identifiers
        url_lower = url.lower()
        
        # Common state patterns
        state_patterns = [
            "ga", "georgia", "bcbsga", "bluecrossga",
            "al", "alabama", "bcbsal", 
            "fl", "florida", "bcbsfl",
            "tn", "tennessee", "bcbstn",
            "nc", "northcarolina", "bcbsnc",
            "sc", "southcarolina", "bcbssc",
            "nh", "newhampshire", "bcbsnh",
            "ma", "massachusetts", "bcbsma",
            "ca", "california", "bcbsca"
        ]
        
        found_states = []
        for pattern in state_patterns:
            if pattern in url_lower:
                found_states.append(pattern)
        
        patterns["state_indicators"] = found_states
        
        return patterns
        
    except Exception as e:
        return {"error": str(e)}

def sample_url_patterns(url: str, max_samples: int = 1000, chunk_size_mb: int = 50) -> Dict[str, Any]:
    """Sample URL patterns from the TOC file."""
    try:
        print(f"  [*] Sampling URL patterns from file...")
        headers = get_cloudfront_headers()
        
        # Use smaller chunks for sampling
        chunk_size = chunk_size_mb * 1024 * 1024
        
        with requests.get(url, headers=headers, timeout=600, stream=True) as resp:
            resp.raise_for_status()
            
            # Set up decompressor
            if url.endswith('.gz'):
                decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
            
            buffer = ""
            downloaded_mb = 0
            samples = []
            domain_counts = defaultdict(int)
            state_patterns = defaultdict(int)
            
            print(f"  [*] Collecting URL samples...")
            
            for chunk in resp.iter_content(chunk_size=8192):
                downloaded_mb += len(chunk) / 1024 / 1024
                
                # Decompress chunk
                if url.endswith('.gz'):
                    try:
                        text_chunk = decompressor.decompress(chunk).decode('utf-8', errors='ignore')
                    except:
                        continue
                else:
                    text_chunk = chunk.decode('utf-8', errors='ignore')
                
                buffer += text_chunk
                
                # Look for location URLs in the buffer
                location_matches = re.findall(r'"location":\s*"([^"]+)"', buffer)
                
                for location_url in location_matches:
                    if len(samples) >= max_samples:
                        break
                    
                    samples.append(location_url)
                    
                    # Analyze patterns
                    patterns = extract_domain_patterns(location_url)
                    if "domain" in patterns:
                        domain_counts[patterns["domain"]] += 1
                    
                    for state in patterns.get("state_indicators", []):
                        state_patterns[state] += 1
                
                # Keep buffer manageable
                if len(buffer) > 50000:
                    buffer = buffer[-25000:]
                
                # Progress update
                if downloaded_mb % 50 < 0.1:
                    print(f"  [*] Processed {downloaded_mb:.0f}MB, found {len(samples)} URL samples...")
                
                if len(samples) >= max_samples:
                    break
            
            print(f"\n  [+] Sampling complete: {len(samples)} URLs collected")
            
            return {
                "total_samples": len(samples),
                "sample_urls": samples[:20],  # First 20 for inspection
                "domain_counts": dict(domain_counts),
                "state_patterns": dict(state_patterns),
                "all_samples": samples  # Keep all for further analysis
            }
            
    except Exception as e:
        print(f"  [!] Error sampling: {str(e)}")
        return {}

def analyze_georgia_patterns(samples: List[str]) -> List[str]:
    """Analyze samples to find potential Georgia patterns."""
    potential_patterns = []
    
    georgia_indicators = [
        "ga", "georgia", "bcbsga", "bluecrossga", "georgiahealth",
        "anthem.*ga", "bcbs.*ga", "blue.*cross.*ga"
    ]
    
    for pattern in georgia_indicators:
        matches = []
        for url in samples:
            if re.search(pattern, url.lower()):
                matches.append(url)
        
        if matches:
            potential_patterns.append({
                "pattern": pattern,
                "matches": len(matches),
                "sample_urls": matches[:3]
            })
    
    return potential_patterns

def main():
    parser = argparse.ArgumentParser(description="Inspect URL patterns in TOC file")
    parser.add_argument("url", help="URL or path to TOC file")
    parser.add_argument("--samples", type=int, default=2000, help="Number of URL samples to collect")
    parser.add_argument("--chunk-size", type=int, default=100, help="Chunk size in MB")
    parser.add_argument("--search-pattern", help="Specific pattern to search for")
    args = parser.parse_args()
    
    print(f"\n[*] Inspecting URL patterns in: {args.url}")
    
    # Sample URL patterns
    results = sample_url_patterns(args.url, args.samples, args.chunk_size)
    
    if not results:
        print("[X] Failed to sample URLs")
        return
    
    print(f"\n[*] Analysis Results:")
    print(f"=" * 60)
    
    # Show domain distribution
    print(f"\n[DOMAINS] Top domains found:")
    domain_counts = results.get("domain_counts", {})
    for domain, count in sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {domain}: {count} URLs")
    
    # Show state patterns
    print(f"\n[STATE PATTERNS] Geographic indicators found:")
    state_patterns = results.get("state_patterns", {})
    if state_patterns:
        for pattern, count in sorted(state_patterns.items(), key=lambda x: x[1], reverse=True):
            print(f"  '{pattern}': {count} URLs")
    else:
        print("  No common state patterns detected")
    
    # Look for Georgia specifically
    print(f"\n[GEORGIA ANALYSIS] Searching for Georgia patterns...")
    all_samples = results.get("all_samples", [])
    georgia_patterns = analyze_georgia_patterns(all_samples)
    
    if georgia_patterns:
        print("  Found potential Georgia patterns:")
        for pattern_info in georgia_patterns:
            print(f"    Pattern '{pattern_info['pattern']}': {pattern_info['matches']} matches")
            for sample_url in pattern_info['sample_urls']:
                print(f"      - {sample_url}")
    else:
        print("  ❌ No obvious Georgia patterns found in samples")
        print("  💡 Try these alternative patterns:")
        print("    --pattern 'georgia'")
        print("    --pattern 'anthem.*13'  (if Georgia has a specific code)")
        print("    --pattern 'bcbs.*atlanta'")
    
    # Custom search if provided
    if args.search_pattern:
        print(f"\n[CUSTOM SEARCH] Searching for pattern: '{args.search_pattern}'")
        matches = []
        for url in all_samples:
            if re.search(args.search_pattern.lower(), url.lower()):
                matches.append(url)
        
        print(f"  Found {len(matches)} matches:")
        for match in matches[:10]:
            print(f"    - {match}")
        
        if len(matches) > 10:
            print(f"    ... and {len(matches) - 10} more")
    
    # Show sample URLs for manual inspection
    print(f"\n[SAMPLE URLs] First 10 URLs for manual inspection:")
    sample_urls = results.get("sample_urls", [])[:10]
    for i, url in enumerate(sample_urls, 1):
        print(f"  {i:2d}. {url}")
    
    # Save detailed results
    output_path = Path("url_analysis")
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_path / f"url_patterns_{timestamp}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n[+] Detailed analysis saved to: {output_file}")
    print(f"[+] Use the results above to identify the correct pattern for filtering")

if __name__ == "__main__":
    main()