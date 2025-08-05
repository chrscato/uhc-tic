#!/usr/bin/env python3
"""Extract all URLs from TOC file and analyze patterns comprehensively.

This script extracts all URLs from the TOC file and provides detailed analysis
of domains, state patterns, file naming conventions, and regional organization.
"""

import json
import gzip
import zlib
import requests
import os
import re
from collections import defaultdict, Counter
from typing import Dict, Any, List, Optional, Set, Tuple
from pathlib import Path
from datetime import datetime
import argparse
from urllib.parse import urlparse
from tic_mrf_scraper.utils.http_headers import get_cloudfront_headers

def extract_all_urls_streaming(url: str, chunk_size_mb: int = 100) -> List[str]:
    """Extract ALL URLs from the TOC file using streaming."""
    try:
        print(f"  [*] Extracting all URLs from file...")
        headers = get_cloudfront_headers()
        
        with requests.get(url, headers=headers, timeout=600, stream=True) as resp:
            resp.raise_for_status()
            
            # Set up decompressor
            if url.endswith('.gz'):
                decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
            
            buffer = ""
            downloaded_mb = 0
            all_urls = []
            chunk_size = chunk_size_mb * 1024 * 1024
            
            print(f"  [*] Processing file in {chunk_size_mb}MB chunks...")
            
            for chunk in resp.iter_content(chunk_size=8192):
                downloaded_mb += len(chunk) / 1024 / 1024
                
                if downloaded_mb % 500 < 0.1:  # Every 500MB
                    print(f"  [*] Processed {downloaded_mb:.0f}MB, found {len(all_urls)} URLs...")
                
                # Decompress chunk
                if url.endswith('.gz'):
                    try:
                        text_chunk = decompressor.decompress(chunk).decode('utf-8', errors='ignore')
                    except:
                        continue
                else:
                    text_chunk = chunk.decode('utf-8', errors='ignore')
                
                buffer += text_chunk
                
                # Extract all location URLs from buffer
                location_matches = re.findall(r'"location":\s*"([^"]+)"', buffer)
                all_urls.extend(location_matches)
                
                # Keep buffer manageable - only keep recent data for pattern matching
                if len(buffer) > 100000:
                    buffer = buffer[-50000:]
            
            print(f"\n  [+] Extraction complete: {len(all_urls)} total URLs found")
            return all_urls
            
    except Exception as e:
        print(f"  [!] Error extracting URLs: {str(e)}")
        return []

def analyze_url_patterns(urls: List[str]) -> Dict[str, Any]:
    """Comprehensive analysis of URL patterns."""
    print(f"  [*] Analyzing {len(urls)} URLs...")
    
    analysis = {
        "total_urls": len(urls),
        "domains": Counter(),
        "state_codes": Counter(),
        "file_patterns": Counter(),
        "path_patterns": Counter(),
        "regional_breakdown": defaultdict(list),
        "unique_domains": set(),
        "state_domain_mapping": defaultdict(set),
        "anomalies": []
    }
    
    # State abbreviation mapping for better detection
    state_mappings = {
        'al': 'Alabama', 'ak': 'Alaska', 'az': 'Arizona', 'ar': 'Arkansas', 'ca': 'California',
        'co': 'Colorado', 'ct': 'Connecticut', 'de': 'Delaware', 'fl': 'Florida', 'ga': 'Georgia',
        'hi': 'Hawaii', 'id': 'Idaho', 'il': 'Illinois', 'in': 'Indiana', 'ia': 'Iowa',
        'ks': 'Kansas', 'ky': 'Kentucky', 'la': 'Louisiana', 'me': 'Maine', 'md': 'Maryland',
        'ma': 'Massachusetts', 'mi': 'Michigan', 'mn': 'Minnesota', 'ms': 'Mississippi', 'mo': 'Missouri',
        'mt': 'Montana', 'ne': 'Nebraska', 'nv': 'Nevada', 'nh': 'New Hampshire', 'nj': 'New Jersey',
        'nm': 'New Mexico', 'ny': 'New York', 'nc': 'North Carolina', 'nd': 'North Dakota', 'oh': 'Ohio',
        'ok': 'Oklahoma', 'or': 'Oregon', 'pa': 'Pennsylvania', 'ri': 'Rhode Island', 'sc': 'South Carolina',
        'sd': 'South Dakota', 'tn': 'Tennessee', 'tx': 'Texas', 'ut': 'Utah', 'vt': 'Vermont',
        'va': 'Virginia', 'wa': 'Washington', 'wv': 'West Virginia', 'wi': 'Wisconsin', 'wy': 'Wyoming'
    }
    
    for url in urls:
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            path = parsed.path
            
            analysis["domains"][domain] += 1
            analysis["unique_domains"].add(domain)
            
            # Extract state patterns from domain
            domain_lower = domain.lower()
            found_states = []
            
            # Look for state codes in domain (e.g., anthembcbsnh, anthembcca)
            for state_code, state_name in state_mappings.items():
                if f"bcbs{state_code}" in domain_lower or f"anthem{state_code}" in domain_lower:
                    found_states.append(state_code)
                    analysis["state_codes"][state_code] += 1
                    analysis["state_domain_mapping"][state_code].add(domain)
                elif state_name.lower().replace(' ', '') in domain_lower:
                    found_states.append(state_code)
                    analysis["state_codes"][state_code] += 1
                    analysis["state_domain_mapping"][state_code].add(domain)
            
            # File pattern analysis
            filename = path.split('/')[-1] if '/' in path else path
            if filename:
                # Extract file pattern (remove specific identifiers but keep structure)
                pattern = re.sub(r'\d{4}-\d{2}_\d+_[A-Z0-9]+_', 'YYYY-MM_NNN_XXX_', filename)
                pattern = re.sub(r'_\d+_of_\d+', '_N_of_M', pattern)
                analysis["file_patterns"][pattern[:100]] += 1  # Limit length
            
            # Path pattern analysis
            path_parts = [p for p in path.split('/') if p]
            if len(path_parts) >= 2:
                path_pattern = '/'.join(path_parts[:2]) + '/...'
                analysis["path_patterns"][path_pattern] += 1
            
            # Regional breakdown
            if found_states:
                for state in found_states:
                    analysis["regional_breakdown"][state].append({
                        "domain": domain,
                        "sample_url": url[:100] + "..." if len(url) > 100 else url
                    })
            else:
                # URLs without clear state identification
                analysis["anomalies"].append({
                    "url": url[:200] + "..." if len(url) > 200 else url,
                    "domain": domain,
                    "reason": "No state pattern detected"
                })
                
        except Exception as e:
            analysis["anomalies"].append({
                "url": url[:200] + "..." if len(url) > 200 else url,
                "reason": f"Parse error: {str(e)}"
            })
    
    # Convert defaultdicts to regular dicts for JSON serialization
    analysis["regional_breakdown"] = dict(analysis["regional_breakdown"])
    analysis["state_domain_mapping"] = {k: list(v) for k, v in analysis["state_domain_mapping"].items()}
    analysis["unique_domains"] = list(analysis["unique_domains"])
    
    return analysis

def generate_comprehensive_report(analysis: Dict[str, Any]) -> str:
    """Generate a human-readable comprehensive report."""
    report = []
    report.append("=" * 80)
    report.append("COMPREHENSIVE URL PATTERN ANALYSIS REPORT")
    report.append("=" * 80)
    
    # Overview
    report.append(f"\n📊 OVERVIEW")
    report.append(f"   Total URLs analyzed: {analysis['total_urls']:,}")
    report.append(f"   Unique domains: {len(analysis['unique_domains'])}")
    report.append(f"   States detected: {len(analysis['state_codes'])}")
    report.append(f"   Anomalous URLs: {len(analysis['anomalies'])}")
    
    # Domain breakdown
    report.append(f"\n🌐 TOP DOMAINS")
    for domain, count in analysis["domains"].most_common(15):
        pct = (count / analysis["total_urls"]) * 100
        report.append(f"   {domain}: {count:,} URLs ({pct:.1f}%)")
    
    # State breakdown
    report.append(f"\n🗺️  STATE/REGIONAL BREAKDOWN")
    if analysis["state_codes"]:
        for state, count in analysis["state_codes"].most_common():
            domains = analysis["state_domain_mapping"].get(state, [])
            report.append(f"   {state.upper()}: {count:,} URLs")
            for domain in domains:
                report.append(f"      └─ {domain}")
    else:
        report.append("   ❌ No clear state patterns detected!")
    
    # Georgia specific analysis
    report.append(f"\n🍑 GEORGIA ANALYSIS")
    ga_urls = analysis["state_codes"].get("ga", 0)
    if ga_urls > 0:
        report.append(f"   ✅ Found {ga_urls:,} Georgia URLs")
        ga_domains = analysis["state_domain_mapping"].get("ga", [])
        for domain in ga_domains:
            report.append(f"      Domain: {domain}")
        
        # Show sample Georgia URLs
        ga_samples = analysis["regional_breakdown"].get("ga", [])[:5]
        report.append(f"   Sample Georgia URLs:")
        for sample in ga_samples:
            report.append(f"      • {sample['sample_url']}")
    else:
        report.append(f"   ❌ No Georgia URLs found")
        report.append(f"   💡 Georgia may be:")
        report.append(f"      • In a different TOC file")
        report.append(f"      • Using a different domain pattern")
        report.append(f"      • Named differently (e.g., 'georgia', 'atl', 'south')")
    
    # File patterns
    report.append(f"\n📁 FILE NAMING PATTERNS")
    for pattern, count in list(analysis["file_patterns"].most_common(10)):
        report.append(f"   {pattern}: {count:,} files")
    
    # Anomalies
    if analysis["anomalies"]:
        report.append(f"\n⚠️  ANOMALIES ({len(analysis['anomalies'])} found)")
        for anomaly in analysis["anomalies"][:10]:
            report.append(f"   • {anomaly['reason']}: {anomaly.get('domain', 'N/A')}")
            report.append(f"     URL: {anomaly['url']}")
        
        if len(analysis["anomalies"]) > 10:
            report.append(f"   ... and {len(analysis['anomalies']) - 10} more anomalies")
    
    # Recommendations
    report.append(f"\n💡 FILTERING RECOMMENDATIONS")
    if ga_urls > 0:
        ga_domains = analysis["state_domain_mapping"].get("ga", [])
        for domain in ga_domains:
            domain_pattern = domain.replace("anthembcbs", "").replace(".mrf.bcbs.com", "")
            report.append(f"   For Georgia: --pattern \"{domain_pattern}\"")
    else:
        report.append(f"   For Georgia: ❌ Not available in this file")
    
    # Show patterns for other states as examples
    report.append(f"   For other states:")
    for state, domains in list(analysis["state_domain_mapping"].items())[:5]:
        for domain in domains:
            domain_pattern = domain.replace("anthembcbs", "").replace(".mrf.bcbs.com", "")
            if domain_pattern and domain_pattern != domain:
                report.append(f"      {state.upper()}: --pattern \"{domain_pattern}\"")
                break
    
    return "\n".join(report)

def main():
    parser = argparse.ArgumentParser(description="Extract and analyze all URLs from TOC file")
    parser.add_argument("url", help="URL or path to TOC file")
    parser.add_argument("--output", default="url_analysis", help="Output directory")
    parser.add_argument("--chunk-size", type=int, default=100, help="Chunk size in MB")
    parser.add_argument("--save-urls", action="store_true", help="Save all URLs to a text file")
    args = parser.parse_args()
    
    # Create output directory
    output_path = Path(args.output)
    output_path.mkdir(exist_ok=True)
    
    print(f"\n[*] Extracting all URLs from: {args.url}")
    
    # Check file size
    try:
        headers = get_cloudfront_headers()
        resp = requests.head(args.url, headers=headers, timeout=30)
        size = int(resp.headers.get('content-length', 0))
        if size:
            size_gb = size / 1024 / 1024 / 1024
            print(f"[*] File size: {size_gb:.1f} GB")
            print(f"[*] Estimated processing time: {size_gb * 2:.0f}-{size_gb * 5:.0f} minutes")
    except:
        pass
    
    # Extract all URLs
    all_urls = extract_all_urls_streaming(args.url, args.chunk_size)
    
    if not all_urls:
        print("[X] No URLs extracted")
        return
    
    print(f"\n[*] Analyzing patterns...")
    
    # Analyze patterns
    analysis = analyze_url_patterns(all_urls)
    
    # Generate report
    report = generate_comprehensive_report(analysis)
    print(report)
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save detailed analysis
    analysis_file = output_path / f"complete_url_analysis_{timestamp}.json"
    with open(analysis_file, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)
    
    # Save human-readable report
    report_file = output_path / f"url_analysis_report_{timestamp}.txt"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    # Optionally save all URLs
    if args.save_urls:
        urls_file = output_path / f"all_urls_{timestamp}.txt"
        with open(urls_file, 'w', encoding='utf-8') as f:
            for url in all_urls:
                f.write(f"{url}\n")
        print(f"[+] All URLs saved to: {urls_file}")
    
    print(f"\n[+] Analysis complete!")
    print(f"[+] Detailed analysis: {analysis_file}")
    print(f"[+] Human report: {report_file}")
    print(f"[+] Total URLs processed: {len(all_urls):,}")

if __name__ == "__main__":
    main()