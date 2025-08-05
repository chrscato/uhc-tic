#!/usr/bin/env python3
"""Test script to validate provider reference URL extraction from MRF files."""

import json
import yaml
import random
import gzip
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, Any, List, Optional
from io import BytesIO

from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced, fetch_url
from tic_mrf_scraper.schema.detector import SchemaDetector
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

# Set seed for reproducible random sampling
random.seed(42)

# Configure logging
logger = get_logger(__name__)

def load_payer_endpoints() -> Dict[str, str]:
    """Load payer endpoints from production config."""
    try:
        with open("production_config.yaml", 'r') as f:
            config = yaml.safe_load(f)
            return config.get("payer_endpoints", {})
    except Exception as e:
        logger.error(f"Failed to load config: {str(e)}")
        return {}

def decompress_if_needed(content: bytes, url: str) -> Dict[str, Any]:
    """Decompress gzipped content if needed and parse JSON."""
    try:
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                return json.load(gz)
        return json.loads(content.decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to decompress/parse content: {str(e)}")
        raise

def analyze_provider_references(data: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze provider references structure in MRF file."""
    analysis = {
        "pattern_type": None,
        "provider_references_count": 0,
        "external_urls": [],
        "sample_external_fetch": None
    }
    
    # First check for provider references at top level
    refs = []
    if "provider_references" in data:
        refs = data["provider_references"]
        logger.info(f"Found {len(refs)} provider references at top level")
    elif "provider_groups" in data:
        refs = data["provider_groups"]
        logger.info(f"Found {len(refs)} provider groups at top level")
    
    # If no refs at top level, check in_network items
    if not refs and "in_network" in data:
        logger.info("Checking in_network items for provider references...")
        for item in data["in_network"]:
            if isinstance(item, dict):
                if "provider_references" in item:
                    refs.extend(item["provider_references"])
                elif "provider_groups" in item:
                    refs.extend(item["provider_groups"])
        if refs:
            logger.info(f"Found {len(refs)} provider references in in_network items")
    
    if not refs:
        logger.warning("No provider references found in data")
        return analysis
    
    analysis["provider_references_count"] = len(refs)
    
    # Analyze reference structure
    has_urls = False
    has_inline = False
    
    for ref in refs:
        if isinstance(ref, dict):
            if "location" in ref:
                has_urls = True
                url = ref["location"]
                if url not in analysis["external_urls"]:
                    analysis["external_urls"].append(url)
            if "providers" in ref or "provider_groups" in ref:
                has_inline = True
    
    # Determine pattern type
    if has_urls:
        analysis["pattern_type"] = "prov_ref_url"
        logger.info(f"Found {len(analysis['external_urls'])} unique provider reference URLs")
    elif has_inline:
        analysis["pattern_type"] = "prov_ref_infile"
        logger.info("Found inline provider data")
    
    # Test one external URL if available
    if analysis["external_urls"]:
        test_url = analysis["external_urls"][0]
        try:
            logger.info(f"Testing external provider reference URL: {test_url}")
            content = fetch_url(test_url)
            provider_data = decompress_if_needed(content, test_url)
            
            # Extract sample provider info
            sample_npi = None
            provider_groups = 0
            
            if isinstance(provider_data, dict):
                groups = provider_data.get("provider_groups", [])
                provider_groups = len(groups)
                
                # Look for first NPI
                for group in groups:
                    for provider in group.get("providers", []):
                        if "npi" in provider:
                            sample_npi = provider["npi"]
                            break
                    if sample_npi:
                        break
            
            analysis["sample_external_fetch"] = {
                "url": test_url,
                "success": True,
                "provider_groups_found": provider_groups,
                "sample_npi": sample_npi
            }
        except Exception as e:
            logger.error(f"Failed to fetch external URL: {str(e)}")
            analysis["sample_external_fetch"] = {
                "url": test_url,
                "success": False,
                "error": str(e)
            }
    
    return analysis

def analyze_payer(payer_name: str, index_url: str) -> Dict[str, Any]:
    """Analyze provider reference patterns for a payer."""
    logger.info(f"\nAnalyzing payer: {payer_name}")
    logger.info(f"Index URL: {index_url}")
    
    result = {
        "payer_name": payer_name,
        "index_url": index_url,
        "total_in_network_files": 0,
        "sampled_files_count": 0,
        "analyzed_file": None,
        "provider_reference_analysis": None,
        "error": None
    }
    
    try:
        # Get all in_network files
        all_files = [f for f in list_mrf_blobs_enhanced(index_url) if f["type"] == "in_network_rates"]
        result["total_in_network_files"] = len(all_files)
        
        if not all_files:
            logger.warning(f"No in-network files found for {payer_name}")
            return result
        
        # Filter out tiny files (likely empty or minimal content)
        logger.info("Filtering files by size hints...")
        filtered_files = []
        for f in all_files:
            # Look for size hints in URL or metadata
            has_size_hint = False
            if "size" in f:
                has_size_hint = f["size"] > 1000  # At least 1KB
            elif "content_length" in f:
                has_size_hint = f["content_length"] > 1000
            else:
                # Look for hints in URL that suggest substantial content
                url_lower = f["url"].lower()
                has_size_hint = any(hint in url_lower for hint in 
                    ["full", "complete", "all", "comprehensive", "rates", "network"])
            
            if has_size_hint:
                filtered_files.append(f)
        
        if filtered_files:
            logger.info(f"Found {len(filtered_files)} files with size hints")
            all_files = filtered_files
        
        # Sample random files
        sample_size = min(12, len(all_files))
        sampled_files = random.sample(all_files, sample_size)
        result["sampled_files_count"] = sample_size
        
        print(f"\n{'-' * 80}")
        print(f"Selected {sample_size} random files for {payer_name}:")
        print(f"{'-' * 80}")
        for i, file in enumerate(sampled_files, 1):
            print(f"\n{i}. URL: {file['url']}")
            if "plan_name" in file:
                print(f"   Plan: {file['plan_name']}")
            if "size" in file:
                print(f"   Size: {file['size']} bytes")
            elif "content_length" in file:
                print(f"   Size: {file['content_length']} bytes")
        print(f"\n{'-' * 80}")
        
        # Try files until we find one with provider references
        for test_file in sampled_files:
            print(f"\nTesting file: {test_file['url']}")
            try:
                content = fetch_url(test_file["url"])
                data = decompress_if_needed(content, test_file["url"])
                
                # Log file structure
                if isinstance(data, dict):
                    top_keys = list(data.keys())
                    print(f"   Top level keys: {top_keys}")
                    
                    # Show provider reference info if present
                    if "provider_references" in data:
                        refs = data["provider_references"]
                        print(f"   Found {len(refs)} provider references at top level")
                        if refs:
                            print(f"   Sample reference keys: {list(refs[0].keys())}")
                    
                    # Show in_network info
                    if "in_network" in data:
                        in_network = data["in_network"]
                        print(f"   Found {len(in_network)} in_network items")
                        if in_network:
                            print(f"   Sample in_network keys: {list(in_network[0].keys())}")
                        
                        # Check sample of in_network items
                        for item in data["in_network"][:sample_size]:
                            if isinstance(item, dict) and ("provider_references" in item or "provider_groups" in item):
                                logger.info("Found provider references in in_network items")
                                result["analyzed_file"] = {
                                    "url": test_file["url"],
                                    "plan_name": test_file.get("plan_name", "Unknown")
                                }
                                result["provider_reference_analysis"] = analyze_provider_references(data)
                                return result
                
                # If we found provider references at top level
                if "provider_references" in data or "provider_groups" in data:
                    result["analyzed_file"] = {
                        "url": test_file["url"],
                        "plan_name": test_file.get("plan_name", "Unknown")
                    }
                    result["provider_reference_analysis"] = analyze_provider_references(data)
                    return result
                    
            except Exception as e:
                logger.warning(f"Failed to analyze {test_file['url']}: {str(e)}")
                continue
        
        # If we get here, we didn't find any files with provider references
        logger.warning("No files with provider references found in sample")
        
        # Analyze smallest file
        logger.info(f"Analyzing smallest file: {smallest_file['url']}")
        content = fetch_url(smallest_file["url"])
        data = decompress_if_needed(content, smallest_file["url"])
        
        # Log file structure for debugging
        if isinstance(data, dict):
            logger.info(f"File structure - top level keys: {list(data.keys())}")
            if "provider_references" in data:
                logger.info(f"Found {len(data['provider_references'])} provider references")
                if data['provider_references']:
                    sample_ref = data['provider_references'][0]
                    logger.info(f"Sample provider reference keys: {list(sample_ref.keys())}")
        else:
            logger.warning("File content is not a dictionary")
        
        # Analyze provider references
        result["provider_reference_analysis"] = analyze_provider_references(data)
        
    except Exception as e:
        logger.error(f"Error analyzing {payer_name}: {str(e)}")
        result["error"] = str(e)
    
    return result

def main():
    """Run provider reference extraction test."""
    setup_logging()
    
    # Load payer endpoints
    payer_endpoints = load_payer_endpoints()
    if not payer_endpoints:
        logger.error("No payer endpoints found in config")
        return
    
    # Analyze each payer
    results = []
    for payer_name, index_url in payer_endpoints.items():
        if not index_url or index_url.startswith("file://"):
            logger.info(f"Skipping {payer_name} (local file or empty URL)")
            continue
            
        result = analyze_payer(payer_name, index_url)
        results.append(result)
    
    # Save results
    output = {
        "timestamp": datetime.now(UTC).isoformat(),
        "payer_results": results
    }
    
    output_file = Path("test_provider_ref_results.json")
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)
    
    logger.info(f"\nResults saved to {output_file}")
    
    # Print summary
    print("\nSummary:")
    print("-" * 50)
    for result in results:
        print(f"\nPayer: {result['payer_name']}")
        print(f"Total in-network files: {result['total_in_network_files']}")
        print(f"Files sampled: {result['sampled_files_count']}")
        
        if result.get("error"):
            print(f"Error: {result['error']}")
            continue
            
        analysis = result.get("provider_reference_analysis")
        if analysis:
            print(f"Pattern type: {analysis['pattern_type']}")
            print(f"Provider references: {analysis['provider_references_count']}")
            if analysis['pattern_type'] == 'prov_ref_url':
                print(f"External URLs found: {len(analysis['external_urls'])}")
                fetch_result = analysis.get('sample_external_fetch', {})
                if fetch_result:
                    status = "✓" if fetch_result.get('success') else "✗"
                    print(f"External fetch test: {status}")
                    if fetch_result.get('success'):
                        print(f"  Provider groups: {fetch_result['provider_groups_found']}")
                        print(f"  Sample NPI: {fetch_result['sample_npi']}")

if __name__ == "__main__":
    main()