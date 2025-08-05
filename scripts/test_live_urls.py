#!/usr/bin/env python3
"""Test script for live MRF URLs."""

import json
import argparse
from typing import Dict, Any, Optional, List, Set
from pathlib import Path
import requests
import gzip
from io import BytesIO
import re
import browser_cookie3
import urllib.parse
import yaml

from tic_mrf_scraper.schema.detector import SchemaDetector
from tic_mrf_scraper.parsers.factory import ParserFactory
from tic_mrf_scraper.stream.dynamic_parser import DynamicStreamingParser
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger
from tic_mrf_scraper.fetch.blobs import get_cloudfront_headers

logger = get_logger(__name__)

def is_gzipped(url: str, headers: Dict[str, str], content: bytes) -> bool:
    """
    Determine if content is gzipped.

    Args:
        url: The URL of the content
        headers: Response headers
        content: Raw content bytes

    Returns:
        bool: True if content is gzipped
    """
    # Check content-encoding header
    if headers.get('content-encoding') == 'gzip':
        return True
    
    # Check URL pattern (ignoring query params)
    if re.search(r'\.gz([?#]|$)', url):
        return True
    
    # Check magic numbers for gzip
    return content.startswith(b'\x1f\x8b')

def analyze_url_auth(url: str) -> Dict[str, Any]:
    """Analyze URL for authentication requirements."""
    auth_info = {
        "requires_auth": False,
        "auth_type": None,
        "details": []
    }
    
    # Check for CloudFront signed URLs
    if "cloudfront.net" in url.lower():
        auth_info["requires_auth"] = True
        auth_info["auth_type"] = "CloudFront Signed URL"
        if "Key-Pair-Id=" in url:
            auth_info["details"].append("Uses CloudFront Key Pair authentication")
        if "Signature=" in url:
            auth_info["details"].append("Contains CloudFront signature")
        if "Expires=" in url:
            auth_info["details"].append("Has expiration timestamp")
    
    # Add other auth checks as needed
    return auth_info

def get_browser_cookies(url: str) -> Dict[str, str]:
    """Get cookies from installed browsers for the given URL."""
    domain = urllib.parse.urlparse(url).netloc
    cookies = {}
    
    # Try different browsers
    browsers = [
        ('Chrome', browser_cookie3.chrome),
        ('Firefox', browser_cookie3.firefox),
        ('Edge', browser_cookie3.edge),
        ('Safari', browser_cookie3.safari),
    ]
    
    for browser_name, browser_func in browsers:
        try:
            browser_cookies = browser_func(domain_name=domain)
            if browser_cookies:
                print(f"Found cookies from {browser_name}")
                for cookie in browser_cookies:
                    cookies[cookie.name] = cookie.value
        except Exception:
            continue
    
    return cookies

def fetch_url_content(url: str, ignore_errors: bool = False) -> Optional[Dict[str, Any]]:
    """
    Fetch and parse JSON content from URL.

    Args:
        url: URL to fetch
        ignore_errors: If True, return None on errors instead of raising exceptions

    Returns:
        Dict containing parsed JSON data, or None if ignore_errors is True and fetch fails
    """
    logger.info(f"Fetching {url}")
    
    # Get CloudFront-compatible headers
    headers = get_cloudfront_headers()
    
    try:
        # Get browser cookies for the domain
        cookies = get_browser_cookies(url)
        if cookies:
            print(f"Using {len(cookies)} cookies from your browser")
        
        # Stream the response to handle large files
        with requests.get(url, stream=True, cookies=cookies, headers=headers) as response:
            if response.status_code == 403:
                auth_info = analyze_url_auth(url)
                if auth_info["requires_auth"]:
                    error_msg = f"Access denied (403). URL requires authentication:\n"
                    error_msg += f"- Auth type: {auth_info['auth_type']}\n"
                    for detail in auth_info["details"]:
                        error_msg += f"- {detail}\n"
                    logger.warning(error_msg)
                    if ignore_errors:
                        return None
                    raise requests.exceptions.HTTPError(error_msg)
            
            response.raise_for_status()
            
            # Read raw content into BytesIO
            content = BytesIO()
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive chunks
                    content.write(chunk)
            content.seek(0)
            raw_content = content.getvalue()

            # Check if content is gzipped
            is_gz = is_gzipped(url, response.headers, raw_content)
            print(f"Content type: {'gzipped' if is_gz else 'plain'}")

            if is_gz:
                try:
                    print("Decompressing gzipped content...")
                    # Decompress gzip
                    decompressed = gzip.decompress(raw_content)
                    print(f"Successfully decompressed content (size: {len(decompressed)} bytes)")
                    
                    # Parse JSON
                    return json.loads(decompressed)
                except Exception as e:
                    logger.error(f"Error decompressing gzip content: {str(e)}")
                    if ignore_errors:
                        return None
                    raise
            else:
                # For non-gzipped content
                content_str = raw_content.decode('utf-8')
                return json.loads(content_str)
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch URL {url}: {str(e)}")
        if ignore_errors:
            return None
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from {url}: {str(e)}")
        if ignore_errors:
            return None
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching {url}: {str(e)}")
        if ignore_errors:
            return None
        raise

def extract_provider_ref_urls(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract provider reference URLs from in-network file.

    Args:
        data: In-network file JSON data

    Returns:
        List of provider reference entries with URLs
    """
    provider_refs = []
    
    if "provider_references" in data:
        for ref in data["provider_references"]:
            if "provider_group_id" in ref and "location" in ref:
                provider_refs.append({
                    "id": ref["provider_group_id"],
                    "url": ref["location"]
                })
    
    return provider_refs

def load_cpt_whitelist():
    """Load CPT whitelist from production config."""
    try:
        with open("production_config.yaml", 'r') as f:
            import yaml
            config = yaml.safe_load(f)
            return set(config.get('cpt_whitelist', []))
    except Exception as e:
        print(f"Warning: Could not load CPT whitelist: {str(e)}")
        return set()

def get_toc_url(url: str) -> Optional[str]:
    """Get TOC URL for a given MRF URL."""
    # Extract domain and base path
    parsed = urllib.parse.urlparse(url)
    domain = parsed.netloc
    
    # Look up in config
    with open("production_config.yaml", 'r') as f:
        config = yaml.safe_load(f)
        for payer, toc_url in config.get('endpoints', {}).items():
            if isinstance(toc_url, str) and domain in toc_url:
                return toc_url
    return None

def test_mrf_url(in_network_url: str):
    """
    Test parsing a live MRF URL.

    Args:
        in_network_url: URL to in_network file
    """
    print(f"\nTesting in_network URL: {in_network_url}")
    print("-" * 50)
    
    # Try to find TOC URL
    toc_url = get_toc_url(in_network_url)
    if toc_url:
        print(f"\nFound TOC URL: {toc_url}")
        try:
            print("Fetching TOC file...")
            toc_data = fetch_url_content(toc_url)
            if toc_data:
                print("✓ Successfully fetched TOC")
                # Extract fresh URLs from TOC
                print("\nExtracting fresh URLs from TOC...")
                
                # Find matching file entry
                target_filename = Path(in_network_url).name
                fresh_url = None
                
                for file_entry in toc_data.get("in_network_files", []):
                    if file_entry.get("file_name") == target_filename:
                        fresh_url = file_entry.get("url")
                        print(f"Found fresh URL for {target_filename}")
                        break
                
                if fresh_url:
                    print("Using fresh URL from TOC")
                    in_network_url = fresh_url
                else:
                    print("Warning: Could not find fresh URL in TOC")
        except Exception as e:
            print(f"✗ Failed to fetch TOC: {str(e)}")
    else:
        print("\nNo TOC URL found in config for this domain")

    try:
        # Load CPT whitelist
        cpt_whitelist = load_cpt_whitelist()
        print(f"\nLoaded {len(cpt_whitelist)} CPT codes from whitelist")

        # Load and detect schema
        print("\nFetching and parsing in-network file...")
        data = fetch_url_content(in_network_url)
        print("Successfully loaded in-network file")
        
        # Print sample of data to verify content
        print("\nSample of loaded data:")
        if isinstance(data, dict):
            keys = list(data.keys())
            print(f"Top-level keys: {keys}")
            if "provider_references" in data:
                print(f"Number of provider references: {len(data['provider_references'])}")
            if "in_network" in data:
                print(f"Number of in_network items: {len(data['in_network'])}")
        
        detector = SchemaDetector()
        schema_type = detector.detect_schema(data)
        print(f"\nDetected schema type: {schema_type}")

        # Initialize counters for whitelisted vs non-whitelisted
        total_codes_seen = set()
        whitelisted_codes_seen = set()
        total_records = 0
        whitelisted_records = 0

        provider_refs = []
        if schema_type == "prov_ref_url":
            # Extract provider reference URLs but don't try to fetch them
            provider_refs = extract_provider_ref_urls(data)
            print(f"\nFound {len(provider_refs)} provider references")
            
            # Extract provider info from the main file instead
            print("\nExtracting provider information from main file...")
            provider_info = {}
            
            # Look for provider data in the main file structure
            if "provider_references" in data:
                for ref in data["provider_references"]:
                    if "provider_group_id" in ref:
                        provider_id = ref["provider_group_id"]
                        provider_info[provider_id] = {
                            "group_id": provider_id,
                            "providers": ref.get("providers", []),
                            "provider_groups": ref.get("provider_groups", []),
                            "npi_numbers": [],
                            "tin_numbers": []
                        }
                        
                        # Extract NPIs and TINs from the provider data
                        for provider in ref.get("providers", []):
                            if "npi" in provider:
                                provider_info[provider_id]["npi_numbers"].append(provider["npi"])
                            if "tin" in provider:
                                provider_info[provider_id]["tin_numbers"].append(provider["tin"])
            
            print(f"\nExtracted information for {len(provider_info)} provider groups")
            print("\nProvider Group Statistics:")
            total_npi = sum(len(info["npi_numbers"]) for info in provider_info.values())
            total_tin = sum(len(info["tin_numbers"]) for info in provider_info.values())
            print(f"- Total NPIs found: {total_npi}")
            print(f"- Total TINs found: {total_tin}")
            
            # Continue processing as in-network rates
            print("\nProcessing as in-network rates...")
            schema_type = "in_network"

        # Create parser
        parser_factory = ParserFactory()
        parser = parser_factory.create_parser(data, payer_name="TEST_PAYER")
        if not parser:
            print("Failed to create parser")
            return

        # Parse records
        print("\nParsing records...")
        record_count = 0
        provider_count = 0
        rate_count = 0
        unique_npis = set()
        unique_tins = set()
        billing_codes: Dict[str, int] = {}  # Track billing code frequencies
        whitelisted_billing_codes: Dict[str, int] = {}  # Track whitelisted code frequencies

        dynamic_parser = DynamicStreamingParser(
            payer_name="TEST_PAYER",
            chunk_size=1000  # Process in smaller chunks for large files
        )

        for record in dynamic_parser.parse_stream(data, schema_type=schema_type, parser=parser):
            if record_count == 0:
                print("\nSample record:")
                print(json.dumps(record, indent=2))

            record_count += 1
            
            # Track providers
            if "provider_npi" in record:
                provider_count += 1
                unique_npis.add(record["provider_npi"])
            if "provider_tin" in record:
                unique_tins.add(record["provider_tin"])
            
            # Track rates and billing codes
            if "negotiated_rate" in record:
                rate_count += 1

            # Handle both billing_code and service_code fields
            code = record.get("billing_code") or record.get("service_code")
            if code:
                billing_codes[code] = billing_codes.get(code, 0) + 1
                
                # Check if code is in whitelist
                if code in cpt_whitelist:
                    whitelisted_billing_codes[code] = whitelisted_billing_codes.get(code, 0) + 1

            # Progress update
            if record_count % 1000 == 0:
                print(f"Processed {record_count} records...")

        # Generate summary
        print(f"\nResults:")
        print(f"Total records: {record_count}")
        print(f"Provider records: {provider_count}")
        print(f"Rate records: {rate_count}")
        print(f"Unique NPIs: {len(unique_npis)}")
        print(f"Unique TINs: {len(unique_tins)}")
        
        # Show billing code distribution
        print(f"\nTop 10 billing codes (all):")
        top_codes = sorted(billing_codes.items(), key=lambda x: x[1], reverse=True)[:10]
        for code, count in top_codes:
            print(f"- {code}: {count} records")

        # Show whitelisted billing code distribution
        print(f"\nWhitelisted billing codes ({len(whitelisted_billing_codes)} found):")
        whitelisted_top = sorted(whitelisted_billing_codes.items(), key=lambda x: x[1], reverse=True)
        for code, count in whitelisted_top:
            print(f"- {code}: {count} records")

        # Calculate whitelisted percentages
        total_code_records = sum(billing_codes.values())
        whitelisted_code_records = sum(whitelisted_billing_codes.values())
        whitelist_pct = (whitelisted_code_records / total_code_records * 100) if total_code_records > 0 else 0
        print(f"\nWhitelist coverage:")
        print(f"Total records with codes: {total_code_records}")
        print(f"Records with whitelisted codes: {whitelisted_code_records}")
        print(f"Percentage whitelisted: {whitelist_pct:.2f}%")

        # Save detailed analysis
        output_dir = Path("parser_test_results")
        output_dir.mkdir(exist_ok=True)

        analysis = {
            "url": in_network_url,
            "schema_type": schema_type,
            "stats": {
                "total_records": record_count,
                "provider_records": provider_count,
                "rate_records": rate_count,
                "unique_npis": len(unique_npis),
                "unique_tins": len(unique_tins),
                "total_code_records": total_code_records,
                "whitelisted_code_records": whitelisted_code_records,
                "whitelist_coverage_pct": whitelist_pct
            },
            "provider_references": provider_refs if schema_type == "prov_ref_url" else None,
            "billing_codes": {
                "all": dict(sorted(billing_codes.items(), key=lambda x: x[1], reverse=True)),
                "whitelisted": dict(sorted(whitelisted_billing_codes.items(), key=lambda x: x[1], reverse=True))
            },
            "sample_npis": list(unique_npis)[:10],  # First 10 NPIs
            "sample_tins": list(unique_tins)[:10]   # First 10 TINs
        }

        analysis_file = output_dir / f"analysis_{Path(in_network_url).name.split('?')[0]}"
        with open(analysis_file, 'w') as f:
            json.dump(analysis, f, indent=2)
        print(f"\nSaved detailed analysis to {analysis_file}")

        # Save sample records
        sample_records = []
        record_count = 0
        
        # Get another batch of sample records
        for record in dynamic_parser.parse_stream(data, schema_type=schema_type, parser=parser):
            sample_records.append(record)
            record_count += 1
            if record_count >= 10:  # Save 10 sample records
                break

        sample_file = output_dir / f"sample_records_{Path(in_network_url).name.split('?')[0]}"
        with open(sample_file, 'w') as f:
            json.dump(sample_records, f, indent=2)
        print(f"Saved sample records to {sample_file}")

    except Exception as e:
        import traceback
        print(f"Error processing URL: {str(e)}")
        print("\nFull traceback:")
        print(traceback.format_exc())

def main():
    """Run test script."""
    parser = argparse.ArgumentParser(description="Test live MRF URLs")
    parser.add_argument("--in-network-url", required=True, help="URL to in_network file")
    parser.add_argument("--auth-token", help="Optional authorization token")
    parser.add_argument("--custom-headers", help="Optional JSON string of custom headers to add")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    
    # Update headers if custom ones provided
    if args.custom_headers:
        try:
            custom_headers = json.loads(args.custom_headers)
            headers.update(custom_headers)
            print(f"Added custom headers: {custom_headers}")
        except json.JSONDecodeError:
            print("Warning: Failed to parse custom headers JSON")
    
    if args.auth_token:
        headers['Authorization'] = f'Bearer {args.auth_token}'
        print("Added authorization token")
    
    # Run test
    test_mrf_url(args.in_network_url)

if __name__ == "__main__":
    main()