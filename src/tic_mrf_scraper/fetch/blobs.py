"""Enhanced module for fetching MRF blob URLs with Table of Contents support."""

import json
import requests
import gzip
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Iterator
from tenacity import retry, stop_after_attempt, wait_exponential
from ..utils.backoff_logger import get_logger
from ..utils.http_headers import get_cloudfront_headers

logger = get_logger(__name__)



def is_local_file(path: str) -> bool:
    """Check if the path is a local file."""
    return os.path.exists(path) or path.startswith(('file://', 'C:', 'D:', '/', '\\'))

def load_local_file(file_path: str) -> Dict[str, Any]:
    """Load JSON from a local file, handling gzip compression."""
    logger.info("loading_local_file", path=file_path)
    
    # Remove file:// prefix if present
    if file_path.startswith('file://'):
        file_path = file_path[7:]
    
    # Handle gzip compression
    if file_path.endswith('.gz'):
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            data = json.load(f)
    else:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    
    logger.info("local_file_loaded", path=file_path, keys=list(data.keys()) if isinstance(data, dict) else "array")
    return data

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def list_mrf_blobs_enhanced(index_url: str) -> Iterator[Dict[str, Any]]:
    """Yield MRF blob URLs with metadata from an index file.

    Args:
        index_url: URL or local file path to the index file

    Yields:
        Dictionaries containing MRF blob information with metadata
    """
    logger.info("fetching_enhanced_index", url=index_url)

    # Handle local files
    if is_local_file(index_url):
        data = load_local_file(index_url)
    else:
        # Handle HTTP URLs with CloudFront-compatible headers
        headers = get_cloudfront_headers()
        resp = requests.get(index_url, headers=headers, timeout=300)
        resp.raise_for_status()
        data = resp.json()

    logger.info("index_response_keys", keys=list(data.keys()) if isinstance(data, dict) else "array")

    if not isinstance(data, dict):
        raise ValueError(f"Expected dict response, got {type(data)}")

    count = 0

    # Handle standard Table of Contents structure
    if "reporting_structure" in data:
        logger.info("processing_table_of_contents")

        for i, structure in enumerate(data["reporting_structure"]):
            logger.info(
                "processing_reporting_structure",
                index=i,
                keys=list(structure.keys()),
            )

            # Extract plan information
            plan_name = structure.get("plan_name", f"plan_{i}")
            plan_id = structure.get("plan_id")
            plan_market_type = structure.get("plan_market_type")

            # Process in-network files
            if "in_network_files" in structure:
                for j, file_info in enumerate(structure["in_network_files"]):
                    if "location" in file_info:
                        mrf_info = {
                            "url": file_info["location"],
                            "type": "in_network_rates",
                            "plan_name": plan_name,
                            "plan_id": plan_id,
                            "plan_market_type": plan_market_type,
                            "description": file_info.get("description", ""),
                            "reporting_structure_index": i,
                            "file_index": j,
                        }

                        # Check for provider reference file
                        if "provider_references" in structure:
                            for provider_ref in structure["provider_references"]:
                                if "location" in provider_ref:
                                    mrf_info["provider_reference_url"] = provider_ref["location"]
                                    break

                        count += 1
                        yield mrf_info

            # Process allowed amount files
            if "allowed_amount_file" in structure:
                allowed_file = structure["allowed_amount_file"]
                if "location" in allowed_file:
                    mrf_info = {
                        "url": allowed_file["location"],
                        "type": "allowed_amounts",
                        "plan_name": plan_name,
                        "plan_id": plan_id,
                        "plan_market_type": plan_market_type,
                        "description": allowed_file.get("description", ""),
                        "reporting_structure_index": i,
                        "file_index": 0,
                    }
                    count += 1
                    yield mrf_info

    # Handle legacy blobs structure
    elif "blobs" in data:
        logger.info("processing_legacy_blobs")
        for i, blob in enumerate(data["blobs"]):
            if "url" in blob:
                mrf_info = {
                    "url": blob["url"],
                    "type": "unknown",
                    "plan_name": blob.get("name", f"blob_{i}"),
                    "plan_id": None,
                    "plan_market_type": None,
                    "description": blob.get("description", ""),
                    "reporting_structure_index": 0,
                    "file_index": i,
                }
                count += 1
                yield mrf_info

    else:
        available_keys = list(data.keys())
        logger.error("unknown_index_structure", keys=available_keys)
        raise ValueError(
            f"Response missing expected keys. Available keys: {available_keys}"
        )

    logger.info("found_mrf_files", count=count)

def list_mrf_blobs(index_url: str) -> List[str]:
    """Legacy function for backward compatibility - returns just URLs."""
    enhanced_results = list_mrf_blobs_enhanced(index_url)
    return [mrf["url"] for mrf in enhanced_results]

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def fetch_url(url: str) -> bytes:
    """Fetch data from URL or local file with retry logic.
    
    Args:
        url: URL or local file path to fetch
        
    Returns:
        Response content as bytes
    """
    logger.info("fetching_url", url=url)
    
    # Handle local files
    if is_local_file(url):
        # Remove file:// prefix if present
        if url.startswith('file://'):
            file_path = url[7:]
        else:
            file_path = url
        
        # Handle gzip compression
        if file_path.endswith('.gz'):
            with gzip.open(file_path, 'rb') as f:
                return f.read()
        else:
            with open(file_path, 'rb') as f:
                return f.read()
    else:
        # Handle HTTP URLs with CloudFront-compatible headers
        headers = get_cloudfront_headers()
        resp = requests.get(url, stream=True, headers=headers, timeout=300)  # Stream for large files
        resp.raise_for_status()
        return resp.content

def analyze_index_structure(index_url: str) -> Dict[str, Any]:
    """Analyze the structure of an index file for debugging.
    
    Args:
        index_url: URL or local file path to the index file
        
    Returns:
        Analysis of the index structure
    """
    logger.info("analyzing_index_structure", url=index_url)
    
    try:
        # Handle local files
        if is_local_file(index_url):
            data = load_local_file(index_url)
        else:
            # Handle HTTP URLs with CloudFront-compatible headers
            headers = get_cloudfront_headers(url)
            resp = requests.get(index_url, headers=headers, timeout=300)
            resp.raise_for_status()
            data = resp.json()
        
        analysis = {
            "url": index_url,
            "status": "success",
            "root_type": type(data).__name__,
            "top_level_keys": list(data.keys()) if isinstance(data, dict) else [],
            "estimated_mrf_count": 0,
            "structure_type": "unknown",
            "plans_identified": [],
            "sample_urls": []
        }
        
        if isinstance(data, dict):
            if "reporting_structure" in data:
                analysis["structure_type"] = "table_of_contents"
                analysis["estimated_mrf_count"] = len(data["reporting_structure"])
                
                # Sample first few plans
                for structure in data["reporting_structure"][:3]:
                    plan_info = {
                        "plan_name": structure.get("plan_name"),
                        "plan_id": structure.get("plan_id"),
                        "in_network_files": len(structure.get("in_network_files", [])),
                        "has_allowed_amounts": "allowed_amount_file" in structure,
                        "has_provider_references": "provider_references" in structure
                    }
                    analysis["plans_identified"].append(plan_info)
                    
                    # Collect sample URLs
                    if "in_network_files" in structure:
                        for file_info in structure["in_network_files"][:2]:
                            if "location" in file_info:
                                analysis["sample_urls"].append(file_info["location"])
            
            elif "blobs" in data:
                analysis["structure_type"] = "legacy_blobs"
                analysis["estimated_mrf_count"] = len(data["blobs"])
                analysis["sample_urls"] = [blob.get("url") for blob in data["blobs"][:3] if blob.get("url")]
        
        return analysis
        
    except Exception as e:
        return {
            "url": index_url,
            "status": "error",
            "error": str(e)
        }
