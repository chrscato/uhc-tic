#!/usr/bin/env python3
"""Production ETL Pipeline for Healthcare Rates Data with Full Index Processing and S3 Upload."""

import os
import uuid
import gc
import psutil
import time
import itertools
import argparse
import requests
from urllib.parse import urlparse

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Loaded environment variables from .env file")
except ImportError:
    print("python-dotenv not installed, using system environment variables")
except Exception as e:
    print(f"Warning: Could not load .env file: {e}")
import hashlib
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Iterator
from dataclasses import dataclass, asdict, field
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
from tqdm import tqdm
import logging
import structlog
import yaml

from tic_mrf_scraper.fetch.blobs import analyze_index_structure
from tic_mrf_scraper.payers import get_handler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger
from tic_mrf_scraper.schema.detector import SchemaDetector
from tic_mrf_scraper.parsers.factory import ParserFactory
from tic_mrf_scraper.stream.dynamic_parser import DynamicStreamingParser
from tic_mrf_scraper.diagnostics import (
    identify_index,
    detect_compression,
    identify_in_network,
)
from tic_mrf_scraper.utils.dedup_cache import SQLiteDedupCache

# Configure logging levels - suppress all debug output
logging.getLogger('tic_mrf_scraper.stream.parser').setLevel(logging.WARNING)
logging.getLogger('tic_mrf_scraper.fetch.blobs').setLevel(logging.WARNING)
logging.getLogger('tic_mrf_scraper.transform.normalize').setLevel(logging.WARNING)

# Configure structlog
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

# Get logger for main pipeline
logger = get_logger(__name__)

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def log_memory_usage(stage: str):
    """Log memory usage for monitoring."""
    memory_mb = get_memory_usage()
    logger.info("memory_usage", stage=stage, memory_mb=memory_mb)
    return memory_mb

def check_memory_pressure(config: "ETLConfig"):
    """Check if memory usage exceeds configured threshold."""
    memory_mb = get_memory_usage()
    if memory_mb > config.memory_threshold_mb:
        logger.warning(
            "memory_pressure_detected",
            memory_mb=memory_mb,
            threshold_mb=config.memory_threshold_mb,
        )
        return True
    return False

def _peek_file_structure(url: str) -> Optional[Dict[str, Any]]:
    """
    Peek at file structure for schema detection.
    
    Args:
        url: URL to MRF file
        
    Returns:
        Dict containing sample data or None if error
    """
    try:
        # Use existing fetch_url function
        content = fetch_url(url)
        
        # Handle gzipped content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                data = json.load(gz)
        else:
            data = json.loads(content.decode('utf-8'))
            
        return data
    except Exception as e:
        logger.error("failed_to_peek_file_structure", url=url, error=str(e))
        return None

def force_memory_cleanup():
    """Aggressive memory cleanup."""
    gc.collect()
    memory_mb = get_memory_usage()
    logger.info("forced_memory_cleanup", memory_mb_after=memory_mb)
    return memory_mb

def get_file_size(url: str) -> Optional[int]:
    """Get file size in bytes from URL using HEAD request."""
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        if response.status_code == 200:
            return int(response.headers.get('content-length', 0))
        return None
    except Exception as e:
        logger.warning(f"Failed to get file size for {url}: {str(e)}")
        return None

def format_size(size_bytes: int) -> str:
    """Format size in bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"

# Global progress tracking
class ProgressTracker:
    def __init__(self):
        self.current_payer = ""
        self.files_completed = 0
        self.total_files = 0
        self.records_processed = 0
        self.start_time = time.time()
        self.pbar = None
    
    def update_progress(self, payer: str, files_completed: int, total_files: int, 
                       records_processed: int):
        if self.pbar is None:
            self.pbar = tqdm(total=total_files, desc=f"Processing {payer}", 
                           unit="files", leave=True)
        
        self.current_payer = payer
        self.files_completed = files_completed
        self.total_files = total_files
        self.records_processed = records_processed
        
        # Calculate processing rate and ETA
        elapsed_time = time.time() - self.start_time
        if elapsed_time > 0:
            rate = records_processed / elapsed_time
            remaining_files = total_files - files_completed
            eta = remaining_files / (files_completed / elapsed_time) if files_completed > 0 else 0
            
            # Get memory usage
            memory_mb = get_memory_usage()
            
            # Update progress bar description
            self.pbar.set_description(
                f"{payer} | {records_processed:,} records | {rate:.1f} rec/s | {memory_mb:.1f}MB | ETA: {eta:.1f}s"
            )
            self.pbar.update(files_completed - self.pbar.n)
    
    def close(self):
        if self.pbar:
            self.pbar.close()

progress = ProgressTracker()

@dataclass
class ETLConfig:
    """ETL Pipeline Configuration."""
    # Input sources
    payer_endpoints: Dict[str, str]
    cpt_whitelist: List[str]
    
    # Processing configuration
    batch_size: int = 10000
    parallel_workers: int = 2
    max_files_per_payer: Optional[int] = None
    max_records_per_file: Optional[int] = None
    safety_limit_records_per_file: int = 100000  # Hard limit to prevent crashes

    # Size-based processing
    small_file_limit_gb: float = 2.0  # Process files < 2GB
    medium_file_limit_gb: float = 5.0  # Process files < 5GB
    process_size_range: str = "all"  # One of: "small", "medium", "large", "all"
    test_mode: bool = False  # If True, only process first 10 smallest files
    
    # Memory management
    memory_threshold_mb: int = field(
        default_factory=lambda: int(psutil.virtual_memory().total / 1024 / 1024 * 0.8)
    )
    
    # Output configuration
    local_output_dir: str = "ortho_radiology_data_default"
    s3_bucket: Optional[str] = None
    s3_prefix: str = "healthcare-rates-ortho-radiology"
    
    # Data versioning
    schema_version: str = "v2.1.0"
    processing_version: str = "tic-etl-v1.0"
    
    # Quality thresholds
    min_completeness_pct: float = 80.0
    min_accuracy_score: float = 0.85

class UUIDGenerator:
    """Deterministic UUID generation for consistent entity identification."""
    
    @staticmethod
    def generate_uuid(namespace: str, *components: str) -> str:
        """Generate deterministic UUID for deduplication."""
        content = "|".join(str(c) for c in components)
        namespace_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"healthcare.{namespace}")
        return str(uuid.uuid5(namespace_uuid, content))
    
    @staticmethod
    def payer_uuid(payer_name: str, parent_org: str = "") -> str:
        return UUIDGenerator.generate_uuid("payers", payer_name, parent_org)
    
    @staticmethod
    def organization_uuid(tin: str, org_name: str = "") -> str:
        return UUIDGenerator.generate_uuid("organizations", tin, org_name)
    
    @staticmethod
    def provider_uuid(npi: str) -> str:
        return UUIDGenerator.generate_uuid("providers", npi)
    
    @staticmethod
    def rate_uuid(payer_uuid: str, org_uuid: str, service_code: str, 
                  rate: float, effective_date: str) -> str:
        return UUIDGenerator.generate_uuid(
            "rates", payer_uuid, org_uuid, service_code, 
            f"{rate:.2f}", effective_date
        )

class DataQualityValidator:
    """Data quality validation and scoring."""
    
    @staticmethod
    def validate_rate_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and score a rate record."""
        quality_flags = {
            "is_validated": True,
            "has_conflicts": False,
            "confidence_score": 1.0,
            "validation_notes": []
        }
        
        # Required field validation - handle both service_code and service_codes
        required_fields = ["negotiated_rate", "payer_uuid", "organization_uuid"]
        missing_fields = [f for f in required_fields if not record.get(f)]
        
        # Check for service_code or service_codes
        has_service_code = record.get("service_code") or record.get("service_codes")
        if not has_service_code:
            missing_fields.append("service_code")
        
        if missing_fields:
            quality_flags["is_validated"] = False
            quality_flags["confidence_score"] -= 0.3
            quality_flags["validation_notes"].append(f"Missing required fields: {missing_fields}")
        
        # Rate reasonableness check
        rate = record.get("negotiated_rate", 0)
        if rate <= 0 or rate > 10000:  # Reasonable rate bounds
            quality_flags["has_conflicts"] = True
            quality_flags["confidence_score"] -= 0.2
            quality_flags["validation_notes"].append(f"Unusual rate value: ${rate}")
        
        # NPI validation
        npi_list = record.get("provider_network", {}).get("npi_list", [])
        if not npi_list:
            quality_flags["confidence_score"] -= 0.1
            quality_flags["validation_notes"].append("No NPIs associated")
        
        quality_flags["validation_notes"] = "; ".join(quality_flags["validation_notes"])
        return quality_flags

class ProductionETLPipeline:
    """Memory-efficient production ETL pipeline with full index processing and S3 upload."""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.cpt_whitelist_set = set(config.cpt_whitelist)
        self.uuid_gen = UUIDGenerator()
        self.validator = DataQualityValidator()
        self.s3_client = boto3.client('s3') if config.s3_bucket else None
        self.local_parquet_writers: Dict[Path, pq.ParquetWriter] = {}

        # Initialize local temp directory for S3 uploads
        if self.s3_client:
            self.temp_dir = tempfile.mkdtemp(prefix="etl_pipeline_")
            logger.info("created_temp_directory", temp_dir=self.temp_dir)
        else:
            self.setup_local_output_structure()
            self.temp_dir = None
        
        # Processing statistics
        self.stats = {
            "payers_processed": 0,
            "total_files_found": 0,
            "files_processed": 0,
            "files_succeeded": 0,
            "files_failed": 0,
            "records_extracted": 0,
            "records_validated": 0,
            "s3_uploads": 0,
            "processing_start": datetime.now(timezone.utc),
            "errors": []
        }
        
        # Log initial memory usage
        log_memory_usage("pipeline_initialization")
    
    def setup_local_output_structure(self):
        """Create local output directory structure (fallback if no S3)."""
        base_dir = Path(self.config.local_output_dir)
        for subdir in ["payers", "organizations", "providers", "rates", "analytics"]:
            (base_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    def cleanup_temp_directory(self):
        """Clean up temporary directory."""
        if self.temp_dir:
            import shutil
            shutil.rmtree(self.temp_dir, ignore_errors=True)
            logger.info("cleaned_temp_directory", temp_dir=self.temp_dir)

    def close_parquet_writers(self):
        """Close all open parquet writers."""
        for writer in self.local_parquet_writers.values():
            try:
                writer.close()
            except Exception:
                pass
        self.local_parquet_writers.clear()
    
    def process_all_payers(self):
        """Process all configured payers with full index processing."""
        logger.info("starting_production_etl_full_index", 
                   payers=len(self.config.payer_endpoints),
                   s3_enabled=bool(self.s3_client),
                   config=asdict(self.config))
        
        try:
            # Process payers sequentially for better resource management
            for payer_name, index_url in self.config.payer_endpoints.items():
                try:
                    # Log memory before processing each payer
                    log_memory_usage(f"before_payer_{payer_name}")
                    
                    # Check memory pressure before starting payer
                    if check_memory_pressure(self.config):
                        logger.warning("memory_pressure_before_payer", payer=payer_name)
                        force_memory_cleanup()
                    
                    payer_stats = self.process_payer(payer_name, index_url)
                    logger.info("completed_payer", payer=payer_name, stats=payer_stats)
                    self.stats["payers_processed"] += 1
                    
                    # Update overall stats
                    self.stats["total_files_found"] += payer_stats.get("files_found", 0)
                    self.stats["files_processed"] += payer_stats.get("files_processed", 0)
                    self.stats["files_succeeded"] += payer_stats.get("files_succeeded", 0)
                    self.stats["files_failed"] += payer_stats.get("files_failed", 0)
                    self.stats["records_extracted"] += payer_stats.get("records_extracted", 0)
                    self.stats["records_validated"] += payer_stats.get("records_validated", 0)
                    
                    # Force garbage collection after each payer
                    force_memory_cleanup()
                    log_memory_usage(f"after_payer_{payer_name}")
                    
                except Exception as e:
                    error_msg = f"Failed processing {payer_name}: {str(e)}"
                    logger.error("payer_processing_failed", payer=payer_name, error=str(e))
                    self.stats["errors"].append(error_msg)

            # Generate final outputs if not using S3
            if not self.s3_client:
                self.close_parquet_writers()
                self.generate_aggregated_tables()
            
            self.log_final_statistics()
            
        finally:
            # Always close writers and cleanup temp directory
            if not self.s3_client:
                self.close_parquet_writers()
            self.cleanup_temp_directory()
    
    def process_payer(self, payer_name: str, index_url: str) -> Dict[str, Any]:
        """Process a single payer's COMPLETE MRF index with all files."""
        logger.info(f"Starting processing for {payer_name}")
        
        payer_stats = {
            "files_found": 0,
            "files_processed": 0,
            "files_succeeded": 0,
            "files_failed": 0,
            "records_extracted": 0,
            "records_validated": 0,
            "failed_files": [],
            "start_time": time.time()
        }
        
        try:
            # Create payer record
            payer_uuid = self.create_payer_record(payer_name, index_url)

            # Analyze index structure
            index_info = identify_index(index_url)
            payer_stats["index_analysis"] = index_info
            logger.info("index_analysis", payer=payer_name, analysis=index_info)

            # Get MRF files from index using handler
            handler = get_handler(payer_name)

            # First pass: get all files with their sizes
            rate_files_with_size: List[Tuple[Dict, int]] = []
            total_rate_files = 0

            logger.info(f"Scanning files and getting sizes for {payer_name}...")
            for f in handler.list_mrf_files(index_url):
                if f["type"] == "in_network_rates":
                    total_rate_files += 1
                    file_size = get_file_size(f["url"]) or 0
                    rate_files_with_size.append((f, file_size))

                    if total_rate_files % 100 == 0:
                        logger.info(f"Scanned {total_rate_files} files...")

            payer_stats["files_found"] = total_rate_files

            if total_rate_files == 0:
                logger.warning(f"No rate files found for {payer_name}")
                return payer_stats

            # Sort files by size
            rate_files_with_size.sort(key=lambda x: x[1])

            # Filter files based on size range
            small_limit_bytes = int(self.config.small_file_limit_gb * 1024 * 1024 * 1024)
            medium_limit_bytes = int(self.config.medium_file_limit_gb * 1024 * 1024 * 1024)

            # Handle test mode first (10 smallest files)
            if self.config.test_mode:
                original_count = len(rate_files_with_size)
                rate_files_with_size = rate_files_with_size[:10]  # Already sorted by size
                logger.info(f"Test mode: Processing 10 smallest files (from {original_count} total)")
                logger.info(f"Size range of test files: {format_size(rate_files_with_size[0][1])} - {format_size(rate_files_with_size[-1][1])}")
            else:
                # Filter by size range
                if self.config.process_size_range == "small":
                    rate_files_with_size = [(f, s) for f, s in rate_files_with_size if s < small_limit_bytes]
                    size_desc = f"small files < {self.config.small_file_limit_gb}GB"
                elif self.config.process_size_range == "medium":
                    rate_files_with_size = [(f, s) for f, s in rate_files_with_size if small_limit_bytes <= s < medium_limit_bytes]
                    size_desc = f"medium files {self.config.small_file_limit_gb}-{self.config.medium_file_limit_gb}GB"
                elif self.config.process_size_range == "large":
                    rate_files_with_size = [(f, s) for f, s in rate_files_with_size if s >= medium_limit_bytes]
                    size_desc = f"large files >= {self.config.medium_file_limit_gb}GB"
                else:  # "all"
                    size_desc = "all files"
                logger.info(f"Processing {len(rate_files_with_size)} {size_desc}")

            # Log size distribution
            if rate_files_with_size:
                size_stats = {
                    "min_size": format_size(rate_files_with_size[0][1]),
                    "max_size": format_size(rate_files_with_size[-1][1]),
                    "total_files": len(rate_files_with_size),
                    "size_ranges": {
                        f"<{self.config.small_file_limit_gb}GB": len([f for f, s in rate_files_with_size if s < small_limit_bytes]),
                        f"{self.config.small_file_limit_gb}-{self.config.medium_file_limit_gb}GB": len([f for f, s in rate_files_with_size if small_limit_bytes <= s < medium_limit_bytes]),
                        f">{self.config.medium_file_limit_gb}GB": len([f for f, s in rate_files_with_size if s >= medium_limit_bytes])
                    }
                }
                logger.info("file_size_distribution", stats=size_stats)

            if self.config.max_files_per_payer:
                rate_files_with_size = rate_files_with_size[:self.config.max_files_per_payer]

            # Process files
            total_files = len(rate_files_with_size)
            for file_index, (file_info, file_size) in enumerate(rate_files_with_size, 1):
                payer_stats["files_processed"] += 1
                
                # Log file size before processing
                logger.info(f"Processing file {file_index}/{total_files} ({format_size(file_size)})")
                
                try:
                    # Log memory before processing each file
                    log_memory_usage(f"before_file_{file_index}")
                    
                    # Check memory pressure before starting file
                    if check_memory_pressure(self.config):
                        logger.warning("memory_pressure_before_file", 
                                     file_index=file_index, 
                                     file_url=file_info["url"])
                        force_memory_cleanup()
                    
                    file_stats = self.process_mrf_file_enhanced(
                        payer_uuid, payer_name, file_info, handler, file_index, total_rate_files
                    )
                    
                    payer_stats["files_succeeded"] += 1
                    payer_stats["records_extracted"] += file_stats["records_extracted"]
                    payer_stats["records_validated"] += file_stats["records_validated"]
                    
                    # Update progress
                    progress.update_progress(
                        payer=payer_name,
                        files_completed=file_index,
                        total_files=total_rate_files,
                        records_processed=payer_stats["records_extracted"],
                    )
                    
                    # Force garbage collection after each file
                    force_memory_cleanup()
                    log_memory_usage(f"after_file_{file_index}")
                    
                except Exception as e:
                    error_msg = f"Failed processing file {file_info['url']}: {str(e)}"
                    logger.error(error_msg)
                    payer_stats["files_failed"] += 1
                    payer_stats["failed_files"].append({
                        "url": file_info["url"],
                        "error": str(e)
                    })
            
            # Log completion
            elapsed = time.time() - payer_stats["start_time"]
            logger.info(
                f"Completed {payer_name}: {payer_stats['files_succeeded']}/{payer_stats['files_found']} "
                f"files, {payer_stats['records_extracted']:,} records in {elapsed:.1f}s"
            )
            
            return payer_stats
            
        except Exception as e:
            logger.error(f"Failed processing payer {payer_name}: {str(e)}")
            raise
    
    def process_mrf_file_enhanced(self, payer_uuid: str, payer_name: str,
                                file_info: Dict[str, Any], handler, file_index: int, total_files: int) -> Dict[str, Any]:
        """Process a single MRF file with direct S3 upload and enhanced logging."""
        dedup_cache = SQLiteDedupCache()
        file_stats = {
            "records_extracted": 0,
            "records_validated": 0,
            "organizations_created": 0,
            "start_time": time.time(),
            "s3_uploads": 0
        }
        
        # Create S3-friendly filename
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        plan_safe_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in file_info["plan_name"])
        filename_base = f"{payer_name}_{plan_safe_name}_{timestamp}"
        
        # Batch collectors for S3 upload - use much smaller batches
        rate_batch = []
        org_batch = []
        provider_batch = []
        
        # Use much smaller batch size for memory efficiency
        batch_size = min(self.config.batch_size, 500)  # Cap at 500 records per batch
        
        # Safety limit for records per file to prevent memory issues
        max_records_per_file_limit = self.config.safety_limit_records_per_file

        # Gather diagnostics before parsing
        compression = detect_compression(file_info["url"])
        in_net_info = identify_in_network(file_info["url"], sample_size=1)
        file_stats["compression"] = compression
        file_stats["in_network_sample"] = in_net_info
        logger.info(
            "file_diagnostics",
            url=file_info["url"],
            compression=compression,
            in_network_keys=in_net_info.get("sample_keys"),
        )
        
        # Process records with streaming parser
        try:
            # Add timeout protection for file processing (5 minutes per file)
            import threading
            import queue
            
            # Create a queue to communicate between threads
            result_queue = queue.Queue()
            exception_queue = queue.Queue()
            
            def process_file_with_timeout():
                try:
                    record_count = 0
                    for raw_record in stream_parse_enhanced(
                        file_info["url"],
                        payer_name,
                        file_info.get("provider_reference_url"),
                        handler
                    ):
                        result_queue.put(raw_record)
                        record_count += 1
                        if record_count % 1000 == 0:
                            logger.info(f"Processed {record_count} records from {file_info['url']}")
                    result_queue.put(None)  # Signal completion
                except Exception as e:
                    exception_queue.put(e)
            
            # Start processing in a separate thread
            processing_thread = threading.Thread(target=process_file_with_timeout)
            processing_thread.daemon = True
            processing_thread.start()
            
            # Process records with timeout
            timeout_seconds = 300  # 5 minutes per file
            start_time = time.time()
            
            while True:
                try:
                    # Check for timeout
                    if time.time() - start_time > timeout_seconds:
                        logger.error(f"File processing timeout after {timeout_seconds} seconds: {file_info['url']}")
                        break
                    
                    # Try to get next record with short timeout
                    try:
                        raw_record = result_queue.get(timeout=1.0)
                        if raw_record is None:  # Processing completed
                            break
                    except queue.Empty:
                        # Check if thread is still alive
                        if not processing_thread.is_alive():
                            # Check for exceptions
                            try:
                                exception = exception_queue.get_nowait()
                                raise exception
                            except queue.Empty:
                                logger.warning(f"Processing thread died for {file_info['url']}")
                                break
                        continue
                    
                    # Check safety limits
                    if file_stats["records_extracted"] >= self.config.safety_limit_records_per_file:
                        logger.warning("reached_safety_limit", 
                                     url=file_info["url"],
                                     records_processed=file_stats["records_extracted"],
                                     limit=self.config.safety_limit_records_per_file)
                        break
                    
                    if (
                        self.config.max_records_per_file is not None
                        and file_stats["records_extracted"] >= self.config.max_records_per_file
                    ):
                        break

                    file_stats["records_extracted"] += 1
                except Exception as e:
                    logger.error(f"Error processing record: {str(e)}")
                    break
                
                # Check memory pressure every 100 records
                if file_stats["records_extracted"] % 100 == 0:
                    if check_memory_pressure(self.config):
                        # Force cleanup and write current batches immediately
                        if rate_batch:
                            upload_stats = self.write_batches_to_s3(
                                rate_batch, org_batch, provider_batch, 
                                payer_name, filename_base, file_stats["s3_uploads"]
                            )
                            file_stats["s3_uploads"] += upload_stats["files_uploaded"]
                            self.stats["s3_uploads"] += upload_stats["files_uploaded"]
                            
                            # Clear batches to free memory and reset dedup cache
                            rate_batch, org_batch, provider_batch = [], [], []
                            dedup_cache.reset()
                            force_memory_cleanup()
                
                # Normalize and validate
                normalized = normalize_tic_record(
                    raw_record,
                    self.cpt_whitelist_set,
                    payer_name
                )
                    
                # Debug normalization for first few records
                if file_stats["records_extracted"] < 5:
                    logger.info("debug_normalization",
                              record_extracted=file_stats["records_extracted"],
                              raw_record_keys=list(raw_record.keys()),
                              normalized_keys=list(normalized.keys()) if normalized else None,
                              is_normalized=bool(normalized))
                
                if not normalized:
                    continue
                
                # Create structured records
                rate_record = self.create_rate_record(
                    payer_uuid, normalized, file_info, raw_record
                )
                    
                # Debug record creation for first few records
                if file_stats["records_extracted"] < 5:
                    logger.info("debug_record_creation",
                              record_extracted=file_stats["records_extracted"],
                              normalized_keys=list(normalized.keys()),
                              rate_record_keys=list(rate_record.keys()),
                              has_payer_uuid=bool(rate_record.get("payer_uuid")),
                              has_org_uuid=bool(rate_record.get("organization_uuid")),
                              npi_list=rate_record.get("provider_network", {}).get("npi_list", []))
                
                # Validate quality
                quality_flags = self.validator.validate_rate_record(rate_record)
                rate_record["quality_flags"] = quality_flags
                
                # Debug logging for first few records
                if file_stats["records_extracted"] < 5:
                    logger.info("debug_validation", 
                              record_keys=list(rate_record.keys()),
                              has_payer_uuid=bool(rate_record.get("payer_uuid")),
                              has_org_uuid=bool(rate_record.get("organization_uuid")),
                              npi_list=rate_record.get("provider_network", {}).get("npi_list", []),
                              is_validated=quality_flags["is_validated"],
                              validation_notes=quality_flags["validation_notes"])
                
                # Additional debug for validation failures
                if not quality_flags["is_validated"] and file_stats["records_extracted"] < 10:
                    logger.warning("debug_validation_failure",
                                 record_extracted=file_stats["records_extracted"],
                                 validation_notes=quality_flags["validation_notes"],
                                 has_payer_uuid=bool(rate_record.get("payer_uuid")),
                                 has_org_uuid=bool(rate_record.get("organization_uuid")),
                                 npi_count=len(rate_record.get("provider_network", {}).get("npi_list", [])))
                
                if quality_flags["is_validated"]:
                    file_stats["records_validated"] += 1
                    rate_batch.append(rate_record)
                    logger.info("added_to_batch", 
                              batch_size=len(rate_batch),
                              target_size=batch_size,
                              record_type="rate")
                    
                    # Create organization record if new
                    org_uuid = rate_record["organization_uuid"]
                    if org_uuid not in dedup_cache:
                        org_record = self.create_organization_record(normalized, raw_record)
                        org_batch.append(org_record)
                        dedup_cache.add(org_uuid)
                        file_stats["organizations_created"] += 1
                    
                    # Create provider records
                    provider_records = self.create_provider_records(normalized, raw_record)
                    provider_batch.extend(provider_records)
                
                # Write batches when full or when memory pressure detected
                if len(rate_batch) >= batch_size or check_memory_pressure(self.config):
                    logger.info("attempting_batch_upload",
                              rate_batch_size=len(rate_batch),
                              org_batch_size=len(org_batch),
                              provider_batch_size=len(provider_batch),
                              memory_pressure=check_memory_pressure(self.config),
                              s3_client_exists=bool(self.s3_client))
                    upload_stats = self.write_batches_to_s3(
                        rate_batch, org_batch, provider_batch, 
                        payer_name, filename_base, file_stats["s3_uploads"]
                    )
                    file_stats["s3_uploads"] += upload_stats["files_uploaded"]
                    self.stats["s3_uploads"] += upload_stats["files_uploaded"]
                    
                    # Clear batches to free memory and reset dedup cache
                    rate_batch, org_batch, provider_batch = [], [], []
                    dedup_cache.reset()

                    # Force garbage collection after each batch
                    force_memory_cleanup()
        except Exception as e:
            logger.error(f"Failed processing file {file_info['url']}: {str(e)}")
            raise
        finally:
            # Write final batches
            if rate_batch:
                upload_stats = self.write_batches_to_s3(
                    rate_batch, org_batch, provider_batch,
                    payer_name, filename_base, file_stats["s3_uploads"]
                )
                file_stats["s3_uploads"] += upload_stats["files_uploaded"]
                self.stats["s3_uploads"] += upload_stats["files_uploaded"]
                dedup_cache.reset()
            dedup_cache.close()

        file_stats["processing_time"] = time.time() - file_stats["start_time"]
        return file_stats
    
    def write_batches_to_s3(self, rate_batch: List[Dict], org_batch: List[Dict], 
                           provider_batch: List[Dict], payer_name: str, 
                           filename_base: str, batch_number: int) -> Dict[str, Any]:
        """Write batches directly to S3 with organized paths."""
        logger.info("write_batches_to_s3_called",
                   has_s3_client=bool(self.s3_client),
                   rate_batch_size=len(rate_batch),
                   org_batch_size=len(org_batch),
                   provider_batch_size=len(provider_batch))
        upload_stats = {"files_uploaded": 0, "bytes_uploaded": 0}
        
        if not self.s3_client:
            # Fallback to local storage
            return self.write_batches_local(rate_batch, org_batch, provider_batch, payer_name)
        
        # Current date for partitioning
        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        batch_timestamp = datetime.now(timezone.utc).strftime("%H%M%S")
        
        # Upload rates batch
        if rate_batch:
            rates_key = f"{self.config.s3_prefix}/rates/payer={payer_name}/date={current_date}/{filename_base}_rates_batch_{batch_number:04d}_{batch_timestamp}.parquet"
            success = self.upload_batch_to_s3(rate_batch, rates_key, "rates")
            if success:
                upload_stats["files_uploaded"] += 1
        
        # Upload organizations batch
        if org_batch:
            orgs_key = f"{self.config.s3_prefix}/organizations/payer={payer_name}/date={current_date}/{filename_base}_orgs_batch_{batch_number:04d}_{batch_timestamp}.parquet"
            success = self.upload_batch_to_s3(org_batch, orgs_key, "organizations")
            if success:
                upload_stats["files_uploaded"] += 1
        
        # Upload providers batch
        if provider_batch:
            providers_key = f"{self.config.s3_prefix}/providers/payer={payer_name}/date={current_date}/{filename_base}_providers_batch_{batch_number:04d}_{batch_timestamp}.parquet"
            success = self.upload_batch_to_s3(provider_batch, providers_key, "providers")
            if success:
                upload_stats["files_uploaded"] += 1
        
        return upload_stats
    
    def upload_batch_to_s3(self, batch_data: List[Dict], s3_key: str, data_type: str) -> bool:
        """Upload a single batch to S3."""
        if not batch_data:
            return True
        
        try:
            # Create temporary parquet file
            temp_file = Path(self.temp_dir) / f"temp_{data_type}_{int(time.time())}.parquet"
            
            # Convert to DataFrame and write to parquet
            df = pd.DataFrame(batch_data)
            df.to_parquet(temp_file, index=False, compression='snappy')
            
            # Upload to S3
            self.s3_client.upload_file(str(temp_file), self.config.s3_bucket, s3_key)
            
            # Get file size for stats
            file_size = temp_file.stat().st_size
            
            # Clean up temp file
            temp_file.unlink()
            
            logger.info("uploaded_batch_to_s3",
                       s3_key=s3_key,
                       records=len(batch_data),
                       file_size_mb=file_size / 1024 / 1024,
                       data_type=data_type)
            
            return True
            
        except Exception as e:
            logger.error("s3_upload_failed",
                        s3_key=s3_key,
                        data_type=data_type,
                        records=len(batch_data),
                        error=str(e))
            return False
    
    def write_batches_local(self, rate_batch: List[Dict], org_batch: List[Dict], 
                           provider_batch: List[Dict], payer_name: str) -> Dict[str, Any]:
        """Fallback: write batches to local files."""
        upload_stats = {"files_uploaded": 0, "bytes_uploaded": 0}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if rate_batch:
            rates_file = (Path(self.config.local_output_dir) / "rates" / 
                         f"rates_{payer_name}_{timestamp}.parquet")
            self.append_to_parquet(rates_file, rate_batch)
            upload_stats["files_uploaded"] += 1
        
        if org_batch:
            orgs_file = (Path(self.config.local_output_dir) / "organizations" / 
                        f"organizations_{payer_name}_{timestamp}.parquet")
            self.append_to_parquet(orgs_file, org_batch)
            upload_stats["files_uploaded"] += 1
        
        if provider_batch:
            providers_file = (Path(self.config.local_output_dir) / "providers" / 
                            f"providers_{payer_name}_{timestamp}.parquet")
            self.append_to_parquet(providers_file, provider_batch)
            upload_stats["files_uploaded"] += 1
        
        return upload_stats
    
    def create_payer_record(self, payer_name: str, index_url: str) -> str:
        """Create and store payer master record."""
        payer_uuid = self.uuid_gen.payer_uuid(payer_name)
        
        payer_record = {
            "payer_uuid": payer_uuid,
            "payer_name": payer_name,
            "payer_type": "Commercial",
            "parent_organization": "",
            "state_licenses": [],
            "market_type": "Unknown",
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "data_source": "TiC_MRF",
            "index_url": index_url,
            "last_scraped": datetime.now(timezone.utc)
        }
        
        return payer_uuid
    
    def create_rate_record(self, payer_uuid: str, normalized: Dict[str, Any], 
                          file_info: Dict[str, Any], raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Create a structured rate record."""
        
        # Generate organization UUID
        org_uuid = self.uuid_gen.organization_uuid(
            normalized.get("provider_tin", ""), 
            normalized.get("provider_name", "")
        )
        
        # Generate rate UUID - handle both service_code and service_codes
        service_code = normalized.get("service_code", "")
        if not service_code and normalized.get("service_codes"):
            # Use first service code if service_code is not available
            service_codes = normalized["service_codes"]
            service_code = service_codes[0] if service_codes else ""
        
        rate_uuid = self.uuid_gen.rate_uuid(
            payer_uuid,
            org_uuid,
            service_code,
            normalized["negotiated_rate"],
            normalized.get("expiration_date", "")
        )
        
        # Extract NPI list
        npi_list = normalized.get("provider_npi", [])
        if npi_list is None:
            npi_list = []
        elif isinstance(npi_list, (int, str)):
            npi_list = [str(npi_list)]
        elif isinstance(npi_list, list):
            npi_list = [str(npi) for npi in npi_list]
        else:
            npi_list = []
        
        return {
            "rate_uuid": rate_uuid,
            "payer_uuid": payer_uuid,
            "organization_uuid": org_uuid,
            "service_code": service_code,
            "service_description": normalized.get("description", ""),
            "billing_code_type": normalized.get("billing_code_type", ""),
            "negotiated_rate": float(normalized["negotiated_rate"]),
            "billing_class": normalized.get("billing_class", ""),
            "rate_type": normalized.get("negotiated_type", "negotiated"),
            "service_codes": normalized.get("service_codes", []),
            "plan_details": {
                "plan_name": file_info.get("plan_name", ""),
                "plan_id": file_info.get("plan_id", ""),
                "plan_type": file_info.get("plan_market_type", ""),
                "market_type": "Commercial"
            },
            "contract_period": {
                "effective_date": None,
                "expiration_date": normalized.get("expiration_date"),
                "last_updated_on": None
            },
            "provider_network": {
                "npi_list": npi_list,
                "npi_count": len(npi_list),
                "coverage_type": "Organization"
            },
            "geographic_scope": {
                "states": [],
                "zip_codes": [],
                "counties": []
            },
            "data_lineage": {
                "source_file_url": file_info["url"],
                "source_file_hash": hashlib.md5(file_info["url"].encode()).hexdigest(),
                "extraction_timestamp": datetime.now(timezone.utc),
                "processing_version": self.config.processing_version
            },
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
    
    def create_organization_record(self, normalized: Dict[str, Any], raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Create organization record."""
        tin = normalized.get("provider_tin", "")
        org_name = normalized.get("provider_name", "")
        
        org_uuid = self.uuid_gen.organization_uuid(tin, org_name)
        
        # Get NPI list safely
        npi_list = normalized.get("provider_npi", [])
        if npi_list is None:
            npi_list = []
        
        return {
            "organization_uuid": org_uuid,
            "tin": tin,
            "organization_name": org_name or f"Organization-{tin}",
            "organization_type": "Unknown",
            "parent_system": "",
            "npi_count": len(npi_list),
            "primary_specialty": "",
            "is_facility": normalized.get("billing_class") == "facility",
            "headquarters_address": {
                "street": "",
                "city": "",
                "state": "",
                "zip": "",
                "lat": None,
                "lng": None
            },
            "service_areas": [],
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "data_quality_score": 0.8
        }
    
    def create_provider_records(self, normalized: Dict[str, Any], raw_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create provider records for NPIs."""
        npi_list = normalized.get("provider_npi", [])
        if npi_list is None or not npi_list:
            return []
        
        if isinstance(npi_list, (int, str)):
            npi_list = [str(npi_list)]
        
        org_uuid = self.uuid_gen.organization_uuid(
            normalized.get("provider_tin", ""), 
            normalized.get("provider_name", "")
        )
        
        provider_records = []
        for npi in npi_list:
            npi_str = str(npi)
            provider_uuid = self.uuid_gen.provider_uuid(npi_str)
            
            provider_record = {
                "provider_uuid": provider_uuid,
                "npi": npi_str,
                "organization_uuid": org_uuid,
                "provider_name": {
                    "first": "",
                    "last": "",
                    "middle": "",
                    "suffix": ""
                },
                "credentials": [],
                "primary_specialty": "",
                "secondary_specialties": [],
                "provider_type": "Individual",
                "gender": "Unknown",
                "addresses": [],
                "is_active": True,
                "enumeration_date": None,
                "last_updated": None,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            provider_records.append(provider_record)
        
        return provider_records
    
    def append_to_parquet(self, file_path: Path, records: List[Dict]):
        """Append records to parquet file (local fallback)."""
        if not records:
            return
        table = pa.Table.from_pylist(records)
        writer = self.local_parquet_writers.get(file_path)
        if writer is None:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(file_path, table.schema)
            self.local_parquet_writers[file_path] = writer
        writer.write_table(table)
        logger.info("wrote_local_batch", file=str(file_path), records=len(records))
    
    def generate_aggregated_tables(self):
        """Generate final aggregated parquet tables (for local storage only)."""
        if self.s3_client:
            logger.info("skipping_local_aggregation", reason="using_s3_storage")
            return
        
        logger.info("generating_aggregated_tables")
        
        # Combine all rate files
        self.combine_table_files("rates")
        self.combine_table_files("organizations") 
        self.combine_table_files("providers")
        
        # Generate analytics table
        self.generate_analytics_table()
    
    def combine_table_files(self, table_name: str):
        """Combine all staging files for a table into final parquet without loading all data."""
        staging_dir = Path(self.config.local_output_dir) / table_name

        # Exclude any previously generated final file
        staging_files = [
            f for f in staging_dir.glob("*.parquet")
            if not f.name.endswith("_final.parquet")
        ]

        if not staging_files:
            logger.warning("no_staging_files", table=table_name)
            return

        logger.info("combining_table_files", table=table_name, files=len(staging_files))

        final_file = staging_dir / f"{table_name}_final.parquet"
        if final_file.exists():
            final_file.unlink()

        uuid_col = f"{table_name.rstrip('s')}_uuid"
        seen_uuids = set()
        writer = None
        records_written = 0
        uuid_col_present = False

        for file_path in staging_files:
            parquet_file = pq.ParquetFile(file_path)

            for batch in parquet_file.iter_batches():
                df = batch.to_pandas()

                if uuid_col in df.columns:
                    uuid_col_present = True
                    mask = ~df[uuid_col].isin(seen_uuids)
                    new_uuids = df.loc[mask, uuid_col]
                    seen_uuids.update(new_uuids.tolist())
                    df = df[mask]

                if df.empty:
                    continue

                table = pa.Table.from_pandas(df, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(final_file, table.schema)
                writer.write_table(table)
                records_written += len(df)

        if writer:
            writer.close()
            logger.info(
                "created_final_table",
                table=table_name,
                records=len(seen_uuids) if uuid_col_present else records_written,
                file=str(final_file),
            )

        # Clean up staging files
        for file_path in staging_files:
            file_path.unlink()
    
    def generate_analytics_table(self):
        """Generate pre-computed analytics table."""
        logger.info("generating_analytics_table")
        
        rates_file = (Path(self.config.local_output_dir) / "rates" / "rates_final.parquet")
        if not rates_file.exists():
            logger.warning("no_rates_data_for_analytics")
            return
        
        df = pd.read_parquet(rates_file)
        
        # National-level analytics by service code
        analytics_records = []
        
        for service_code in df['service_code'].unique():
            code_data = df[df['service_code'] == service_code]
            rates = code_data['negotiated_rate']
            
            analytics_record = {
                "analytics_uuid": self.uuid_gen.generate_uuid("analytics", "national", service_code),
                "service_code": service_code,
                "geographic_scope": {
                    "level": "National",
                    "identifier": "US",
                    "name": "United States"
                },
                "market_statistics": {
                    "provider_count": code_data['organization_uuid'].nunique(),
                    "payer_count": code_data['payer_uuid'].nunique(),
                    "rate_observations": len(code_data),
                    "median_rate": float(rates.median()),
                    "mean_rate": float(rates.mean()),
                    "std_dev": float(rates.std()),
                    "percentiles": {
                        "p10": float(rates.quantile(0.10)),
                        "p25": float(rates.quantile(0.25)),
                        "p75": float(rates.quantile(0.75)),
                        "p90": float(rates.quantile(0.90)),
                        "p95": float(rates.quantile(0.95))
                    }
                },
                "payer_analysis": [],
                "trend_analysis": {
                    "rate_change_6m": 0.0,
                    "rate_change_12m": 0.0,
                    "volatility_score": float(rates.std() / rates.mean() if rates.mean() > 0 else 0)
                },
                "computation_date": datetime.now(timezone.utc),
                "data_freshness": datetime.now(timezone.utc)
            }
            
            analytics_records.append(analytics_record)
        
        # Write analytics table
        if analytics_records:
            analytics_file = (Path(self.config.local_output_dir) / "analytics" / "analytics_final.parquet")
            df_analytics = pd.DataFrame(analytics_records)
            df_analytics.to_parquet(analytics_file, index=False)
            
            logger.info("created_analytics_table", 
                       records=len(analytics_records),
                       file=str(analytics_file))
    
    def log_final_statistics(self):
        """Log final processing statistics."""
        processing_time = datetime.now(timezone.utc) - self.stats["processing_start"]
        
        final_stats = {
            **self.stats,
            "processing_time_seconds": processing_time.total_seconds(),
            "processing_rate_per_second": self.stats["records_validated"] / processing_time.total_seconds() if processing_time.total_seconds() > 0 else 0,
            "completion_time": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info("etl_pipeline_completed", final_stats=final_stats)
        
        # Save statistics to file
        if self.s3_client:
            # Save stats to S3
            stats_key = f"{self.config.s3_prefix}/processing_statistics/{datetime.now().strftime('%Y-%m-%d')}/processing_statistics_{int(time.time())}.json"
            try:
                temp_stats_file = Path(self.temp_dir) / "processing_statistics.json"
                with open(temp_stats_file, 'w') as f:
                    json.dump(final_stats, f, indent=2, default=str)
                
                self.s3_client.upload_file(str(temp_stats_file), self.config.s3_bucket, stats_key)
                logger.info("uploaded_stats_to_s3", s3_key=stats_key)
                
                temp_stats_file.unlink()
            except Exception as e:
                logger.error("failed_to_upload_stats", error=str(e))
        else:
            # Save stats locally
            stats_file = Path(self.config.local_output_dir) / "processing_statistics.json"
            with open(stats_file, 'w') as f:
                json.dump(final_stats, f, indent=2, default=str)


def create_production_config() -> ETLConfig:
    """Create production ETL configuration from YAML file."""
    # Read YAML configuration
    with open('production_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Get active payers (non-commented ones)
    active_payers = [payer for payer in config['endpoints'].keys() 
                    if not payer.startswith('#')]
    
    # Generate output directory name based on active payers
    if active_payers:
        # Use all active payers for directory name
        payer_names = "_".join(active_payers)
        output_dir = f"ortho_radiology_data_{payer_names}"
    else:
        # Fallback if no active payers
        output_dir = "ortho_radiology_data_default"
    
    # Substitute environment variables in S3 configuration
    s3_prefix = config['output']['s3']['prefix']
    if s3_prefix.startswith('${') and s3_prefix.endswith('}'):
        # Extract environment variable name
        env_var = s3_prefix[2:-1]  # Remove ${ and }
        s3_prefix = os.getenv(env_var, s3_prefix)
        logger.info("substituted_environment_variable", 
                   original=s3_prefix, 
                   env_var=env_var, 
                   value=s3_prefix)
    
    # Ensure s3_prefix doesn't have trailing slash to avoid double slashes
    s3_prefix = s3_prefix.rstrip('/')
    
    s3_bucket = config['output']['s3']['bucket']
    if s3_bucket.startswith('${') and s3_bucket.endswith('}'):
        env_var = s3_bucket[2:-1]
        s3_bucket = os.getenv(env_var, s3_bucket)
    
    s3_region = config['output']['s3']['region']
    if s3_region.startswith('${') and s3_region.endswith('}'):
        env_var = s3_region[2:-1]
        s3_region = os.getenv(env_var, s3_region)
    
    return ETLConfig(
        payer_endpoints=config['endpoints'],
        cpt_whitelist=config['cpt_whitelist'],
        batch_size=config['processing']['batch_size'],
        parallel_workers=config['processing']['parallel_workers'],
        max_files_per_payer=config['processing'].get('max_files_per_payer'),
        max_records_per_file=config['processing'].get('max_records_per_file'),
        safety_limit_records_per_file=config['processing'].get('safety_limit_records_per_file', 100000),
        local_output_dir=output_dir,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        schema_version=config['versioning']['schema_version'],
        processing_version=config['versioning']['processing_version'],
        min_completeness_pct=config['processing']['min_completeness_pct'],
        min_accuracy_score=config['processing']['min_accuracy_score']
    )


def main():
    """Main entry point for production ETL pipeline."""
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description="Production ETL Pipeline for Healthcare Rates Data")
        parser.add_argument("--small-run", action="store_true", help=f"Process only files smaller than 2GB")
        parser.add_argument("--med-run", action="store_true", help=f"Process only files between 2GB and 5GB")
        parser.add_argument("--large-run", action="store_true", help=f"Process only files larger than 5GB")
        parser.add_argument("--test", action="store_true", help="Process only the 10 smallest files")
        args = parser.parse_args()

        # Load configuration
        config = create_production_config()
        
        # Set test mode and size range based on command line arguments
        config.test_mode = args.test
        
        if not config.test_mode:  # Size range only matters if not in test mode
            if args.small_run:
                config.process_size_range = "small"
            elif args.med_run:
                config.process_size_range = "medium"
            elif args.large_run:
                config.process_size_range = "large"
            else:
                config.process_size_range = "all"
        
        # Initialize pipeline
        pipeline = ProductionETLPipeline(config)
        
        # Process all payers
        pipeline.process_all_payers()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        # Clean up progress tracker
        progress.close()


if __name__ == "__main__":
    main()