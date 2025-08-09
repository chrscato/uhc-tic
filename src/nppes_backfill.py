#!/usr/bin/env python3
"""Simplified NPPES Provider Information Backfill Script."""

import os
import json
import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import logging
from tqdm import tqdm
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class NPPESConfig:
    """Configuration for NPPES provider data management."""
    # API Configuration
    npi_api_base_url: str = "https://npiregistry.cms.hhs.gov/api/"
    api_version: str = "2.1"
    request_delay: float = 0.1  # Delay between requests to be respectful
    
    # Processing Configuration
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # File Configuration
    input_providers_file: str = "output/providers_20250806_181227.parquet"
    nppes_output_file: str = "nppes_data/nppes_providers.parquet"
    
    # Quality Control
    min_success_rate: float = 0.95
    log_failed_npis: bool = True
    
    # Testing/Sampling Configuration
    limit: Optional[int] = None  # Limit number of NPIs to process for testing

class NPIAPIClient:
    """Client for interacting with the NPI Registry API."""
    
    def __init__(self, config: NPPESConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TiC-NPPES-Manager/1.0'
        })
    
    def get_provider_info(self, npi: str) -> Optional[Dict[str, Any]]:
        """Fetch provider information from NPI Registry API."""
        url = f"{self.config.npi_api_base_url}"
        params = {
            'number': npi,
            'version': self.config.api_version,
            'pretty': 'true'
        }
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('result_count', 0) > 0 and data.get('results'):
                    return data['results'][0]
                else:
                    logger.warning(f"No results found for NPI: {npi}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"API request failed for NPI {npi} (attempt {attempt + 1}): {str(e)}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Failed to fetch NPI {npi} after {self.config.max_retries} attempts")
                    return None
        
        return None
    
    def batch_get_provider_info(self, npis: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """Fetch provider information for multiple NPIs with rate limiting."""
        results = {}
        
        for npi in tqdm(npis, desc="Fetching provider data"):
            provider_info = self.get_provider_info(npi)
            results[npi] = provider_info
            
            # Rate limiting
            time.sleep(self.config.request_delay)
        
        return results

class NPPESBackfill:
    """Simplified NPPES backfill processor."""
    
    def __init__(self, config: NPPESConfig):
        self.config = config
        self.api_client = NPIAPIClient(config)
        
        # Ensure output directory exists
        Path(self.config.nppes_output_file).parent.mkdir(parents=True, exist_ok=True)
    
    def extract_npis_from_file(self) -> List[str]:
        """Extract unique NPIs from the input providers file."""
        input_file = Path(self.config.input_providers_file)
        
        if not input_file.exists():
            raise ValueError(f"Input providers file not found: {input_file}")
        
        logger.info(f"Reading providers from: {input_file}")
        
        try:
            df = pd.read_parquet(input_file)
            logger.info(f"Loaded providers file with {len(df)} records")
            
            # Check for NPI column (could be 'npi', 'provider_npi', etc.)
            npi_columns = [col for col in df.columns if 'npi' in col.lower()]
            if npi_columns:
                npi_col = npi_columns[0]
                npis = df[npi_col].dropna().astype(str).tolist()
                unique_npis = list(set(npis))  # Remove duplicates
                logger.info(f"Found {len(unique_npis)} unique NPIs in {input_file.name}")
                logger.debug(f"Available columns: {list(df.columns)}")
                return unique_npis
            else:
                logger.warning(f"No NPI column found in {input_file.name}. Available columns: {list(df.columns)}")
                return []
                
        except Exception as e:
            logger.error(f"Error processing {input_file}: {str(e)}")
            raise
    
    def load_existing_nppes_data(self) -> pd.DataFrame:
        """Load existing NPPES data if available."""
        nppes_file = Path(self.config.nppes_output_file)
        if nppes_file.exists():
            logger.info(f"Loading existing NPPES data from {nppes_file}")
            return pd.read_parquet(nppes_file)
        else:
            logger.info("No existing NPPES data found. Starting fresh.")
            return pd.DataFrame()
    
    def get_new_npis(self, existing_nppes_df: pd.DataFrame, all_npis: List[str]) -> List[str]:
        """Get NPIs that are not already in the NPPES dataset."""
        if existing_nppes_df.empty:
            new_npis = all_npis
        else:
            existing_npis = set(existing_nppes_df['npi'].astype(str))
            new_npis = [npi for npi in all_npis if npi not in existing_npis]
        
        logger.info(f"Found {len(new_npis)} new NPIs out of {len(all_npis)} total")
        
        # Apply limit if specified (for testing)
        if self.config.limit and len(new_npis) > self.config.limit:
            logger.info(f"Limiting to {self.config.limit} NPIs for testing")
            new_npis = new_npis[:self.config.limit]
        
        return new_npis
    
    def fetch_and_process_nppes_data(self, npis: List[str]) -> pd.DataFrame:
        """Fetch and process NPPES data for given NPIs."""
        logger.info(f"Fetching NPPES data for {len(npis)} NPIs...")
        
        # Fetch provider information from API
        npi_data = self.api_client.batch_get_provider_info(npis)
        
        # Process the data
        processed_records = []
        failed_npis = []
        
        for npi, api_data in npi_data.items():
            if api_data:
                processed_record = self._process_nppes_record(npi, api_data)
                processed_records.append(processed_record)
            else:
                failed_npis.append(npi)
        
        # Log failed NPIs if requested
        if self.config.log_failed_npis and failed_npis:
            failed_file = Path(self.config.nppes_output_file).parent / "failed_npis.json"
            with open(failed_file, 'w') as f:
                json.dump(failed_npis, f, indent=2)
            logger.warning(f"Failed to fetch {len(failed_npis)} NPIs. See {failed_file}")
        
        # Calculate success rate
        success_rate = len(processed_records) / len(npis)
        logger.info(f"NPPES fetch success rate: {success_rate:.2%}")
        
        if success_rate < self.config.min_success_rate:
            logger.warning(f"Success rate {success_rate:.2%} below threshold {self.config.min_success_rate:.2%}")
        
        return pd.DataFrame(processed_records)
    
    def _process_nppes_record(self, npi: str, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single NPPES record from API data."""
        record = {'npi': npi}
        
        # Extract basic information
        basic = api_data.get('basic', {})
        record['provider_name'] = {
            'first': basic.get('first_name', ''),
            'last': basic.get('last_name', ''),
            'middle': basic.get('middle_name', ''),
            'suffix': ''
        }
        
        # Extract credentials
        credentials = []
        if basic.get('credential'):
            credentials.append(basic['credential'])
        record['credentials'] = credentials
        
        # Extract gender
        record['gender'] = basic.get('sex', 'Unknown')
        
        # Extract enumeration date
        if basic.get('enumeration_date'):
            try:
                record['enumeration_date'] = datetime.strptime(
                    basic['enumeration_date'], '%Y-%m-%d'
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                record['enumeration_date'] = None
        
        # Extract last updated
        if basic.get('last_updated'):
            try:
                record['last_updated'] = datetime.strptime(
                    basic['last_updated'], '%Y-%m-%d'
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                record['last_updated'] = None
        
        # Extract addresses
        addresses = []
        for addr in api_data.get('addresses', []):
            if addr.get('address_purpose') == 'LOCATION':  # Prefer location addresses
                address_record = {
                    'type': addr.get('address_type', ''),
                    'purpose': addr.get('address_purpose', ''),
                    'street': addr.get('address_1', ''),
                    'city': addr.get('city', ''),
                    'state': addr.get('state', ''),
                    'zip': addr.get('postal_code', ''),
                    'country': addr.get('country_name', ''),
                    'phone': addr.get('telephone_number', ''),
                    'fax': addr.get('fax_number', '')
                }
                addresses.append(address_record)
        
        record['addresses'] = addresses
        
        # Extract specialties from taxonomies
        primary_specialty = ""
        secondary_specialties = []
        
        for taxonomy in api_data.get('taxonomies', []):
            specialty_desc = taxonomy.get('desc', '')
            if taxonomy.get('primary'):
                primary_specialty = specialty_desc
            else:
                secondary_specialties.append(specialty_desc)
        
        record['primary_specialty'] = primary_specialty
        record['secondary_specialties'] = secondary_specialties
        
        # Determine provider type
        if api_data.get('enumeration_type') == 'NPI-1':
            record['provider_type'] = 'Individual'
        elif api_data.get('enumeration_type') == 'NPI-2':
            record['provider_type'] = 'Organization'
        else:
            record['provider_type'] = 'Unknown'
        
        # Add metadata
        record['metadata'] = {
            'fetched_at': datetime.now(timezone.utc),
            'api_version': self.config.api_version,
            'data_source': 'NPI_Registry_API',
            'fetch_status': 'success'
        }
        
        return record
    
    def update_nppes_dataset(self, new_data: pd.DataFrame):
        """Update the NPPES dataset with new data."""
        logger.info(f"Updating NPPES dataset with {len(new_data)} new records...")
        
        # Load existing data
        existing_data = self.load_existing_nppes_data()
        
        if existing_data.empty:
            # First time creation
            combined_data = new_data
        else:
            # Combine existing and new data, removing duplicates
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            combined_data = combined_data.drop_duplicates(subset=['npi'], keep='last')
        
        # Save the updated dataset
        combined_data.to_parquet(self.config.nppes_output_file, index=False, compression='snappy')
        
        logger.info(f"NPPES dataset updated: {len(combined_data)} total records")
        logger.info(f"NPPES file saved to: {self.config.nppes_output_file}")
    
    def generate_summary_stats(self):
        """Generate summary statistics for the NPPES dataset."""
        nppes_file = Path(self.config.nppes_output_file)
        if not nppes_file.exists():
            logger.warning("No NPPES dataset found to generate statistics")
            return
        
        df = pd.read_parquet(nppes_file)
        
        stats = {
            'total_providers': len(df),
            'individual_providers': len(df[df['provider_type'] == 'Individual']),
            'organization_providers': len(df[df['provider_type'] == 'Organization']),
            'providers_with_addresses': df['addresses'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum(),
            'providers_with_specialties': df['primary_specialty'].apply(lambda x: isinstance(x, str) and bool(x.strip())).sum(),
            'providers_with_credentials': df['credentials'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum(),
            'successfully_fetched': df['metadata'].apply(lambda x: isinstance(x, dict) and x.get('fetch_status') == 'success').sum(),
            'unique_states': len(set([addr.get('state') for addresses in df['addresses'] if isinstance(addresses, list) for addr in addresses if isinstance(addr, dict) and addr.get('state')])),
            'unique_primary_specialties': df['primary_specialty'].apply(lambda x: x if isinstance(x, str) and x.strip() else None).dropna().nunique(),
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
        
        # Save statistics
        stats_file = Path(self.config.nppes_output_file).parent / "nppes_statistics.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        
        logger.info(f"NPPES statistics saved to: {stats_file}")
        logger.info(f"Summary: {stats['total_providers']} total providers in NPPES dataset")
    
    def run_backfill(self):
        """Run the complete NPPES backfill process."""
        logger.info("Starting NPPES backfill process...")
        
        start_time = time.time()
        
        try:
            # Extract NPIs from input file
            all_npis = self.extract_npis_from_file()
            
            if not all_npis:
                logger.warning("No NPIs found in input file")
                return
            
            # Load existing NPPES data
            existing_nppes_df = self.load_existing_nppes_data()
            
            # Get new NPIs that need to be fetched
            new_npis = self.get_new_npis(existing_nppes_df, all_npis)
            
            if not new_npis:
                logger.info("No new NPIs to fetch. NPPES dataset is up to date.")
                return
            
            # Fetch and process new NPPES data
            new_nppes_data = self.fetch_and_process_nppes_data(new_npis)
            
            # Update the NPPES dataset
            self.update_nppes_dataset(new_nppes_data)
            
            # Generate summary statistics
            self.generate_summary_stats()
            
            elapsed_time = time.time() - start_time
            logger.info(f"NPPES backfill completed in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"NPPES backfill failed: {str(e)}")
            raise

def main():
    """Main entry point for NPPES backfill script."""
    parser = argparse.ArgumentParser(
        description="Simplified NPPES Provider Information Backfill Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with default settings
  python src/nppes_backfill.py
  
  # Run with custom input file
  python src/nppes_backfill.py --input-file path/to/providers.parquet
  
  # Test with small sample
  python src/nppes_backfill.py --limit 50
  
  # Run with custom settings
  python src/nppes_backfill.py --limit 1000 --request-delay 0.2
        """
    )
    
    parser.add_argument(
        '--input-file',
        type=str,
        default='output/providers_20250806_181227.parquet',
        help='Path to input providers parquet file (default: output/providers_20250806_181227.parquet)'
    )
    
    parser.add_argument(
        '--output-file',
        type=str,
        default='nppes_data/nppes_providers.parquet',
        help='Path to output NPPES parquet file (default: nppes_data/nppes_providers.parquet)'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of NPIs to process (useful for testing)'
    )
    
    parser.add_argument(
        '--request-delay',
        type=float,
        default=0.1,
        help='Delay between API requests in seconds (default: 0.1)'
    )
    
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Maximum number of retries for failed API requests (default: 3)'
    )
    
    args = parser.parse_args()
    
    try:
        # Create configuration
        config = NPPESConfig(
            input_providers_file=args.input_file,
            nppes_output_file=args.output_file,
            limit=args.limit,
            request_delay=args.request_delay,
            max_retries=args.max_retries
        )
        
        # Log configuration
        logger.info("NPPES Backfill Configuration:")
        logger.info(f"  Input File: {config.input_providers_file}")
        logger.info(f"  Output File: {config.nppes_output_file}")
        logger.info(f"  Limit: {config.limit or 'No limit'}")
        logger.info(f"  Request delay: {config.request_delay}s")
        logger.info(f"  Max retries: {config.max_retries}")
        
        # Initialize backfill processor
        backfill_processor = NPPESBackfill(config)
        
        # Run backfill
        backfill_processor.run_backfill()
        
    except Exception as e:
        logger.error(f"NPPES backfill failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 