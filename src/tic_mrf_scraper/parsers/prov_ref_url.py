"""Parser for MRFs with external provider references (Cigna-style)."""

from typing import Dict, Any, List, Optional, Set, Iterator
from .base import BaseDynamicParser
from ..fetch.multi_url_fetcher import MultiUrlFetcher

class ProvRefUrlParser(BaseDynamicParser):
    """Parser for MRFs with external provider references."""

    def __init__(self, 
                 payer_name: str, 
                 cpt_whitelist: Optional[Set[str]] = None,
                 fetcher: Optional[MultiUrlFetcher] = None):
        """
        Initialize URL parser.

        Args:
            payer_name: Name of the payer
            cpt_whitelist: Optional set of allowed CPT codes
            fetcher: Optional custom URL fetcher
        """
        super().__init__(payer_name, cpt_whitelist)
        self.fetcher = fetcher or MultiUrlFetcher()

    def parse_provider_references(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse and fetch external provider references.

        Args:
            data: Raw MRF JSON data

        Returns:
            Dict mapping provider reference IDs to provider data
        """
        provider_refs = {}
        urls = []
        id_to_url = {}

        # Collect URLs and map IDs
        for ref in data.get("provider_references", []):
            ref_id = ref.get("provider_group_id")
            url = ref.get("location")
            if ref_id and url:
                urls.append(url)
                id_to_url[url] = ref_id

        if not urls:
            self.logger.warning("No provider reference URLs found")
            return provider_refs

        # Fetch provider data from URLs
        self.logger.info(f"Fetching {len(urls)} provider reference URLs")
        results = self.fetcher.fetch_all(urls)

        # Process results
        for url, result in results.items():
            ref_id = id_to_url[url]
            if isinstance(result, dict) and "provider_groups" in result:
                provider_refs[ref_id] = {
                    "provider_groups": result["provider_groups"]
                }
                
                # Log provider group stats
                group_count = len(result["provider_groups"])
                npi_count = sum(len(group.get("npi", [])) 
                              for group in result["provider_groups"])
                self.logger.debug(
                    f"Fetched provider group {ref_id}: {group_count} groups, {npi_count} NPIs"
                )
            else:
                self.logger.warning(f"Invalid provider data from {url}")

        return provider_refs

    def parse(self, data: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Parse MRF data into normalized records.

        Args:
            data: Raw MRF JSON data

        Yields:
            Normalized rate records
        """
        try:
            # Parse and fetch provider references
            provider_refs = self.parse_provider_references(data)
            if not provider_refs:
                self.logger.warning("No provider references fetched")
                return

            # Parse rates with resolved provider data
            yield from self.parse_in_network_rates(data, provider_refs)
            
            self.logger.info(
                f"Processed provider references with "
                f"{len(provider_refs)} references"
            )

        except Exception as e:
            self.logger.error(f"Error parsing MRF data: {str(e)}")
            return