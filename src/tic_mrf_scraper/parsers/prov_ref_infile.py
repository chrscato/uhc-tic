"""Parser for MRFs with inline provider references (Anthem-style)."""

from typing import Dict, Any, List, Optional, Set, Iterator
from .base import BaseDynamicParser

class ProvRefInfileParser(BaseDynamicParser):
    """Parser for MRFs with inline provider references."""

    def parse_provider_references(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse inline provider references.

        Args:
            data: Raw MRF JSON data

        Returns:
            Dict mapping provider reference IDs to provider data
        """
        provider_refs = {}
        
        for ref in data.get("provider_references", []):
            ref_id = ref.get("provider_group_id")
            if ref_id and "provider_groups" in ref:
                provider_refs[ref_id] = {
                    "provider_groups": ref["provider_groups"]
                }
                
                # Log provider group stats
                group_count = len(ref["provider_groups"])
                npi_count = sum(len(group.get("npi", [])) 
                              for group in ref["provider_groups"])
                self.logger.debug(
                    f"Parsed provider group {ref_id}: {group_count} groups, {npi_count} NPIs"
                )

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
            # Parse provider references first
            provider_refs = self.parse_provider_references(data)
            if not provider_refs:
                self.logger.warning("No provider references found")
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