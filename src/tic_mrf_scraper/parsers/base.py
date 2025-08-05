"""Base class for dynamic MRF parsers."""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Set, Generator, Iterator
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

class BaseDynamicParser(ABC):
    """Abstract base class for dynamic MRF parsers."""

    def __init__(self, payer_name: str, cpt_whitelist: Optional[Set[str]] = None):
        """
        Initialize base parser.

        Args:
            payer_name: Name of the payer
            cpt_whitelist: Optional set of allowed CPT codes
        """
        self.payer_name = payer_name
        self.cpt_whitelist = cpt_whitelist or set()
        self.logger = logger

    @abstractmethod
    def parse_provider_references(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse provider references section.

        Args:
            data: Raw MRF JSON data

        Returns:
            Dict mapping provider reference IDs to provider data
        """
        pass

    def normalize_rate_record(self, 
                            rate_data: Dict[str, Any], 
                            provider_data: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Normalize a rate record with provider data.

        Args:
            rate_data: Raw rate data from in_network section
            provider_data: Provider reference data

        Yields:
            Normalized rate records (one per provider NPI)
        """
        billing_code = rate_data.get("billing_code")
        if not billing_code or (self.cpt_whitelist and billing_code not in self.cpt_whitelist):
            return

        base_record = {
            "service_code": billing_code,
            "billing_code_type": rate_data.get("billing_code_type", ""),
            "description": rate_data.get("description", ""),
            "service_codes": [],
            "billing_class": "",
            "negotiated_type": "",
            "expiration_date": "",
            "payer": self.payer_name
        }

        # Process each negotiated rate group
        for rate_group in rate_data.get("negotiated_rates", []):
            rate_info = rate_group.get("negotiated_prices", [{}])[0]
            
            base_record.update({
                "negotiated_rate": float(rate_info.get("negotiated_rate", 0)),
                "service_codes": rate_info.get("service_code", []),
                "billing_class": rate_info.get("billing_class", ""),
                "billing_code_modifier": rate_info.get("billing_code_modifier", [])
            })

            # Create separate record for each provider
            for provider in provider_data.get("provider_groups", []):
                # Handle multiple NPIs
                for npi in provider.get("npi", []):
                    record = base_record.copy()
                    record.update({
                        "provider_npi": str(npi),
                        "provider_tin": provider.get("tin", {}).get("value", "")
                    })
                    yield record

    def parse_in_network_rates(self, 
                             data: Dict[str, Any], 
                             provider_refs: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Parse in_network section with resolved provider references.

        Args:
            data: Raw MRF JSON data
            provider_refs: Resolved provider reference data

        Yields:
            Normalized rate records
        """
        for rate_item in data.get("in_network", []):
            for rate_group in rate_item.get("negotiated_rates", []):
                # Get provider references for this rate
                for ref_id in rate_group.get("provider_references", []):
                    provider_data = provider_refs.get(ref_id)
                    if provider_data:
                        yield from self.normalize_rate_record(rate_item, provider_data)

    @abstractmethod
    def parse(self, data: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Parse MRF data into normalized records.

        Args:
            data: Raw MRF JSON data

        Yields:
            Normalized rate records
        """
        pass