"""UnitedHealthcare Georgia handler."""

from typing import Dict, Any, Iterator
from ..payers import PayerHandler

class UhcGaHandler(PayerHandler):
    """Handler for UnitedHealthcare Georgia MRF files."""

    def parse_in_network(self, data: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """Parse in-network rates records."""
        if "in_network" not in data:
            return

        for item in data["in_network"]:
            if not isinstance(item, dict):
                continue

            record = {
                "billing_code": item.get("billing_code", ""),
                "billing_code_type": item.get("billing_code_type", ""),
                "billing_code_type_version": item.get("billing_code_type_version", ""),
                "description": item.get("description", ""),
                "negotiation_arrangement": item.get("negotiation_arrangement", ""),
                "name": item.get("name", ""),
                "record_type": "rates"
            }

            # Handle negotiated rates
            if "negotiated_rates" in item and isinstance(item["negotiated_rates"], list):
                for rate_info in item["negotiated_rates"]:
                    if not isinstance(rate_info, dict):
                        continue

                    # Get negotiated prices
                    if "negotiated_prices" in rate_info and isinstance(rate_info["negotiated_prices"], list):
                        for price in rate_info["negotiated_prices"]:
                            if not isinstance(price, dict):
                                continue

                            rate_record = record.copy()
                            rate_record.update({
                                "negotiated_rate": price.get("negotiated_rate", 0.0),
                                "negotiated_type": price.get("negotiated_type", ""),
                                "service_code": record["billing_code"],
                                "billing_class": "professional",
                                "expiration_date": price.get("expiration_date", ""),
                                "provider_references": rate_info.get("provider_references", []),
                                "provider_groups": rate_info.get("provider_groups", []),
                                "tin": rate_info.get("tin", {}).get("value", ""),
                                "service_code_type": record["billing_code_type"],
                                "payer": "uhc_ga"
                            })
                            yield rate_record