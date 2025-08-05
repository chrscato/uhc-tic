"""Schema detection for MRF file formats."""

from typing import Dict, Any, Optional
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

class SchemaDetector:
    """Detects and classifies MRF schema formats."""

    SCHEMA_TYPES = {
        "prov_ref_infile": "Provider references embedded in file",
        "prov_ref_url": "Provider references in external URLs"
    }

    def __init__(self):
        """Initialize schema detector."""
        self.logger = logger

    def detect_schema(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Detect the schema type from the input data.

        Args:
            data: Dictionary containing MRF JSON data

        Returns:
            str: Schema type ("prov_ref_infile" or "prov_ref_url") or None if unknown
        """
        try:
            provider_refs = data.get("provider_references", [])
            if not provider_refs:
                self.logger.warning("No provider_references found in data")
                return None

            # Check first provider reference to determine type
            first_ref = provider_refs[0]

            # Check for external URL pattern
            if "location" in first_ref:
                self.logger.debug("Detected provider references in external URLs")
                return "prov_ref_url"

            # Check for embedded provider groups
            if "provider_groups" in first_ref:
                self.logger.debug("Detected embedded provider references")
                return "prov_ref_infile"

            self.logger.warning(
                "Unknown provider_references format - missing both 'location' and 'provider_groups'"
            )
            return None

        except Exception as e:
            self.logger.error(f"Error detecting schema: {str(e)}")
            return None

    def validate_schema(self, data: Dict[str, Any], expected_type: str) -> bool:
        """
        Validate that data matches expected schema type.

        Args:
            data: Dictionary containing MRF JSON data
            expected_type: Expected schema type to validate against

        Returns:
            bool: True if schema matches expected type, False otherwise
        """
        detected = self.detect_schema(data)
        if detected != expected_type:
            self.logger.warning(
                f"Schema validation failed - Expected {expected_type} but detected {detected}"
            )
            return False
        return True