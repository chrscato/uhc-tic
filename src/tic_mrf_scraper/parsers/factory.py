"""Factory for creating MRF parsers based on schema type."""

from typing import Dict, Any, Optional, Type
from .base import BaseDynamicParser
from .prov_ref_infile import ProvRefInfileParser
from .prov_ref_url import ProvRefUrlParser
from ..utils.backoff_logger import get_logger
from ..schema.detector import SchemaDetector

logger = get_logger(__name__)

class ParserFactory:
    """Factory for creating appropriate MRF parsers."""

    def __init__(self):
        """Initialize parser factory."""
        self.detector = SchemaDetector()
        self.logger = logger
        self._parsers: Dict[str, Type[BaseDynamicParser]] = {
            "prov_ref_infile": ProvRefInfileParser,
            "prov_ref_url": ProvRefUrlParser
        }

    def create_parser(self, data: Dict[str, Any], payer_name: str = "unknown") -> Optional[BaseDynamicParser]:
        """
        Create appropriate parser based on schema detection.

        Args:
            data: Raw MRF JSON data
            payer_name: Optional payer name for the parser

        Returns:
            BaseDynamicParser: Appropriate parser instance or None if schema unknown
        """
        schema_type = self.detector.detect_schema(data)
        if not schema_type:
            self.logger.error("Could not detect schema type")
            return None

        parser_class = self._parsers.get(schema_type)
        if not parser_class:
            self.logger.error(f"No parser available for schema type: {schema_type}")
            return None

        return parser_class(payer_name=payer_name)