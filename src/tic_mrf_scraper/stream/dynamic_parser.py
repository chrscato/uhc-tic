"""Enhanced streaming parser with dynamic format detection."""

import json
from typing import Dict, Any, Optional, Set, Generator, Union, TextIO, Iterator
from ..schema.detector import SchemaDetector
from ..parsers.factory import ParserFactory
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

class DynamicStreamingParser:
    """Enhanced streaming parser with dynamic format detection."""

    def __init__(self, 
                 payer_name: str,
                 cpt_whitelist: Optional[Set[str]] = None,
                 chunk_size: int = 10000):
        """
        Initialize dynamic streaming parser.

        Args:
            payer_name: Name of the payer
            cpt_whitelist: Optional set of allowed CPT codes
            chunk_size: Number of in_network items to process per chunk
        """
        self.payer_name = payer_name
        self.cpt_whitelist = cpt_whitelist
        self.chunk_size = chunk_size
        self.detector = SchemaDetector()
        self.parser_factory = ParserFactory()
        self.logger = logger

    def _chunk_in_network(self, 
                         data: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        """
        Split in_network array into chunks while preserving other data.

        Args:
            data: Full MRF JSON data

        Yields:
            Dict containing chunk of in_network items with shared data
        """
        in_network = data.get("in_network", [])
        total_items = len(in_network)
        
        for i in range(0, total_items, self.chunk_size):
            chunk = data.copy()
            chunk["in_network"] = in_network[i:i + self.chunk_size]
            yield chunk

    def parse_stream(self, 
                    input_data: Union[str, Dict[str, Any], TextIO],
                    schema_type: Optional[str] = None,
                    parser: Optional[Any] = None) -> Iterator[Dict[str, Any]]:
        """
        Parse streaming MRF data with dynamic format detection.

        Args:
            input_data: Path to MRF JSON file, file-like object, or dict containing data
            schema_type: Optional pre-detected schema type
            parser: Optional pre-created parser instance

        Yields:
            Normalized rate records
        """
        try:
            # Load data if needed
            if isinstance(input_data, dict):
                data = input_data
            elif isinstance(input_data, str):
                with open(input_data) as f:
                    data = json.load(f)
            else:
                data = json.load(input_data)
            
            # Use provided schema type or detect
            if not schema_type:
                schema_type = self.detector.detect_schema(data)
                if not schema_type:
                    self.logger.error("Could not detect schema type")
                    return

            # Use provided parser or create new one
            if not parser:
                parser = self.parser_factory.create_parser(data, self.payer_name)
                if not parser:
                    self.logger.error("Could not create parser")
                    return

            # Initialize parser with payer info
            parser.payer_name = self.payer_name
            parser.cpt_whitelist = self.cpt_whitelist

            # Process data in chunks
            total_records = 0
            for chunk in self._chunk_in_network(data):
                for record in parser.parse(chunk):
                    total_records += 1
                    yield record

            self.logger.info(
                f"Completed streaming parse: {total_records} records processed "
                f"using {schema_type} parser"
            )

        except Exception as e:
            self.logger.error(f"Error in streaming parse: {str(e)}")
            return