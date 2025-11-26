"""
Request Scanner - identifies batching requests on-chain.

Monitors a script address for new UTXOs that represent batch requests.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, List, Optional, Set, Tuple

import structlog

from pycardano import UTxO

from batcher.config import BatcherConfig, get_config
from batcher.core.request import BatchRequest
from batcher.node.interface import NodeInterface

logger = structlog.get_logger(__name__)


class DatumParser(ABC):
    """
    Abstract base class for parsing request datums.
    
    DApps should implement this to extract request details from their specific datum format.
    """
    
    @abstractmethod
    def is_valid_request(self, datum: Any) -> bool:
        """
        Check if a datum represents a valid batch request.
        
        Args:
            datum: The decoded datum from the UTXO
            
        Returns:
            True if this is a valid batch request datum
        """
        pass
    
    @abstractmethod
    def parse_request_details(
        self,
        datum: Any,
        utxo: UTxO,
    ) -> Tuple[Optional[str], Optional[str], dict]:
        """
        Parse request details from datum.
        
        Args:
            datum: The decoded datum
            utxo: The UTXO containing the request
            
        Returns:
            Tuple of (requester_address, request_type, request_params)
        """
        pass


class DefaultDatumParser(DatumParser):
    """
    Default datum parser that accepts all UTXOs at the script address.
    
    This is a basic implementation - DApps should create their own parser
    that understands their specific datum format.
    """
    
    def __init__(self, datum_tag: Optional[str] = None):
        """
        Initialize with optional datum tag filter.
        
        Args:
            datum_tag: If provided, only accept datums with this tag
        """
        self.datum_tag = datum_tag
    
    def is_valid_request(self, datum: Any) -> bool:
        """Accept all datums, optionally filtering by tag."""
        if datum is None:
            return True  # Accept UTXOs without datum
        
        if self.datum_tag:
            # Try to find the tag in various datum formats
            try:
                if isinstance(datum, dict):
                    return datum.get("tag") == self.datum_tag
                if isinstance(datum, (list, tuple)) and len(datum) > 0:
                    return str(datum[0]) == self.datum_tag
            except Exception:
                pass
            return False
        
        return True
    
    def parse_request_details(
        self,
        datum: Any,
        utxo: UTxO,
    ) -> Tuple[Optional[str], Optional[str], dict]:
        """Extract basic details from datum."""
        requester_address = None
        request_type = None
        params = {}
        
        if datum is None:
            return requester_address, request_type, params
        
        try:
            if isinstance(datum, dict):
                requester_address = datum.get("requester") or datum.get("owner")
                request_type = datum.get("type") or datum.get("action")
                params = {k: v for k, v in datum.items() 
                         if k not in ("requester", "owner", "type", "action", "tag")}
            
            elif isinstance(datum, (list, tuple)):
                # Common pattern: [tag, requester, type, ...params]
                if len(datum) >= 2:
                    # First element might be tag, skip it if string
                    start = 1 if isinstance(datum[0], str) else 0
                    if len(datum) > start:
                        requester_address = str(datum[start]) if datum[start] else None
                    if len(datum) > start + 1:
                        request_type = str(datum[start + 1]) if datum[start + 1] else None
                    if len(datum) > start + 2:
                        params = {"extra": datum[start + 2:]}
                        
        except Exception as e:
            logger.debug("datum_parse_warning", error=str(e))
        
        return requester_address, request_type, params


class RequestScanner:
    """
    Scans the blockchain for batch requests.
    
    Monitors a script address and identifies UTXOs that represent
    pending batch requests based on their datum.
    
    Responsibilities:
    - Query UTXOs at the script address
    - Parse datums to identify valid requests  
    - Track known requests to detect new ones
    - Detect spent/cancelled requests
    """
    
    def __init__(
        self,
        node: NodeInterface,
        script_address: str,
        datum_parser: Optional[DatumParser] = None,
        config: Optional[BatcherConfig] = None,
    ):
        """
        Initialize the request scanner.
        
        Args:
            node: Node interface for blockchain queries
            script_address: Script address to monitor for requests
            datum_parser: Parser for interpreting request datums
            config: Batcher configuration
        """
        self.node = node
        self.script_address = script_address
        self.datum_parser = datum_parser or DefaultDatumParser(
            datum_tag=config.request_datum_tag if config else None
        )
        self.config = config or get_config()
        
        # Track known request IDs to detect new ones
        self._known_request_ids: Set[str] = set()
        
        # Statistics
        self._scan_count = 0
        self._last_scan_time: Optional[datetime] = None
    
    async def scan_for_requests(self) -> List[BatchRequest]:
        """
        Scan for batch requests on-chain.
        
        Returns:
            List of BatchRequest objects for valid requests found
        """
        self._scan_count += 1
        self._last_scan_time = datetime.utcnow()
        
        logger.debug(
            "scanning_for_requests",
            address=self.script_address[:30] + "...",
            scan_number=self._scan_count,
        )
        
        try:
            # Get UTXOs with datums at the script address
            utxos_with_datums = await self.node.get_script_utxos_with_datum(
                self.script_address,
                datum_filter=self.datum_parser.is_valid_request,
            )
            
            requests = []
            for utxo, datum in utxos_with_datums:
                request = self._create_request_from_utxo(utxo, datum)
                if request:
                    requests.append(request)
            
            logger.info(
                "scan_complete",
                total_utxos=len(utxos_with_datums),
                valid_requests=len(requests),
            )
            
            return requests
            
        except Exception as e:
            logger.error("scan_failed", error=str(e))
            raise
    
    async def scan_for_new_requests(self) -> List[BatchRequest]:
        """
        Scan and return only new requests (not previously seen).
        
        Returns:
            List of newly discovered BatchRequest objects
        """
        all_requests = await self.scan_for_requests()
        
        new_requests = []
        for request in all_requests:
            if request.request_id not in self._known_request_ids:
                new_requests.append(request)
                self._known_request_ids.add(request.request_id)
        
        if new_requests:
            logger.info(
                "new_requests_found",
                count=len(new_requests),
                ids=[r.request_id[:20] + "..." for r in new_requests],
            )
        
        return new_requests
    
    async def check_requests_still_valid(
        self,
        request_ids: List[str],
    ) -> Tuple[List[str], List[str]]:
        """
        Check which requests are still valid (UTXO not spent).
        
        Args:
            request_ids: List of request IDs to check
            
        Returns:
            Tuple of (valid_ids, spent_ids)
        """
        valid_ids = []
        spent_ids = []
        
        for request_id in request_ids:
            # Parse tx_hash#index from request_id
            parts = request_id.split("#")
            if len(parts) != 2:
                spent_ids.append(request_id)
                continue
            
            tx_hash, output_index = parts[0], int(parts[1])
            
            if await self.node.check_utxo_exists(tx_hash, output_index):
                valid_ids.append(request_id)
            else:
                spent_ids.append(request_id)
                self._known_request_ids.discard(request_id)
        
        if spent_ids:
            logger.info(
                "spent_requests_detected",
                count=len(spent_ids),
                ids=[rid[:20] + "..." for rid in spent_ids],
            )
        
        return valid_ids, spent_ids
    
    def _create_request_from_utxo(
        self,
        utxo: UTxO,
        datum: Any,
    ) -> Optional[BatchRequest]:
        """
        Create a BatchRequest from a UTXO and its datum.
        
        Args:
            utxo: The UTXO containing the request
            datum: The decoded datum
            
        Returns:
            BatchRequest if valid, None otherwise
        """
        try:
            tx_hash = str(utxo.input.transaction_id)
            output_index = utxo.input.index
            request_id = f"{tx_hash}#{output_index}"
            
            # Parse request details from datum
            requester_address, request_type, params = self.datum_parser.parse_request_details(
                datum, utxo
            )
            
            # Get datum hash if available
            datum_hash = None
            if hasattr(utxo.output, 'datum_hash') and utxo.output.datum_hash:
                datum_hash = utxo.output.datum_hash.hex() if isinstance(
                    utxo.output.datum_hash, bytes
                ) else str(utxo.output.datum_hash)
            
            request = BatchRequest(
                request_id=request_id,
                utxo_ref=utxo.input,
                value=utxo.output.amount,
                datum=datum,
                datum_hash=datum_hash,
                requester_address=requester_address,
                request_type=request_type,
                request_params=params,
            )
            
            logger.debug(
                "request_created",
                request_id=request_id,
                ada_amount=request.ada_amount,
                request_type=request_type,
            )
            
            return request
            
        except Exception as e:
            logger.warning(
                "request_creation_failed",
                tx_hash=str(utxo.input.transaction_id)[:20],
                error=str(e),
            )
            return None
    
    def register_known_request(self, request_id: str) -> None:
        """
        Register a request as known (e.g., loaded from database).
        
        Args:
            request_id: The request ID to register
        """
        self._known_request_ids.add(request_id)
    
    def register_known_requests(self, request_ids: List[str]) -> None:
        """Register multiple requests as known."""
        self._known_request_ids.update(request_ids)
    
    @property
    def known_request_count(self) -> int:
        """Get count of known request IDs."""
        return len(self._known_request_ids)
    
    def get_stats(self) -> dict:
        """Get scanner statistics."""
        return {
            "script_address": self.script_address[:30] + "...",
            "scan_count": self._scan_count,
            "last_scan_time": self._last_scan_time.isoformat() if self._last_scan_time else None,
            "known_requests": self.known_request_count,
        }

