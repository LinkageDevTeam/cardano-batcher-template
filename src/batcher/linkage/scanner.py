"""
Linkage Finance Request Scanner.

Scans the blockchain for Linkage Finance fund UTXOs and identifies
batch requests based on the fund datum structure.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import structlog

from pycardano import (
    UTxO,
    Address,
    ScriptHash,
    AssetName,
    RawPlutusData,
    IndefiniteList,
)

from batcher.engine.scanner import DatumParser
from batcher.node.interface import NodeInterface
from batcher.linkage.config import LinkageConfig, VERIFICATION_NFT_NAME
from batcher.linkage.types import (
    LinkageRequest,
    LinkageRequestType,
    IndexParameters,
    ImmutableParams,
    IndexedToken,
    TokenData,
    token_name_from_parameters,
)

logger = structlog.get_logger(__name__)


class LinkageDatumParser(DatumParser):
    """
    Datum parser for Linkage Finance fund UTXOs.
    
    Parses IndexParameters datums and extracts fund information
    needed for batch processing.
    """
    
    def __init__(self, config: LinkageConfig):
        """
        Initialize the parser.
        
        Args:
            config: Linkage configuration
        """
        self.config = config
        self._fund_script_hash = config.fund_script_hash
        self._auth_script_hash = config.authenticator_script_hash
    
    def is_valid_request(self, datum: Any) -> bool:
        """
        Check if a datum represents a valid Linkage fund UTXO.
        
        A valid Linkage fund datum is an IndexParameters structure
        with properly formatted immutable params.
        
        Args:
            datum: The decoded datum from the UTXO
            
        Returns:
            True if this is a valid Linkage fund datum
        """
        if datum is None:
            return False
        
        try:
            # Try to parse as IndexParameters
            if isinstance(datum, IndexParameters):
                return self._validate_index_parameters(datum)
            
            # If raw data, try to convert
            if isinstance(datum, dict):
                return self._is_valid_dict_datum(datum)
            
            if isinstance(datum, RawPlutusData):
                return self._is_valid_raw_datum(datum)
            
            return False
            
        except Exception as e:
            logger.debug("datum_validation_failed", error=str(e))
            return False
    
    def _validate_index_parameters(self, params: IndexParameters) -> bool:
        """Validate an IndexParameters structure."""
        try:
            # Check immutable params exist
            if params.immutable_params is None:
                return False
            
            # Check required fields
            imm = params.immutable_params
            if not imm.index_tokens:
                return False
            if not imm.index_name:
                return False
            if not imm.creator:
                return False
            if imm.fund_token_factor <= 0:
                return False
            if imm.royalty_factor < 0:
                return False
            
            # Validate each indexed token
            for idx_token in imm.index_tokens:
                if not isinstance(idx_token, IndexedToken):
                    return False
                if idx_token.factor <= 0:
                    return False
            
            return True
            
        except Exception:
            return False
    
    def _is_valid_dict_datum(self, datum: dict) -> bool:
        """Check if a dictionary looks like an IndexParameters datum."""
        # Check for expected structure
        if "constructor" not in datum and "fields" not in datum:
            # Not a CBOR-decoded Plutus data structure
            return False
        
        # IndexParameters has CONSTR_ID = 0
        if datum.get("constructor") != 0:
            return False
        
        fields = datum.get("fields", [])
        if len(fields) != 2:
            return False
        
        # First field should be immutable params (also constructor 0)
        imm_params = fields[0]
        if not isinstance(imm_params, dict) or imm_params.get("constructor") != 0:
            return False
        
        return True
    
    def _is_valid_raw_datum(self, raw: RawPlutusData) -> bool:
        """Check if raw Plutus data is valid IndexParameters."""
        try:
            # Try to parse as IndexParameters
            parsed = IndexParameters.from_primitive(raw.to_primitive())
            return self._validate_index_parameters(parsed)
        except Exception:
            return False
    
    def parse_request_details(
        self,
        datum: Any,
        utxo: UTxO,
    ) -> Tuple[Optional[str], Optional[str], dict]:
        """
        Parse request details from a Linkage fund datum.
        
        Args:
            datum: The decoded datum
            utxo: The UTXO containing the request
            
        Returns:
            Tuple of (requester_address, request_type, request_params)
        """
        try:
            # Parse the datum
            index_params = self._parse_index_parameters(datum)
            if not index_params:
                return None, None, {}
            
            # Extract details
            creator_pkh = index_params.immutable_params.creator.hex()
            fund_name = index_params.immutable_params.index_name.decode('utf-8', errors='replace')
            
            # Calculate fund token name
            fund_token_name = token_name_from_parameters(index_params).hex()
            
            params = {
                "fund_token_name": fund_token_name,
                "fund_name": fund_name,
                "creator_pkh": creator_pkh,
                "royalty_factor": index_params.immutable_params.royalty_factor,
                "fund_token_factor": index_params.immutable_params.fund_token_factor,
                "accrued_royalty": index_params.accrued_royalty,
                "index_tokens": [
                    {
                        "policy_id": t.token.policy_id.hex(),
                        "token_name": t.token.token_name.hex(),
                        "factor": t.factor,
                    }
                    for t in index_params.immutable_params.index_tokens
                ],
            }
            
            return None, "linkage_fund", params
            
        except Exception as e:
            logger.warning("parse_request_details_failed", error=str(e))
            return None, None, {}
    
    def _parse_index_parameters(self, datum: Any) -> Optional[IndexParameters]:
        """Parse datum to IndexParameters."""
        try:
            if isinstance(datum, IndexParameters):
                return datum.to_sanitized()
            
            if isinstance(datum, RawPlutusData):
                parsed = IndexParameters.from_primitive(datum.to_primitive())
                return parsed.to_sanitized()
            
            if isinstance(datum, dict):
                # Parse from CBOR-like dictionary
                return self._parse_dict_to_index_params(datum)
            
            return None
            
        except Exception as e:
            logger.debug("parse_index_parameters_failed", error=str(e))
            return None
    
    def _parse_dict_to_index_params(self, datum: dict) -> Optional[IndexParameters]:
        """Parse a dictionary to IndexParameters."""
        try:
            fields = datum.get("fields", [])
            if len(fields) != 2:
                return None
            
            imm_fields = fields[0].get("fields", [])
            if len(imm_fields) != 5:
                return None
            
            # Parse index tokens
            index_tokens = []
            token_list = imm_fields[0]
            if isinstance(token_list, list):
                for t in token_list:
                    t_fields = t.get("fields", [])
                    token_fields = t_fields[0].get("fields", [])
                    index_tokens.append(IndexedToken(
                        token=TokenData(
                            policy_id=bytes.fromhex(token_fields[0]),
                            token_name=bytes.fromhex(token_fields[1]),
                        ),
                        factor=t_fields[1],
                    ))
            
            # Parse immutable params
            imm_params = ImmutableParams(
                index_tokens=IndefiniteList(index_tokens),
                index_name=bytes.fromhex(imm_fields[1]),
                creator=bytes.fromhex(imm_fields[2]),
                fund_token_factor=imm_fields[3],
                royalty_factor=imm_fields[4],
            )
            
            # Parse full parameters
            return IndexParameters(
                immutable_params=imm_params,
                accrued_royalty=fields[1],
            )
            
        except Exception as e:
            logger.debug("parse_dict_to_index_params_failed", error=str(e))
            return None


class LinkageRequestScanner:
    """
    Scans for Linkage Finance fund UTXOs and request queue.
    
    This scanner monitors both the fund contract address for existing funds
    and can monitor a separate request queue address for user requests.
    """
    
    def __init__(
        self,
        node: NodeInterface,
        config: LinkageConfig,
    ):
        """
        Initialize the scanner.
        
        Args:
            node: Node interface for blockchain queries
            config: Linkage configuration
        """
        self.node = node
        self.config = config
        self.datum_parser = LinkageDatumParser(config)
        
        # Track known fund UTXOs
        self._known_funds: Dict[str, LinkageRequest] = {}
        self._last_scan_time: Optional[datetime] = None
        
        # Reference UTXO cache
        self._ref_utxo_cache: Dict[str, str] = {}
        self._load_ref_utxo_cache()
    
    def _load_ref_utxo_cache(self):
        """Load reference UTXO cache from disk."""
        cache_path = self.config.ref_utxo_cache_path
        if cache_path.exists():
            try:
                with open(cache_path, 'r') as f:
                    self._ref_utxo_cache = json.load(f)
            except Exception as e:
                logger.warning("failed_to_load_ref_utxo_cache", error=str(e))
    
    def _save_ref_utxo_cache(self):
        """Save reference UTXO cache to disk."""
        try:
            with open(self.config.ref_utxo_cache_path, 'w') as f:
                json.dump(self._ref_utxo_cache, f)
        except Exception as e:
            logger.warning("failed_to_save_ref_utxo_cache", error=str(e))
    
    async def scan_fund_utxos(self) -> List[LinkageRequest]:
        """
        Scan for all fund UTXOs at the fund contract address.
        
        Returns:
            List of LinkageRequest objects representing existing funds
        """
        self._last_scan_time = datetime.utcnow()
        
        logger.debug(
            "scanning_fund_utxos",
            address=self.config.fund_address[:30] + "...",
        )
        
        try:
            # Get all UTXOs at fund address
            utxos = await self.node.get_utxos_at_address(self.config.fund_address)
            
            requests = []
            for utxo in utxos:
                request = await self._parse_fund_utxo(utxo)
                if request:
                    requests.append(request)
            
            logger.info(
                "fund_scan_complete",
                total_utxos=len(utxos),
                valid_funds=len(requests),
            )
            
            return requests
            
        except Exception as e:
            logger.error("fund_scan_failed", error=str(e))
            raise
    
    async def _parse_fund_utxo(self, utxo: UTxO) -> Optional[LinkageRequest]:
        """
        Parse a fund UTXO into a LinkageRequest.
        
        Args:
            utxo: The UTXO to parse
            
        Returns:
            LinkageRequest if valid, None otherwise
        """
        try:
            # Check for inline datum
            if not utxo.output.datum:
                return None
            
            # Parse datum
            raw_datum = utxo.output.datum
            if hasattr(raw_datum, 'data'):
                datum_data = raw_datum.data
            else:
                datum_data = raw_datum
            
            # Validate and parse
            if not self.datum_parser.is_valid_request(datum_data):
                return None
            
            # Parse to IndexParameters
            index_params = self.datum_parser._parse_index_parameters(datum_data)
            if not index_params:
                return None
            
            # Calculate fund token name
            fund_token_name = token_name_from_parameters(index_params).hex()
            
            # Create request ID from UTXO reference
            tx_hash = str(utxo.input.transaction_id)
            output_index = utxo.input.index
            request_id = f"{tx_hash}#{output_index}"
            
            return LinkageRequest(
                request_id=request_id,
                utxo_ref=utxo.input,
                fund_datum=index_params,
                request_type=LinkageRequestType.DEPOSIT,  # Default, can be updated
                user_address="",  # Will be set based on request type
                amount=0,  # Will be set based on request
                fund_token_name=fund_token_name,
                fund_script_hash=self.config.fund_script_hash,
                value=utxo.output.amount,
            )
            
        except Exception as e:
            logger.debug("parse_fund_utxo_failed", error=str(e))
            return None
    
    async def get_fund_by_token_name(
        self,
        fund_token_name: str,
    ) -> Optional[LinkageRequest]:
        """
        Get a specific fund by its token name.
        
        Args:
            fund_token_name: Hex-encoded fund token name
            
        Returns:
            LinkageRequest for the fund or None
        """
        # Scan all funds and find matching one
        funds = await self.scan_fund_utxos()
        
        for fund in funds:
            if fund.fund_token_name == fund_token_name:
                return fund
        
        return None
    
    async def get_reference_script_utxo(
        self,
        script_hash: ScriptHash,
    ) -> Optional[UTxO]:
        """
        Get a reference script UTXO for a given script hash.
        
        Args:
            script_hash: Hash of the script to find
            
        Returns:
            UTxO containing the reference script or None
        """
        script_hash_hex = script_hash.payload.hex()
        
        # Check cache first
        if script_hash_hex in self._ref_utxo_cache:
            try:
                cached_cbor = self._ref_utxo_cache[script_hash_hex]
                return UTxO.from_cbor(bytes.fromhex(cached_cbor))
            except Exception:
                # Cache invalid, remove it
                del self._ref_utxo_cache[script_hash_hex]
        
        # Scan reference script address
        try:
            utxos = await self.node.get_utxos_at_address(self.config.ref_script_address)
            
            for utxo in utxos:
                if utxo.output.script is not None:
                    from pycardano import script_hash as calc_hash
                    if calc_hash(utxo.output.script) == script_hash:
                        # Cache it
                        self._ref_utxo_cache[script_hash_hex] = utxo.to_cbor().hex()
                        self._save_ref_utxo_cache()
                        return utxo
            
            return None
            
        except Exception as e:
            logger.error("get_reference_script_failed", error=str(e))
            return None
    
    async def get_factory_utxo(self) -> Optional[UTxO]:
        """
        Get the factory UTXO containing the verification NFT.
        
        Returns:
            Factory UTxO or None
        """
        try:
            utxos = await self.node.get_utxos_at_address(self.config.factory_address)
            
            for utxo in utxos:
                # Check for verification NFT
                if utxo.output.amount.multi_asset:
                    auth_pid = self.config.authenticator_script_hash
                    if auth_pid in utxo.output.amount.multi_asset:
                        assets = utxo.output.amount.multi_asset[auth_pid]
                        if AssetName(VERIFICATION_NFT_NAME) in assets:
                            return utxo
            
            return None
            
        except Exception as e:
            logger.error("get_factory_utxo_failed", error=str(e))
            return None
    
    def get_stats(self) -> dict:
        """Get scanner statistics."""
        return {
            "fund_address": self.config.fund_address[:30] + "...",
            "known_funds": len(self._known_funds),
            "last_scan_time": self._last_scan_time.isoformat() if self._last_scan_time else None,
            "ref_utxo_cache_size": len(self._ref_utxo_cache),
        }

