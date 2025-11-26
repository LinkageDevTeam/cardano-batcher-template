"""
Blockfrost API adapter for node integration.

Provides blockchain access via the Blockfrost API service.
"""

import asyncio
from typing import Any, List, Optional

import httpx
import structlog

from pycardano import (
    Address,
    MultiAsset,
    Asset,
    AssetName,
    ScriptHash,
    Transaction,
    TransactionId,
    TransactionInput,
    TransactionOutput,
    UTxO,
    Value,
    PlutusData,
    RawPlutusData,
)

from batcher.config import BatcherConfig, get_config
from batcher.node.interface import (
    NodeInterface,
    ProtocolParameters,
    ChainTip,
    NodeConnectionError,
    TransactionSubmitError,
)

logger = structlog.get_logger(__name__)


class BlockfrostAdapter(NodeInterface):
    """
    Blockfrost API adapter.
    
    Implements the NodeInterface using Blockfrost's REST API.
    """
    
    def __init__(self, config: Optional[BatcherConfig] = None):
        """
        Initialize the Blockfrost adapter.
        
        Args:
            config: Batcher configuration. Uses global config if not provided.
        """
        self.config = config or get_config()
        self.base_url = self.config.blockfrost_url
        self.project_id = self.config.blockfrost_project_id
        self._client: Optional[httpx.AsyncClient] = None
        self._protocol_params: Optional[ProtocolParameters] = None
    
    @property
    def headers(self) -> dict:
        """Get request headers with API key."""
        return {
            "project_id": self.project_id or "",
            "Content-Type": "application/json",
        }
    
    async def connect(self) -> None:
        """Establish connection (create HTTP client)."""
        if self._client is not None:
            return
        
        if not self.project_id:
            raise NodeConnectionError("Blockfrost project ID not configured")
        
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self.headers,
            timeout=30.0,
        )
        
        # Test connection
        try:
            response = await self._client.get("/health")
            if response.status_code != 200:
                raise NodeConnectionError(f"Blockfrost health check failed: {response.text}")
            logger.info("blockfrost_connected", base_url=self.base_url)
        except httpx.RequestError as e:
            raise NodeConnectionError(f"Failed to connect to Blockfrost: {e}")
    
    async def disconnect(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("blockfrost_disconnected")
    
    async def _request(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> Any:
        """Make an API request."""
        if not self._client:
            await self.connect()
        
        try:
            response = await self._client.request(method, path, **kwargs)
            
            if response.status_code == 404:
                return None
            
            if response.status_code != 200:
                error_msg = response.text
                logger.error(
                    "blockfrost_request_failed",
                    path=path,
                    status=response.status_code,
                    error=error_msg,
                )
                raise NodeConnectionError(f"Blockfrost API error: {error_msg}")
            
            return response.json()
            
        except httpx.RequestError as e:
            logger.error("blockfrost_request_error", path=path, error=str(e))
            raise NodeConnectionError(f"Blockfrost request failed: {e}")
    
    async def get_protocol_parameters(self) -> ProtocolParameters:
        """Get current protocol parameters from Blockfrost."""
        data = await self._request("GET", "/epochs/latest/parameters")
        
        self._protocol_params = ProtocolParameters(
            min_fee_a=int(data["min_fee_a"]),
            min_fee_b=int(data["min_fee_b"]),
            max_tx_size=int(data["max_tx_size"]),
            max_val_size=int(data["max_val_size"]),
            key_deposit=int(data["key_deposit"]),
            pool_deposit=int(data["pool_deposit"]),
            coins_per_utxo_byte=int(data["coins_per_utxo_word"]) // 8,  # Convert word to byte
            collateral_percentage=int(data["collateral_percent"]),
            max_collateral_inputs=int(data["max_collateral_inputs"]),
            price_mem=float(data["price_mem"]),
            price_step=float(data["price_step"]),
            max_tx_ex_mem=int(data["max_tx_ex_mem"]),
            max_tx_ex_steps=int(data["max_tx_ex_steps"]),
            cost_models=data.get("cost_models"),
        )
        
        return self._protocol_params
    
    async def get_chain_tip(self) -> ChainTip:
        """Get current chain tip."""
        data = await self._request("GET", "/blocks/latest")
        
        return ChainTip(
            slot=int(data["slot"]),
            block_hash=data["hash"],
            block_height=int(data["height"]),
            epoch=int(data["epoch"]),
            epoch_slot=int(data["epoch_slot"]),
        )
    
    async def get_utxos_at_address(
        self,
        address: str,
        asset: Optional[str] = None,
    ) -> List[UTxO]:
        """Get UTXOs at an address."""
        utxos = []
        page = 1
        
        while True:
            path = f"/addresses/{address}/utxos"
            if asset:
                path = f"/addresses/{address}/utxos/{asset}"
            
            data = await self._request("GET", f"{path}?page={page}")
            
            if not data:
                break
            
            for item in data:
                utxo = await self._parse_utxo(item)
                if utxo:
                    utxos.append(utxo)
            
            if len(data) < 100:  # Blockfrost page size
                break
            page += 1
        
        logger.debug("utxos_fetched", address=address[:20] + "...", count=len(utxos))
        return utxos
    
    async def _parse_utxo(self, data: dict) -> Optional[UTxO]:
        """Parse Blockfrost UTXO data into PyCardano UTxO."""
        try:
            tx_hash = data["tx_hash"]
            output_index = int(data["output_index"])
            
            # Parse value
            coin = int(data["amount"][0]["quantity"])  # First amount is always lovelace
            
            # Parse multi-assets
            multi_asset = None
            for amount in data["amount"][1:]:  # Skip lovelace
                if multi_asset is None:
                    multi_asset = MultiAsset()
                
                unit = amount["unit"]
                quantity = int(amount["quantity"])
                
                # Split unit into policy_id and asset_name
                policy_id = unit[:56]
                asset_name = unit[56:]
                
                script_hash = ScriptHash.from_primitive(policy_id)
                if script_hash not in multi_asset:
                    multi_asset[script_hash] = Asset()
                
                asset_name_bytes = bytes.fromhex(asset_name) if asset_name else b""
                multi_asset[script_hash][AssetName(asset_name_bytes)] = quantity
            
            value = Value(coin, multi_asset)
            
            # Create transaction input
            tx_input = TransactionInput(
                TransactionId.from_primitive(tx_hash),
                output_index,
            )
            
            # Parse address
            address = Address.from_primitive(data["address"])
            
            # Create output
            output = TransactionOutput(address, value)
            
            # Handle datum
            if data.get("inline_datum"):
                # Has inline datum
                try:
                    output.datum = RawPlutusData.from_primitive(data["inline_datum"])
                except Exception:
                    pass
            elif data.get("data_hash"):
                # Has datum hash
                output.datum_hash = bytes.fromhex(data["data_hash"])
            
            return UTxO(tx_input, output)
            
        except Exception as e:
            logger.warning("utxo_parse_error", tx_hash=data.get("tx_hash"), error=str(e))
            return None
    
    async def get_utxo(
        self,
        tx_hash: str,
        output_index: int,
    ) -> Optional[UTxO]:
        """Get a specific UTXO."""
        data = await self._request("GET", f"/txs/{tx_hash}/utxos")
        
        if not data:
            return None
        
        # Find the specific output
        for output in data.get("outputs", []):
            if int(output.get("output_index", -1)) == output_index:
                output["tx_hash"] = tx_hash
                return await self._parse_utxo(output)
        
        return None
    
    async def get_datum(self, datum_hash: str) -> Optional[Any]:
        """Get datum by hash."""
        data = await self._request("GET", f"/scripts/datum/{datum_hash}")
        
        if not data:
            return None
        
        try:
            return RawPlutusData.from_primitive(data.get("json_value"))
        except Exception as e:
            logger.warning("datum_parse_error", datum_hash=datum_hash, error=str(e))
            return data.get("json_value")
    
    async def submit_transaction(self, tx: Transaction) -> str:
        """Submit a signed transaction."""
        tx_cbor = tx.to_cbor()
        
        try:
            if not self._client:
                await self.connect()
            
            response = await self._client.post(
                "/tx/submit",
                content=tx_cbor,
                headers={
                    **self.headers,
                    "Content-Type": "application/cbor",
                },
            )
            
            if response.status_code != 200:
                error_data = response.text
                logger.error("tx_submit_failed", error=error_data)
                raise TransactionSubmitError(f"Transaction submission failed: {error_data}")
            
            tx_hash = response.json()
            logger.info("tx_submitted", tx_hash=tx_hash)
            return tx_hash
            
        except httpx.RequestError as e:
            raise TransactionSubmitError(f"Transaction submission request failed: {e}")
    
    async def await_transaction_confirmation(
        self,
        tx_hash: str,
        timeout_seconds: int = 120,
        confirmations: int = 1,
    ) -> bool:
        """Wait for transaction confirmation."""
        start_time = asyncio.get_event_loop().time()
        
        while True:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed > timeout_seconds:
                logger.warning("tx_confirmation_timeout", tx_hash=tx_hash)
                return False
            
            tx_data = await self.get_transaction(tx_hash)
            
            if tx_data and tx_data.get("block"):
                # Transaction is in a block
                current_tip = await self.get_chain_tip()
                tx_block_height = tx_data.get("block_height", 0)
                
                if current_tip.block_height - tx_block_height >= confirmations:
                    logger.info(
                        "tx_confirmed",
                        tx_hash=tx_hash,
                        confirmations=current_tip.block_height - tx_block_height,
                    )
                    return True
            
            await asyncio.sleep(5)  # Poll every 5 seconds
    
    async def get_transaction(self, tx_hash: str) -> Optional[dict]:
        """Get transaction details."""
        return await self._request("GET", f"/txs/{tx_hash}")
    
    async def check_utxo_exists(self, tx_hash: str, output_index: int) -> bool:
        """Check if a UTXO still exists."""
        utxo = await self.get_utxo(tx_hash, output_index)
        return utxo is not None

