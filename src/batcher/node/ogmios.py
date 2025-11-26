"""
Ogmios WebSocket adapter for node integration.

Provides blockchain access via Ogmios JSON-RPC/WebSocket interface.
"""

import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional

import structlog
import websockets
from websockets.client import WebSocketClientProtocol

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


class OgmiosAdapter(NodeInterface):
    """
    Ogmios WebSocket adapter.
    
    Implements the NodeInterface using Ogmios's JSON-RPC over WebSocket.
    """
    
    def __init__(self, config: Optional[BatcherConfig] = None):
        """
        Initialize the Ogmios adapter.
        
        Args:
            config: Batcher configuration. Uses global config if not provided.
        """
        self.config = config or get_config()
        self.host = self.config.ogmios_host
        self.port = self.config.ogmios_port
        self._ws: Optional[WebSocketClientProtocol] = None
        self._protocol_params: Optional[ProtocolParameters] = None
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._receive_task: Optional[asyncio.Task] = None
    
    @property
    def ws_url(self) -> str:
        """Get WebSocket URL."""
        return f"ws://{self.host}:{self.port}"
    
    async def connect(self) -> None:
        """Establish WebSocket connection to Ogmios."""
        if self._ws is not None:
            return
        
        try:
            self._ws = await websockets.connect(
                self.ws_url,
                ping_interval=30,
                ping_timeout=10,
            )
            
            # Start receive loop
            self._receive_task = asyncio.create_task(self._receive_loop())
            
            logger.info("ogmios_connected", url=self.ws_url)
            
        except Exception as e:
            raise NodeConnectionError(f"Failed to connect to Ogmios: {e}")
    
    async def disconnect(self) -> None:
        """Close WebSocket connection."""
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        
        if self._ws:
            await self._ws.close()
            self._ws = None
            logger.info("ogmios_disconnected")
    
    async def _receive_loop(self) -> None:
        """Background task to receive WebSocket messages."""
        try:
            async for message in self._ws:
                data = json.loads(message)
                
                # Match response to request
                request_id = data.get("id")
                if request_id and request_id in self._pending_requests:
                    future = self._pending_requests.pop(request_id)
                    if not future.done():
                        if "error" in data:
                            future.set_exception(
                                NodeConnectionError(data["error"].get("message", "Unknown error"))
                            )
                        else:
                            future.set_result(data.get("result"))
                            
        except websockets.ConnectionClosed:
            logger.warning("ogmios_connection_closed")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("ogmios_receive_error", error=str(e))
    
    async def _request(
        self,
        method: str,
        params: Optional[dict] = None,
        timeout: float = 30.0,
    ) -> Any:
        """Send a JSON-RPC request and await response."""
        if not self._ws:
            await self.connect()
        
        request_id = str(uuid.uuid4())
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "id": request_id,
        }
        if params:
            request["params"] = params
        
        # Create future for response
        future = asyncio.get_event_loop().create_future()
        self._pending_requests[request_id] = future
        
        try:
            await self._ws.send(json.dumps(request))
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending_requests.pop(request_id, None)
            raise NodeConnectionError(f"Ogmios request timeout: {method}")
        except Exception as e:
            self._pending_requests.pop(request_id, None)
            raise NodeConnectionError(f"Ogmios request failed: {e}")
    
    async def get_protocol_parameters(self) -> ProtocolParameters:
        """Get current protocol parameters."""
        result = await self._request("queryLedgerState/protocolParameters")
        
        self._protocol_params = ProtocolParameters(
            min_fee_a=result.get("minFeeCoefficient", 44),
            min_fee_b=result.get("minFeeConstant", {}).get("ada", {}).get("lovelace", 155381),
            max_tx_size=result.get("maxTransactionSize", {}).get("bytes", 16384),
            max_val_size=result.get("maxValueSize", {}).get("bytes", 5000),
            key_deposit=result.get("stakeCredentialDeposit", {}).get("ada", {}).get("lovelace", 2000000),
            pool_deposit=result.get("stakePoolDeposit", {}).get("ada", {}).get("lovelace", 500000000),
            coins_per_utxo_byte=result.get("minUtxoDepositCoefficient", 4310),
            collateral_percentage=result.get("collateralPercentage", 150),
            max_collateral_inputs=result.get("maxCollateralInputs", 3),
            price_mem=float(result.get("scriptExecutionPrices", {}).get("memory", "0.0577")),
            price_step=float(result.get("scriptExecutionPrices", {}).get("cpu", "0.0000721")),
            max_tx_ex_mem=result.get("maxExecutionUnitsPerTransaction", {}).get("memory", 14000000),
            max_tx_ex_steps=result.get("maxExecutionUnitsPerTransaction", {}).get("cpu", 10000000000),
            cost_models=result.get("plutusCostModels"),
        )
        
        return self._protocol_params
    
    async def get_chain_tip(self) -> ChainTip:
        """Get current chain tip."""
        result = await self._request("queryNetwork/tip")
        
        return ChainTip(
            slot=result.get("slot", 0),
            block_hash=result.get("id", ""),
            block_height=result.get("height", 0),
            epoch=0,  # Ogmios tip doesn't include epoch directly
            epoch_slot=0,
        )
    
    async def get_utxos_at_address(
        self,
        address: str,
        asset: Optional[str] = None,
    ) -> List[UTxO]:
        """Get UTXOs at an address using Ogmios."""
        result = await self._request(
            "queryLedgerState/utxo",
            {"addresses": [address]},
        )
        
        utxos = []
        for item in result or []:
            utxo = self._parse_ogmios_utxo(item)
            if utxo:
                # Filter by asset if specified
                if asset:
                    # TODO: Implement asset filtering
                    pass
                utxos.append(utxo)
        
        return utxos
    
    def _parse_ogmios_utxo(self, data: dict) -> Optional[UTxO]:
        """Parse Ogmios UTXO format to PyCardano UTxO."""
        try:
            tx_id = data.get("transaction", {}).get("id")
            output_index = data.get("index", 0)
            
            if not tx_id:
                return None
            
            # Parse value
            value_data = data.get("value", {})
            coin = value_data.get("ada", {}).get("lovelace", 0)
            
            # Parse multi-assets
            multi_asset = None
            for policy_id, assets in value_data.items():
                if policy_id == "ada":
                    continue
                
                if multi_asset is None:
                    multi_asset = MultiAsset()
                
                script_hash = ScriptHash.from_primitive(policy_id)
                if script_hash not in multi_asset:
                    multi_asset[script_hash] = Asset()
                
                for asset_name, quantity in assets.items():
                    asset_name_bytes = bytes.fromhex(asset_name) if asset_name else b""
                    multi_asset[script_hash][AssetName(asset_name_bytes)] = quantity
            
            value = Value(coin, multi_asset)
            
            # Create transaction input
            tx_input = TransactionInput(
                TransactionId.from_primitive(tx_id),
                output_index,
            )
            
            # Parse address
            address = Address.from_primitive(data.get("address"))
            
            # Create output
            output = TransactionOutput(address, value)
            
            # Handle datum
            datum_data = data.get("datum")
            if datum_data:
                try:
                    output.datum = RawPlutusData.from_primitive(datum_data)
                except Exception:
                    pass
            
            datum_hash = data.get("datumHash")
            if datum_hash:
                output.datum_hash = bytes.fromhex(datum_hash)
            
            return UTxO(tx_input, output)
            
        except Exception as e:
            logger.warning("ogmios_utxo_parse_error", error=str(e))
            return None
    
    async def get_utxo(
        self,
        tx_hash: str,
        output_index: int,
    ) -> Optional[UTxO]:
        """Get a specific UTXO."""
        result = await self._request(
            "queryLedgerState/utxo",
            {"outputReferences": [{"transaction": {"id": tx_hash}, "index": output_index}]},
        )
        
        if result and len(result) > 0:
            return self._parse_ogmios_utxo(result[0])
        
        return None
    
    async def get_datum(self, datum_hash: str) -> Optional[Any]:
        """Get datum by hash - requires local state query."""
        # Ogmios v6 doesn't have a direct datum query
        # Datums should be obtained from UTxOs with inline datums
        logger.warning("ogmios_datum_query_not_supported", datum_hash=datum_hash)
        return None
    
    async def submit_transaction(self, tx: Transaction) -> str:
        """Submit a signed transaction."""
        tx_cbor = tx.to_cbor().hex()
        
        try:
            result = await self._request(
                "submitTransaction",
                {"transaction": {"cbor": tx_cbor}},
            )
            
            tx_hash = result.get("transaction", {}).get("id")
            if not tx_hash:
                raise TransactionSubmitError("No transaction hash returned")
            
            logger.info("tx_submitted_ogmios", tx_hash=tx_hash)
            return tx_hash
            
        except NodeConnectionError as e:
            raise TransactionSubmitError(str(e))
    
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
                return False
            
            # Check if transaction is in the ledger
            tx = await self.get_transaction(tx_hash)
            if tx:
                logger.info("tx_confirmed_ogmios", tx_hash=tx_hash)
                return True
            
            await asyncio.sleep(5)
    
    async def get_transaction(self, tx_hash: str) -> Optional[dict]:
        """Get transaction details - limited in Ogmios."""
        # Ogmios doesn't have direct transaction queries
        # We check if any UTxOs from the transaction exist
        try:
            result = await self._request(
                "queryLedgerState/utxo",
                {"outputReferences": [{"transaction": {"id": tx_hash}, "index": 0}]},
            )
            
            if result and len(result) > 0:
                return {"id": tx_hash, "confirmed": True}
            return None
            
        except Exception:
            return None
    
    async def check_utxo_exists(self, tx_hash: str, output_index: int) -> bool:
        """Check if a UTXO still exists."""
        utxo = await self.get_utxo(tx_hash, output_index)
        return utxo is not None

