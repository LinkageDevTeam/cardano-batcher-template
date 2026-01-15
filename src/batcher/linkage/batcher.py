"""
Linkage Finance Batcher Orchestrator.

Main batcher service for processing Linkage Finance operations.
Coordinates scanning, batching, transaction building, and submission.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional

import structlog

from pycardano import Address

from batcher.config import NodeProvider
from batcher.node.interface import NodeInterface, TransactionSubmitError
from batcher.node.blockfrost import BlockfrostAdapter
from batcher.node.ogmios import OgmiosAdapter
from batcher.tx.signer import TransactionSigner

from batcher.linkage.config import LinkageConfig, LinkageNetwork
from batcher.linkage.types import (
    LinkageRequest,
    LinkageRequestType,
    LinkageBatchResult,
)
from batcher.linkage.scanner import LinkageRequestScanner
from batcher.linkage.builder import (
    LinkageTransactionBuilder,
    LinkageBatchBuilder,
    DepositParams,
    WithdrawParams,
    CollectRoyaltyParams,
)

logger = structlog.get_logger(__name__)


@dataclass
class PendingOperation:
    """Represents a pending Linkage operation to be batched."""
    
    operation_id: str
    operation_type: LinkageRequestType
    fund_token_name: str
    user_address: str
    amount: int
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict = field(default_factory=dict)
    
    # State
    status: str = "pending"  # pending, queued, processing, completed, failed
    error_message: Optional[str] = None
    transaction_hash: Optional[str] = None


class LinkageBatcher:
    """
    Main Linkage Finance Batcher.
    
    Provides a complete batching service for Linkage Finance operations:
    - Monitors for fund UTXOs
    - Accepts operation requests via API
    - Groups operations into batches
    - Builds and submits transactions
    - Tracks operation status
    
    Usage:
        ```python
        config = LinkageConfig.from_env()
        batcher = LinkageBatcher(config)
        
        await batcher.initialize()
        
        # Add an operation request
        batcher.queue_deposit(
            fund_token_name="abc123...",
            user_address="addr_test1...",
            multiple=10,
        )
        
        # Start the main loop
        await batcher.start()
        ```
    """
    
    def __init__(
        self,
        config: LinkageConfig,
        node: Optional[NodeInterface] = None,
    ):
        """
        Initialize the Linkage batcher.
        
        Args:
            config: Linkage configuration
            node: Optional custom node interface
        """
        self.config = config
        
        # Initialize node interface
        if node:
            self.node = node
        elif config.use_ogmios:
            from batcher.config import BatcherConfig, NetworkType
            network_type = (
                NetworkType.MAINNET if config.network.value == "mainnet" 
                else NetworkType.PREPROD
            )
            batcher_config = BatcherConfig(
                network=network_type,
                ogmios_host=config.ogmios_host,
                ogmios_port=config.ogmios_port,
            )
            self.node = OgmiosAdapter(batcher_config)
        else:
            from batcher.config import BatcherConfig, NetworkType
            network_type = (
                NetworkType.MAINNET if config.network.value == "mainnet" 
                else NetworkType.PREPROD
            )
            batcher_config = BatcherConfig(
                network=network_type,
                blockfrost_project_id=config.blockfrost_project_id,
            )
            self.node = BlockfrostAdapter(batcher_config)
        
        # Components (lazy initialization)
        self._scanner: Optional[LinkageRequestScanner] = None
        self._tx_builder: Optional[LinkageTransactionBuilder] = None
        self._batch_builder: Optional[LinkageBatchBuilder] = None
        self._signer: Optional[TransactionSigner] = None
        
        # State
        self._initialized = False
        self._running = False
        
        # Operation queues
        self._pending_deposits: List[PendingOperation] = []
        self._pending_withdrawals: List[PendingOperation] = []
        self._pending_royalty_collections: List[PendingOperation] = []
        
        # Fund cache
        self._fund_cache: Dict[str, LinkageRequest] = {}
        self._last_fund_scan: Optional[datetime] = None
        
        # Statistics
        self._stats = {
            "deposits_processed": 0,
            "withdrawals_processed": 0,
            "royalties_collected": 0,
            "total_transactions": 0,
            "failed_transactions": 0,
        }
        
        # Callbacks
        self._on_operation_completed: Optional[Callable] = None
        self._on_operation_failed: Optional[Callable] = None
    
    async def initialize(self) -> None:
        """
        Initialize all components.
        
        Must be called before starting the batcher.
        """
        if self._initialized:
            return
        
        logger.info(
            "linkage_batcher_initializing",
            network=self.config.network.value,
            fund_address=self.config.fund_address[:30] + "...",
        )
        
        # Validate configuration
        self.config.validate()
        
        # Connect to node
        await self.node.connect()
        
        # Initialize scanner
        self._scanner = LinkageRequestScanner(
            node=self.node,
            config=self.config,
        )
        
        # Initialize signer
        from batcher.config import BatcherConfig, NetworkType
        network_type = (
            NetworkType.MAINNET if self.config.network.value == "mainnet" 
            else NetworkType.PREPROD
        )
        signer_config = BatcherConfig(
            network=network_type,
            wallet_signing_key_path=self.config.signing_key_path,
        )
        self._signer = TransactionSigner(signer_config)
        if self.config.signing_key_path:
            self._signer.load_from_config()
        
        # Initialize transaction builder
        self._tx_builder = LinkageTransactionBuilder(
            node=self.node,
            scanner=self._scanner,
            signer=self._signer,
            config=self.config,
        )
        
        # Initialize batch builder
        self._batch_builder = LinkageBatchBuilder(
            tx_builder=self._tx_builder,
            config=self.config,
        )
        
        # Initial fund scan
        await self._refresh_fund_cache()
        
        self._initialized = True
        logger.info(
            "linkage_batcher_initialized",
            cached_funds=len(self._fund_cache),
        )
    
    async def shutdown(self) -> None:
        """Shutdown the batcher and cleanup resources."""
        self._running = False
        
        await self.node.disconnect()
        
        self._initialized = False
        logger.info("linkage_batcher_shutdown")
    
    async def start(self, run_once: bool = False) -> None:
        """
        Start the main batcher loop.
        
        Args:
            run_once: If True, run one iteration and stop (for testing)
        """
        if not self._initialized:
            await self.initialize()
        
        self._running = True
        logger.info("linkage_batcher_starting")
        
        try:
            while self._running:
                await self._run_cycle()
                
                if run_once:
                    break
                
                await asyncio.sleep(self.config.batch_interval_seconds)
                
        except asyncio.CancelledError:
            logger.info("linkage_batcher_cancelled")
        except Exception as e:
            logger.error("linkage_batcher_error", error=str(e))
            raise
        finally:
            await self.shutdown()
    
    def stop(self) -> None:
        """Stop the batcher loop."""
        self._running = False
        logger.info("linkage_batcher_stopping")
    
    async def _run_cycle(self) -> None:
        """Run one complete batcher cycle."""
        # 1. Refresh fund cache periodically
        await self._refresh_fund_cache()
        
        # 2. Process pending deposits
        await self._process_deposits()
        
        # 3. Process pending withdrawals
        await self._process_withdrawals()
        
        # 4. Process pending royalty collections
        await self._process_royalty_collections()
    
    async def _refresh_fund_cache(self) -> None:
        """Refresh the fund cache."""
        try:
            funds = await self._scanner.scan_fund_utxos()
            
            # Update cache
            self._fund_cache = {
                fund.fund_token_name: fund
                for fund in funds
            }
            
            self._last_fund_scan = datetime.utcnow()
            
            logger.debug(
                "fund_cache_refreshed",
                fund_count=len(self._fund_cache),
            )
            
        except Exception as e:
            logger.error("fund_cache_refresh_failed", error=str(e))
    
    async def _process_deposits(self) -> None:
        """Process pending deposit operations."""
        if not self._pending_deposits:
            return
        
        # Take up to batch_size_max operations
        batch = self._pending_deposits[:self.config.batch_size_max]
        self._pending_deposits = self._pending_deposits[self.config.batch_size_max:]
        
        for op in batch:
            await self._process_single_deposit(op)
    
    async def _process_single_deposit(self, op: PendingOperation) -> None:
        """Process a single deposit operation."""
        logger.info(
            "processing_deposit",
            fund=op.fund_token_name[:16] + "...",
            amount=op.amount,
        )
        
        try:
            # Get fund from cache
            fund = self._fund_cache.get(op.fund_token_name)
            if not fund:
                # Try to fetch it
                fund = await self._scanner.get_fund_by_token_name(op.fund_token_name)
                if not fund:
                    raise ValueError(f"Fund not found: {op.fund_token_name}")
            
            # Build deposit params
            params = DepositParams(
                fund_request=fund,
                user_address=Address.from_primitive(op.user_address),
                multiple=op.amount,
            )
            
            # Build transaction
            tx = await self._tx_builder.build_deposit_transaction(params)
            
            # Sign transaction
            if self._signer.is_loaded:
                signed_tx = self._signer.sign_transaction(tx.transaction_body)
            else:
                # Return unsigned for user to sign
                op.status = "awaiting_signature"
                op.metadata["unsigned_tx_cbor"] = tx.to_cbor().hex()
                return
            
            # Submit transaction
            tx_hash = await self.node.submit_transaction(signed_tx)
            
            # Update operation status
            op.status = "completed"
            op.transaction_hash = tx_hash
            
            self._stats["deposits_processed"] += 1
            self._stats["total_transactions"] += 1
            
            logger.info(
                "deposit_completed",
                fund=op.fund_token_name[:16] + "...",
                tx_hash=tx_hash,
            )
            
            if self._on_operation_completed:
                self._on_operation_completed(op)
            
        except Exception as e:
            op.status = "failed"
            op.error_message = str(e)
            
            self._stats["failed_transactions"] += 1
            
            logger.error(
                "deposit_failed",
                fund=op.fund_token_name[:16] + "...",
                error=str(e),
            )
            
            if self._on_operation_failed:
                self._on_operation_failed(op)
    
    async def _process_withdrawals(self) -> None:
        """Process pending withdrawal operations."""
        if not self._pending_withdrawals:
            return
        
        batch = self._pending_withdrawals[:self.config.batch_size_max]
        self._pending_withdrawals = self._pending_withdrawals[self.config.batch_size_max:]
        
        for op in batch:
            await self._process_single_withdrawal(op)
    
    async def _process_single_withdrawal(self, op: PendingOperation) -> None:
        """Process a single withdrawal operation."""
        logger.info(
            "processing_withdrawal",
            fund=op.fund_token_name[:16] + "...",
            amount=op.amount,
        )
        
        try:
            fund = self._fund_cache.get(op.fund_token_name)
            if not fund:
                fund = await self._scanner.get_fund_by_token_name(op.fund_token_name)
                if not fund:
                    raise ValueError(f"Fund not found: {op.fund_token_name}")
            
            params = WithdrawParams(
                fund_request=fund,
                user_address=Address.from_primitive(op.user_address),
                amount=op.amount,
            )
            
            tx = await self._tx_builder.build_withdraw_transaction(params)
            
            if self._signer.is_loaded:
                signed_tx = self._signer.sign_transaction(tx.transaction_body)
            else:
                op.status = "awaiting_signature"
                op.metadata["unsigned_tx_cbor"] = tx.to_cbor().hex()
                return
            
            tx_hash = await self.node.submit_transaction(signed_tx)
            
            op.status = "completed"
            op.transaction_hash = tx_hash
            
            self._stats["withdrawals_processed"] += 1
            self._stats["total_transactions"] += 1
            
            logger.info(
                "withdrawal_completed",
                fund=op.fund_token_name[:16] + "...",
                tx_hash=tx_hash,
            )
            
            if self._on_operation_completed:
                self._on_operation_completed(op)
            
        except Exception as e:
            op.status = "failed"
            op.error_message = str(e)
            self._stats["failed_transactions"] += 1
            
            logger.error(
                "withdrawal_failed",
                fund=op.fund_token_name[:16] + "...",
                error=str(e),
            )
            
            if self._on_operation_failed:
                self._on_operation_failed(op)
    
    async def _process_royalty_collections(self) -> None:
        """Process pending royalty collection operations."""
        if not self._pending_royalty_collections:
            return
        
        batch = self._pending_royalty_collections[:self.config.batch_size_max]
        self._pending_royalty_collections = self._pending_royalty_collections[
            self.config.batch_size_max:
        ]
        
        for op in batch:
            await self._process_single_royalty_collection(op)
    
    async def _process_single_royalty_collection(self, op: PendingOperation) -> None:
        """Process a single royalty collection operation."""
        logger.info(
            "processing_royalty_collection",
            fund=op.fund_token_name[:16] + "...",
            amount=op.amount,
        )
        
        try:
            fund = self._fund_cache.get(op.fund_token_name)
            if not fund:
                fund = await self._scanner.get_fund_by_token_name(op.fund_token_name)
                if not fund:
                    raise ValueError(f"Fund not found: {op.fund_token_name}")
            
            params = CollectRoyaltyParams(
                fund_request=fund,
                creator_address=Address.from_primitive(op.user_address),
                amount=op.amount,
            )
            
            tx = await self._tx_builder.build_collect_royalty_transaction(params)
            
            # Note: Royalty collection requires the creator's signature
            # The batcher may not have this key, so we return unsigned
            op.status = "awaiting_signature"
            op.metadata["unsigned_tx_cbor"] = tx.to_cbor().hex()
            op.metadata["note"] = "Creator signature required"
            
            logger.info(
                "royalty_collection_built",
                fund=op.fund_token_name[:16] + "...",
                status="awaiting_signature",
            )
            
        except Exception as e:
            op.status = "failed"
            op.error_message = str(e)
            self._stats["failed_transactions"] += 1
            
            logger.error(
                "royalty_collection_failed",
                fund=op.fund_token_name[:16] + "...",
                error=str(e),
            )
            
            if self._on_operation_failed:
                self._on_operation_failed(op)
    
    # ==========================================================================
    # Public API for queuing operations
    # ==========================================================================
    
    def queue_deposit(
        self,
        fund_token_name: str,
        user_address: str,
        multiple: int,
        operation_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> PendingOperation:
        """
        Queue a deposit operation.
        
        Args:
            fund_token_name: Hex-encoded fund token name
            user_address: Bech32 user address
            multiple: Deposit multiple
            operation_id: Optional unique ID
            metadata: Optional additional metadata
            
        Returns:
            PendingOperation object for tracking
        """
        import uuid
        
        op = PendingOperation(
            operation_id=operation_id or str(uuid.uuid4()),
            operation_type=LinkageRequestType.DEPOSIT,
            fund_token_name=fund_token_name,
            user_address=user_address,
            amount=multiple,
            metadata=metadata or {},
        )
        
        self._pending_deposits.append(op)
        
        logger.info(
            "deposit_queued",
            operation_id=op.operation_id,
            fund=fund_token_name[:16] + "...",
            multiple=multiple,
        )
        
        return op
    
    def queue_withdrawal(
        self,
        fund_token_name: str,
        user_address: str,
        amount: int,
        operation_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> PendingOperation:
        """
        Queue a withdrawal operation.
        
        Args:
            fund_token_name: Hex-encoded fund token name
            user_address: Bech32 user address
            amount: Fund tokens to burn
            operation_id: Optional unique ID
            metadata: Optional additional metadata
            
        Returns:
            PendingOperation object for tracking
        """
        import uuid
        
        op = PendingOperation(
            operation_id=operation_id or str(uuid.uuid4()),
            operation_type=LinkageRequestType.WITHDRAW,
            fund_token_name=fund_token_name,
            user_address=user_address,
            amount=amount,
            metadata=metadata or {},
        )
        
        self._pending_withdrawals.append(op)
        
        logger.info(
            "withdrawal_queued",
            operation_id=op.operation_id,
            fund=fund_token_name[:16] + "...",
            amount=amount,
        )
        
        return op
    
    def queue_royalty_collection(
        self,
        fund_token_name: str,
        creator_address: str,
        amount: int,
        operation_id: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> PendingOperation:
        """
        Queue a royalty collection operation.
        
        Args:
            fund_token_name: Hex-encoded fund token name
            creator_address: Bech32 creator address
            amount: Royalty tokens to collect
            operation_id: Optional unique ID
            metadata: Optional additional metadata
            
        Returns:
            PendingOperation object for tracking
        """
        import uuid
        
        op = PendingOperation(
            operation_id=operation_id or str(uuid.uuid4()),
            operation_type=LinkageRequestType.COLLECT_ROYALTY,
            fund_token_name=fund_token_name,
            user_address=creator_address,
            amount=amount,
            metadata=metadata or {},
        )
        
        self._pending_royalty_collections.append(op)
        
        logger.info(
            "royalty_collection_queued",
            operation_id=op.operation_id,
            fund=fund_token_name[:16] + "...",
            amount=amount,
        )
        
        return op
    
    # ==========================================================================
    # Query methods
    # ==========================================================================
    
    def get_operation_status(self, operation_id: str) -> Optional[PendingOperation]:
        """
        Get the status of an operation.
        
        Args:
            operation_id: Operation ID to look up
            
        Returns:
            PendingOperation if found
        """
        for op in (
            self._pending_deposits +
            self._pending_withdrawals +
            self._pending_royalty_collections
        ):
            if op.operation_id == operation_id:
                return op
        return None
    
    def get_pending_count(self) -> Dict[str, int]:
        """Get count of pending operations by type."""
        return {
            "deposits": len(self._pending_deposits),
            "withdrawals": len(self._pending_withdrawals),
            "royalty_collections": len(self._pending_royalty_collections),
        }
    
    def get_cached_funds(self) -> List[Dict]:
        """Get list of cached funds."""
        return [
            fund.to_dict()
            for fund in self._fund_cache.values()
        ]
    
    def get_fund_info(self, fund_token_name: str) -> Optional[Dict]:
        """Get info for a specific fund."""
        fund = self._fund_cache.get(fund_token_name)
        if fund:
            return fund.to_dict()
        return None
    
    def get_stats(self) -> Dict:
        """Get batcher statistics."""
        return {
            **self._stats,
            "pending_deposits": len(self._pending_deposits),
            "pending_withdrawals": len(self._pending_withdrawals),
            "pending_royalty_collections": len(self._pending_royalty_collections),
            "cached_funds": len(self._fund_cache),
            "last_fund_scan": (
                self._last_fund_scan.isoformat()
                if self._last_fund_scan else None
            ),
            "initialized": self._initialized,
            "running": self._running,
        }
    
    # ==========================================================================
    # Callback registration
    # ==========================================================================
    
    def on_operation_completed(self, callback: Callable[[PendingOperation], None]):
        """Register callback for completed operations."""
        self._on_operation_completed = callback
    
    def on_operation_failed(self, callback: Callable[[PendingOperation], None]):
        """Register callback for failed operations."""
        self._on_operation_failed = callback

