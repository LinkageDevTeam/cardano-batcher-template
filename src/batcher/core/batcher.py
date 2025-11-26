"""
Main Batcher orchestrator.

Coordinates all components to provide a complete batching service.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Callable, List, Optional

import structlog

from batcher.config import BatcherConfig, NodeProvider, get_config
from batcher.core.batch import Batch, BatchStatus
from batcher.core.request import BatchRequest, RequestStatus
from batcher.engine.matcher import RequestMatcher, MatchingStrategy
from batcher.engine.scanner import RequestScanner, DatumParser
from batcher.node.interface import NodeInterface, TransactionSubmitError
from batcher.node.blockfrost import BlockfrostAdapter
from batcher.node.ogmios import OgmiosAdapter
from batcher.state.request_pool import RequestPool
from batcher.state.database import Database
from batcher.tx.builder import TransactionBuilder, TransactionSpecBuilder
from batcher.tx.signer import TransactionSigner

logger = structlog.get_logger(__name__)


class Batcher:
    """
    Main batcher orchestrator.
    
    Coordinates all batcher components:
    - Request scanning and identification
    - Request pooling and state management
    - Batch formation and matching
    - Transaction construction and submission
    - Monitoring and logging
    
    Usage:
        ```python
        batcher = Batcher(script_address="addr1...")
        await batcher.initialize()
        await batcher.start()  # Runs the main loop
        ```
    """
    
    def __init__(
        self,
        script_address: str,
        config: Optional[BatcherConfig] = None,
        node: Optional[NodeInterface] = None,
        datum_parser: Optional[DatumParser] = None,
        matching_strategy: MatchingStrategy = MatchingStrategy.FIFO,
        tx_spec_builder: Optional[TransactionSpecBuilder] = None,
    ):
        """
        Initialize the batcher.
        
        Args:
            script_address: Address to monitor for batch requests
            config: Batcher configuration
            node: Custom node interface (auto-created based on config if not provided)
            datum_parser: Custom datum parser for request interpretation
            matching_strategy: Strategy for grouping requests into batches
            tx_spec_builder: Custom transaction builder
        """
        self.config = config or get_config()
        self.script_address = script_address
        
        # Initialize node interface
        if node:
            self.node = node
        elif self.config.node_provider == NodeProvider.OGMIOS:
            self.node = OgmiosAdapter(self.config)
        else:
            self.node = BlockfrostAdapter(self.config)
        
        # Initialize components (lazy initialization in initialize())
        self._scanner: Optional[RequestScanner] = None
        self._pool: Optional[RequestPool] = None
        self._matcher: Optional[RequestMatcher] = None
        self._signer: Optional[TransactionSigner] = None
        self._tx_builder: Optional[TransactionBuilder] = None
        self._database: Optional[Database] = None
        
        # Configuration for initialization
        self._datum_parser = datum_parser
        self._matching_strategy = matching_strategy
        self._tx_spec_builder = tx_spec_builder
        
        # State
        self._running = False
        self._initialized = False
        self._last_scan_time: Optional[datetime] = None
        self._last_batch_time: Optional[datetime] = None
        
        # Callbacks
        self._on_new_request: Optional[Callable[[BatchRequest], None]] = None
        self._on_batch_created: Optional[Callable[[Batch], None]] = None
        self._on_batch_submitted: Optional[Callable[[Batch, str], None]] = None
        self._on_batch_confirmed: Optional[Callable[[Batch], None]] = None
        self._on_error: Optional[Callable[[Exception], None]] = None
    
    async def initialize(self) -> None:
        """
        Initialize all components.
        
        Must be called before starting the batcher.
        """
        if self._initialized:
            return
        
        logger.info("batcher_initializing", script_address=self.script_address[:30] + "...")
        
        # Connect to node
        await self.node.connect()
        
        # Initialize components
        self._pool = RequestPool(self.config)
        
        self._scanner = RequestScanner(
            node=self.node,
            script_address=self.script_address,
            datum_parser=self._datum_parser,
            config=self.config,
        )
        
        self._matcher = RequestMatcher(
            strategy=self._matching_strategy,
            config=self.config,
        )
        
        self._signer = TransactionSigner(self.config)
        if self.config.wallet_signing_key_path or self.config.wallet_signing_key_cbor:
            self._signer.load_from_config()
        
        self._tx_builder = TransactionBuilder(
            node=self.node,
            signer=self._signer,
            spec_builder=self._tx_spec_builder,
            config=self.config,
        )
        
        # Initialize database if configured
        if self.config.database_url:
            self._database = Database(self.config)
            await self._database.connect()
            
            # Load pending requests from database
            pending = await self._database.load_pending_requests()
            if pending:
                await self._pool.add_requests(pending)
                self._scanner.register_known_requests([r.request_id for r in pending])
                logger.info("loaded_pending_requests", count=len(pending))
        
        self._initialized = True
        logger.info("batcher_initialized")
    
    async def shutdown(self) -> None:
        """Shutdown the batcher and cleanup resources."""
        self._running = False
        
        if self._database:
            await self._database.disconnect()
        
        await self.node.disconnect()
        
        self._initialized = False
        logger.info("batcher_shutdown")
    
    async def start(self, run_once: bool = False) -> None:
        """
        Start the main batcher loop.
        
        Args:
            run_once: If True, run one iteration and stop (for testing)
        """
        if not self._initialized:
            await self.initialize()
        
        self._running = True
        logger.info("batcher_starting")
        
        try:
            while self._running:
                await self._run_cycle()
                
                if run_once:
                    break
                
                # Wait before next cycle
                await asyncio.sleep(self.config.batch_interval_seconds)
                
        except asyncio.CancelledError:
            logger.info("batcher_cancelled")
        except Exception as e:
            logger.error("batcher_error", error=str(e))
            if self._on_error:
                self._on_error(e)
            raise
        finally:
            await self.shutdown()
    
    def stop(self) -> None:
        """Stop the batcher loop."""
        self._running = False
        logger.info("batcher_stopping")
    
    async def _run_cycle(self) -> None:
        """Run one complete batcher cycle."""
        # 1. Scan for new requests
        await self._scan_requests()
        
        # 2. Validate existing requests (check UTXOs still exist)
        await self._validate_requests()
        
        # 3. Create batches if possible
        batches = await self._create_batches()
        
        # 4. Process batches
        for batch in batches:
            await self._process_batch(batch)
    
    async def _scan_requests(self) -> None:
        """Scan for and add new requests."""
        try:
            new_requests = await self._scanner.scan_for_new_requests()
            
            for request in new_requests:
                await self._pool.add_request(request)
                
                if self._database:
                    await self._database.save_request(request)
                
                if self._on_new_request:
                    self._on_new_request(request)
            
            self._last_scan_time = datetime.utcnow()
            
        except Exception as e:
            logger.error("scan_error", error=str(e))
    
    async def _validate_requests(self) -> None:
        """Validate that pending requests still exist on-chain."""
        try:
            request_ids = await self._pool.get_request_ids_for_validation()
            
            if not request_ids:
                return
            
            valid_ids, spent_ids = await self._scanner.check_requests_still_valid(request_ids)
            
            if spent_ids:
                await self._pool.mark_requests_cancelled(spent_ids)
                
                if self._database:
                    for rid in spent_ids:
                        request = await self._pool.get_request(rid)
                        if request:
                            await self._database.save_request(request)
                            
        except Exception as e:
            logger.error("validation_error", error=str(e))
    
    async def _create_batches(self) -> List[Batch]:
        """Create batches from pending requests."""
        pending = await self._pool.get_pending_requests()
        
        if not self._matcher.can_form_batch(pending):
            return []
        
        batches = self._matcher.create_batches(pending)
        
        for batch in batches:
            # Register batch in pool
            for request in batch.requests:
                await self._pool.add_request_to_batch(request.request_id, batch.batch_id)
            
            if self._database:
                await self._database.save_batch(batch)
            
            if self._on_batch_created:
                self._on_batch_created(batch)
        
        return batches
    
    async def _process_batch(self, batch: Batch) -> bool:
        """
        Process a single batch.
        
        Args:
            batch: Batch to process
            
        Returns:
            True if successful, False otherwise
        """
        logger.info(
            "processing_batch",
            batch_id=batch.batch_id[:8] + "...",
            size=batch.size,
        )
        
        try:
            # Mark as building
            batch.mark_building()
            if self._database:
                await self._database.save_batch(batch)
            
            # Build transaction
            tx = await self._tx_builder.build_batch_transaction(batch)
            
            # Mark as signing (already signed by builder)
            batch.mark_signing()
            
            # Submit transaction
            tx_hash = await self.node.submit_transaction(tx)
            
            # Mark as submitted
            batch.mark_submitted(tx_hash, tx.to_cbor().hex())
            if self._database:
                await self._database.save_batch(batch)
                for request in batch.requests:
                    await self._database.save_request(request)
            
            if self._on_batch_submitted:
                self._on_batch_submitted(batch, tx_hash)
            
            logger.info(
                "batch_submitted",
                batch_id=batch.batch_id[:8] + "...",
                tx_hash=tx_hash,
            )
            
            # Wait for confirmation (optional, can be async)
            confirmed = await self.node.await_transaction_confirmation(tx_hash)
            
            if confirmed:
                batch.mark_confirmed()
                if self._database:
                    await self._database.save_batch(batch)
                    for request in batch.requests:
                        await self._database.save_request(request)
                
                if self._on_batch_confirmed:
                    self._on_batch_confirmed(batch)
                
                logger.info(
                    "batch_confirmed",
                    batch_id=batch.batch_id[:8] + "...",
                    tx_hash=tx_hash,
                )
            
            self._last_batch_time = datetime.utcnow()
            return True
            
        except TransactionSubmitError as e:
            batch.mark_failed(str(e))
            if self._database:
                await self._database.save_batch(batch)
            
            logger.error(
                "batch_submit_failed",
                batch_id=batch.batch_id[:8] + "...",
                error=str(e),
            )
            return False
            
        except Exception as e:
            batch.mark_failed(str(e))
            if self._database:
                await self._database.save_batch(batch)
            
            logger.error(
                "batch_processing_failed",
                batch_id=batch.batch_id[:8] + "...",
                error=str(e),
            )
            return False
    
    # Public API methods
    
    async def scan_requests(self) -> List[BatchRequest]:
        """
        Manually trigger a request scan.
        
        Returns:
            List of newly discovered requests
        """
        if not self._initialized:
            raise RuntimeError("Batcher not initialized")
        
        return await self._scanner.scan_for_new_requests()
    
    async def get_pending_requests(self) -> List[BatchRequest]:
        """Get all pending requests in the pool."""
        if not self._initialized:
            raise RuntimeError("Batcher not initialized")
        
        return await self._pool.get_pending_requests()
    
    async def create_batch_manually(
        self,
        request_ids: Optional[List[str]] = None,
    ) -> Optional[Batch]:
        """
        Manually create a batch from specific requests or all pending.
        
        Args:
            request_ids: Specific request IDs to batch (all pending if None)
            
        Returns:
            Created batch or None if not enough requests
        """
        if not self._initialized:
            raise RuntimeError("Batcher not initialized")
        
        if request_ids:
            requests = []
            for rid in request_ids:
                request = await self._pool.get_request(rid)
                if request and request.status == RequestStatus.PENDING:
                    requests.append(request)
        else:
            requests = await self._pool.get_pending_requests()
        
        batches = self._matcher.create_batches(requests)
        
        if batches:
            batch = batches[0]
            for request in batch.requests:
                await self._pool.add_request_to_batch(request.request_id, batch.batch_id)
            return batch
        
        return None
    
    async def process_batch_manually(self, batch: Batch) -> bool:
        """
        Manually process a batch.
        
        Args:
            batch: Batch to process
            
        Returns:
            True if successful
        """
        if not self._initialized:
            raise RuntimeError("Batcher not initialized")
        
        return await self._process_batch(batch)
    
    async def get_stats(self) -> dict:
        """Get batcher statistics."""
        pool_stats = await self._pool.get_stats() if self._pool else {}
        scanner_stats = self._scanner.get_stats() if self._scanner else {}
        
        return {
            "initialized": self._initialized,
            "running": self._running,
            "script_address": self.script_address[:30] + "...",
            "batcher_address": self._signer.address_str[:30] + "..." if self._signer and self._signer.address_str else None,
            "last_scan_time": self._last_scan_time.isoformat() if self._last_scan_time else None,
            "last_batch_time": self._last_batch_time.isoformat() if self._last_batch_time else None,
            "pool": pool_stats,
            "scanner": scanner_stats,
        }
    
    # Callback registration
    
    def on_new_request(self, callback: Callable[[BatchRequest], None]) -> None:
        """Register callback for new request events."""
        self._on_new_request = callback
    
    def on_batch_created(self, callback: Callable[[Batch], None]) -> None:
        """Register callback for batch creation events."""
        self._on_batch_created = callback
    
    def on_batch_submitted(self, callback: Callable[[Batch, str], None]) -> None:
        """Register callback for batch submission events."""
        self._on_batch_submitted = callback
    
    def on_batch_confirmed(self, callback: Callable[[Batch], None]) -> None:
        """Register callback for batch confirmation events."""
        self._on_batch_confirmed = callback
    
    def on_error(self, callback: Callable[[Exception], None]) -> None:
        """Register callback for error events."""
        self._on_error = callback

