"""
Request Pool - manages pending batch requests.

Tracks requests from identification through processing,
handles request lifecycle and provides query capabilities.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Set

import structlog

from batcher.core.request import BatchRequest, RequestStatus
from batcher.core.batch import Batch, BatchStatus
from batcher.config import BatcherConfig, get_config

logger = structlog.get_logger(__name__)


class RequestPool:
    """
    Manages the pool of pending batch requests.
    
    Responsibilities:
    - Track all identified requests
    - Manage request lifecycle (pending -> queued -> processed)
    - Detect and handle spent/cancelled requests
    - Provide requests for batch formation
    
    Thread-safe for concurrent access.
    """
    
    def __init__(self, config: Optional[BatcherConfig] = None):
        """
        Initialize the request pool.
        
        Args:
            config: Batcher configuration
        """
        self.config = config or get_config()
        
        # Request storage by ID
        self._requests: Dict[str, BatchRequest] = {}
        
        # Index by status for efficient queries
        self._by_status: Dict[RequestStatus, Set[str]] = {
            status: set() for status in RequestStatus
        }
        
        # Active batches
        self._batches: Dict[str, Batch] = {}
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
        # Statistics
        self._stats = {
            "total_received": 0,
            "total_processed": 0,
            "total_failed": 0,
            "total_cancelled": 0,
        }
    
    async def add_request(self, request: BatchRequest) -> bool:
        """
        Add a new request to the pool.
        
        Args:
            request: The request to add
            
        Returns:
            True if added, False if already exists
        """
        async with self._lock:
            if request.request_id in self._requests:
                logger.debug("request_already_exists", request_id=request.request_id)
                return False
            
            self._requests[request.request_id] = request
            self._by_status[request.status].add(request.request_id)
            self._stats["total_received"] += 1
            
            logger.info(
                "request_added",
                request_id=request.request_id,
                ada_amount=request.ada_amount,
            )
            return True
    
    async def add_requests(self, requests: List[BatchRequest]) -> int:
        """
        Add multiple requests to the pool.
        
        Args:
            requests: List of requests to add
            
        Returns:
            Number of requests successfully added
        """
        added = 0
        for request in requests:
            if await self.add_request(request):
                added += 1
        return added
    
    async def get_request(self, request_id: str) -> Optional[BatchRequest]:
        """Get a request by ID."""
        async with self._lock:
            return self._requests.get(request_id)
    
    async def update_request_status(
        self,
        request_id: str,
        new_status: RequestStatus,
        error_message: Optional[str] = None,
    ) -> bool:
        """
        Update a request's status.
        
        Args:
            request_id: ID of the request
            new_status: New status to set
            error_message: Optional error message for failed status
            
        Returns:
            True if updated, False if request not found
        """
        async with self._lock:
            request = self._requests.get(request_id)
            if not request:
                return False
            
            old_status = request.status
            self._by_status[old_status].discard(request_id)
            
            request.status = new_status
            request.updated_at = datetime.utcnow()
            if error_message:
                request.error_message = error_message
            
            self._by_status[new_status].add(request_id)
            
            # Update stats
            if new_status == RequestStatus.CONFIRMED:
                self._stats["total_processed"] += 1
            elif new_status == RequestStatus.FAILED:
                self._stats["total_failed"] += 1
            elif new_status == RequestStatus.CANCELLED:
                self._stats["total_cancelled"] += 1
            
            logger.debug(
                "request_status_updated",
                request_id=request_id,
                old_status=old_status.value,
                new_status=new_status.value,
            )
            return True
    
    async def get_pending_requests(
        self,
        limit: Optional[int] = None,
        min_ada: Optional[int] = None,
        request_type: Optional[str] = None,
    ) -> List[BatchRequest]:
        """
        Get pending requests ready for batching.
        
        Args:
            limit: Maximum number of requests to return
            min_ada: Minimum ADA amount filter
            request_type: Filter by request type
            
        Returns:
            List of pending requests
        """
        async with self._lock:
            pending_ids = self._by_status[RequestStatus.PENDING]
            
            requests = []
            for request_id in pending_ids:
                request = self._requests[request_id]
                
                # Apply filters
                if min_ada and request.ada_amount < min_ada:
                    continue
                if request_type and request.request_type != request_type:
                    continue
                
                requests.append(request)
                
                if limit and len(requests) >= limit:
                    break
            
            # Sort by creation time (oldest first)
            requests.sort(key=lambda r: r.created_at)
            
            return requests
    
    async def get_requests_by_status(self, status: RequestStatus) -> List[BatchRequest]:
        """Get all requests with a given status."""
        async with self._lock:
            request_ids = self._by_status[status]
            return [self._requests[rid] for rid in request_ids if rid in self._requests]
    
    async def remove_request(self, request_id: str) -> Optional[BatchRequest]:
        """
        Remove a request from the pool.
        
        Args:
            request_id: ID of the request to remove
            
        Returns:
            The removed request, or None if not found
        """
        async with self._lock:
            request = self._requests.pop(request_id, None)
            if request:
                self._by_status[request.status].discard(request_id)
                logger.debug("request_removed", request_id=request_id)
            return request
    
    async def mark_requests_cancelled(self, request_ids: List[str]) -> int:
        """
        Mark multiple requests as cancelled (UTXO spent elsewhere).
        
        Args:
            request_ids: List of request IDs to cancel
            
        Returns:
            Number of requests cancelled
        """
        cancelled = 0
        for request_id in request_ids:
            if await self.update_request_status(request_id, RequestStatus.CANCELLED):
                cancelled += 1
        
        if cancelled > 0:
            logger.info("requests_cancelled", count=cancelled)
        
        return cancelled
    
    # Batch management
    
    async def create_batch(self) -> Batch:
        """Create a new batch."""
        async with self._lock:
            batch = Batch()
            self._batches[batch.batch_id] = batch
            logger.debug("batch_created", batch_id=batch.batch_id)
            return batch
    
    async def get_batch(self, batch_id: str) -> Optional[Batch]:
        """Get a batch by ID."""
        async with self._lock:
            return self._batches.get(batch_id)
    
    async def add_request_to_batch(
        self,
        request_id: str,
        batch_id: str,
    ) -> bool:
        """
        Add a request to a batch.
        
        Args:
            request_id: ID of the request
            batch_id: ID of the batch
            
        Returns:
            True if added, False otherwise
        """
        async with self._lock:
            request = self._requests.get(request_id)
            batch = self._batches.get(batch_id)
            
            if not request or not batch:
                return False
            
            if request.status != RequestStatus.PENDING:
                logger.warning(
                    "cannot_add_non_pending_request",
                    request_id=request_id,
                    status=request.status.value,
                )
                return False
            
            # Update status tracking
            self._by_status[request.status].discard(request_id)
            
            batch.add_request(request)
            
            self._by_status[request.status].add(request_id)
            
            logger.debug(
                "request_added_to_batch",
                request_id=request_id,
                batch_id=batch_id,
            )
            return True
    
    async def finalize_batch(self, batch_id: str) -> Optional[Batch]:
        """
        Finalize a batch for processing.
        
        Args:
            batch_id: ID of the batch to finalize
            
        Returns:
            The finalized batch, or None if not found
        """
        async with self._lock:
            batch = self._batches.get(batch_id)
            if batch and batch.status == BatchStatus.COLLECTING:
                batch.mark_ready()
                logger.info(
                    "batch_finalized",
                    batch_id=batch_id,
                    size=batch.size,
                )
            return batch
    
    async def get_active_batches(self) -> List[Batch]:
        """Get all batches that are not yet confirmed or failed."""
        async with self._lock:
            return [
                batch for batch in self._batches.values()
                if batch.status not in (BatchStatus.CONFIRMED, BatchStatus.FAILED)
            ]
    
    async def cleanup_old_batches(self, max_age_hours: int = 24) -> int:
        """
        Remove old completed/failed batches.
        
        Args:
            max_age_hours: Maximum age of batches to keep
            
        Returns:
            Number of batches cleaned up
        """
        async with self._lock:
            cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
            to_remove = []
            
            for batch_id, batch in self._batches.items():
                if batch.status in (BatchStatus.CONFIRMED, BatchStatus.FAILED):
                    if batch.updated_at < cutoff:
                        to_remove.append(batch_id)
            
            for batch_id in to_remove:
                del self._batches[batch_id]
            
            if to_remove:
                logger.info("batches_cleaned_up", count=len(to_remove))
            
            return len(to_remove)
    
    # Statistics and monitoring
    
    async def get_stats(self) -> dict:
        """Get pool statistics."""
        async with self._lock:
            return {
                "total_requests": len(self._requests),
                "pending": len(self._by_status[RequestStatus.PENDING]),
                "queued": len(self._by_status[RequestStatus.QUEUED]),
                "processing": len(self._by_status[RequestStatus.PROCESSING]),
                "submitted": len(self._by_status[RequestStatus.SUBMITTED]),
                "confirmed": len(self._by_status[RequestStatus.CONFIRMED]),
                "failed": len(self._by_status[RequestStatus.FAILED]),
                "cancelled": len(self._by_status[RequestStatus.CANCELLED]),
                "active_batches": len([
                    b for b in self._batches.values()
                    if b.status not in (BatchStatus.CONFIRMED, BatchStatus.FAILED)
                ]),
                **self._stats,
            }
    
    async def get_request_ids_for_validation(self) -> List[str]:
        """
        Get request IDs that need UTXO validation.
        
        Returns IDs of requests in states where the UTXO should still exist.
        """
        async with self._lock:
            ids = []
            for status in [RequestStatus.PENDING, RequestStatus.QUEUED]:
                ids.extend(self._by_status[status])
            return list(ids)

