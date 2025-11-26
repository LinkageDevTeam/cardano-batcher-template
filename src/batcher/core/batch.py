"""
Batch model.

Represents a collection of requests to be processed in a single transaction.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional

from batcher.core.request import BatchRequest, RequestStatus


class BatchStatus(str, Enum):
    """Status of a batch."""
    COLLECTING = "collecting"     # Still accepting requests
    READY = "ready"               # Ready to process
    BUILDING = "building"         # Transaction being constructed
    SIGNING = "signing"           # Transaction being signed
    SUBMITTED = "submitted"       # Transaction submitted
    CONFIRMED = "confirmed"       # Transaction confirmed
    FAILED = "failed"             # Batch processing failed


@dataclass
class Batch:
    """
    Represents a batch of requests to be processed together.
    
    A batch collects multiple BatchRequests and processes them
    in a single on-chain transaction.
    
    Attributes:
        batch_id: Unique identifier for the batch
        requests: List of requests in this batch
        status: Current processing status
        transaction_hash: Hash of the submitted transaction
        created_at: When the batch was created
        submitted_at: When the transaction was submitted
        confirmed_at: When the transaction was confirmed
    """
    
    batch_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    requests: List[BatchRequest] = field(default_factory=list)
    status: BatchStatus = BatchStatus.COLLECTING
    
    # Transaction info
    transaction_hash: Optional[str] = None
    transaction_cbor: Optional[str] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    submitted_at: Optional[datetime] = None
    confirmed_at: Optional[datetime] = None
    
    # Error tracking
    error_message: Optional[str] = None
    retry_count: int = 0
    
    def __post_init__(self):
        """Validate after initialization."""
        if isinstance(self.status, str):
            self.status = BatchStatus(self.status)
    
    def add_request(self, request: BatchRequest) -> None:
        """
        Add a request to this batch.
        
        Args:
            request: The request to add
        """
        if request not in self.requests:
            request.mark_queued(self.batch_id)
            self.requests.append(request)
            self.updated_at = datetime.utcnow()
    
    def remove_request(self, request: BatchRequest) -> bool:
        """
        Remove a request from this batch.
        
        Args:
            request: The request to remove
            
        Returns:
            True if request was removed, False if not found
        """
        if request in self.requests:
            self.requests.remove(request)
            request.batch_id = None
            request.status = RequestStatus.PENDING
            self.updated_at = datetime.utcnow()
            return True
        return False
    
    @property
    def size(self) -> int:
        """Get the number of requests in this batch."""
        return len(self.requests)
    
    @property
    def total_ada(self) -> int:
        """Get total ADA (in lovelace) across all requests."""
        return sum(r.ada_amount for r in self.requests)
    
    @property
    def is_empty(self) -> bool:
        """Check if batch has no requests."""
        return len(self.requests) == 0
    
    def can_accept_more(self, max_size: int) -> bool:
        """Check if batch can accept more requests."""
        return (
            self.status == BatchStatus.COLLECTING and
            self.size < max_size
        )
    
    def is_ready(self, min_size: int) -> bool:
        """Check if batch is ready to process."""
        return self.size >= min_size
    
    def mark_ready(self) -> None:
        """Mark batch as ready to process."""
        self.status = BatchStatus.READY
        self.updated_at = datetime.utcnow()
    
    def mark_building(self) -> None:
        """Mark batch as building transaction."""
        self.status = BatchStatus.BUILDING
        for request in self.requests:
            request.mark_processing()
        self.updated_at = datetime.utcnow()
    
    def mark_signing(self) -> None:
        """Mark batch as signing transaction."""
        self.status = BatchStatus.SIGNING
        self.updated_at = datetime.utcnow()
    
    def mark_submitted(self, tx_hash: str, tx_cbor: Optional[str] = None) -> None:
        """Mark batch as submitted with transaction hash."""
        self.status = BatchStatus.SUBMITTED
        self.transaction_hash = tx_hash
        self.transaction_cbor = tx_cbor
        self.submitted_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        for request in self.requests:
            request.mark_submitted()
    
    def mark_confirmed(self) -> None:
        """Mark batch as confirmed."""
        self.status = BatchStatus.CONFIRMED
        self.confirmed_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        for request in self.requests:
            request.mark_confirmed()
    
    def mark_failed(self, error: str) -> None:
        """Mark batch as failed."""
        self.status = BatchStatus.FAILED
        self.error_message = error
        self.retry_count += 1
        self.updated_at = datetime.utcnow()
        for request in self.requests:
            request.mark_failed(error)
    
    def get_utxo_refs(self) -> list:
        """Get all UTXO references for this batch."""
        return [r.utxo_ref for r in self.requests]
    
    def get_request_ids(self) -> List[str]:
        """Get all request IDs in this batch."""
        return [r.request_id for r in self.requests]
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "batch_id": self.batch_id,
            "status": self.status.value,
            "size": self.size,
            "total_ada_lovelace": self.total_ada,
            "transaction_hash": self.transaction_hash,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "submitted_at": self.submitted_at.isoformat() if self.submitted_at else None,
            "confirmed_at": self.confirmed_at.isoformat() if self.confirmed_at else None,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "requests": [r.to_dict() for r in self.requests],
        }
    
    def __repr__(self) -> str:
        return f"Batch(id={self.batch_id[:8]}..., status={self.status.value}, size={self.size})"

