"""
Batch Request model.

Represents a single batching request identified on-chain.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pycardano import TransactionInput, Value


class RequestStatus(str, Enum):
    """Status of a batch request."""
    PENDING = "pending"           # Request identified, waiting to be batched
    QUEUED = "queued"             # Request assigned to a batch
    PROCESSING = "processing"     # Batch transaction being constructed
    SUBMITTED = "submitted"       # Transaction submitted to network
    CONFIRMED = "confirmed"       # Transaction confirmed on-chain
    FAILED = "failed"             # Request processing failed
    CANCELLED = "cancelled"       # Request cancelled (UTXO spent elsewhere)


@dataclass
class BatchRequest:
    """
    Represents a single batch request identified on-chain.
    
    A batch request corresponds to a UTXO at the batcher's script address
    with a datum that contains the request details.
    
    Attributes:
        request_id: Unique identifier for the request (typically tx_hash#index)
        utxo_ref: Reference to the UTXO containing the request
        value: ADA and native tokens locked in the request
        datum: Decoded datum containing request parameters
        datum_hash: Hash of the datum (if hash-only)
        requester_address: Address to receive the result
        created_at: Timestamp when request was identified
        status: Current processing status
        batch_id: ID of the batch this request is assigned to
        metadata: Additional metadata for the request
    """
    
    # Core identifiers
    request_id: str
    utxo_ref: TransactionInput
    
    # UTXO contents
    value: Value
    datum: Optional[Any] = None
    datum_hash: Optional[str] = None
    
    # Request details (parsed from datum)
    requester_address: Optional[str] = None
    request_type: Optional[str] = None  # e.g., "swap", "withdraw", "transfer"
    request_params: dict = field(default_factory=dict)
    
    # Tracking
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    status: RequestStatus = RequestStatus.PENDING
    
    # Batch assignment
    batch_id: Optional[str] = None
    
    # Additional metadata
    metadata: dict = field(default_factory=dict)
    
    # Error tracking
    error_message: Optional[str] = None
    retry_count: int = 0
    
    def __post_init__(self):
        """Validate and normalize after initialization."""
        if isinstance(self.status, str):
            self.status = RequestStatus(self.status)
    
    @classmethod
    def from_utxo(
        cls,
        tx_hash: str,
        output_index: int,
        value: Value,
        datum: Optional[Any] = None,
        datum_hash: Optional[str] = None,
    ) -> "BatchRequest":
        """
        Create a BatchRequest from UTXO data.
        
        Args:
            tx_hash: Transaction hash containing the UTXO
            output_index: Index of the output in the transaction
            value: Value locked in the UTXO
            datum: Decoded datum (if inline or resolved)
            datum_hash: Datum hash (if hash-only)
            
        Returns:
            New BatchRequest instance
        """
        from pycardano import TransactionId
        
        request_id = f"{tx_hash}#{output_index}"
        utxo_ref = TransactionInput(
            TransactionId.from_primitive(tx_hash),
            output_index
        )
        
        return cls(
            request_id=request_id,
            utxo_ref=utxo_ref,
            value=value,
            datum=datum,
            datum_hash=datum_hash,
        )
    
    def mark_queued(self, batch_id: str) -> None:
        """Mark request as queued in a batch."""
        self.status = RequestStatus.QUEUED
        self.batch_id = batch_id
        self.updated_at = datetime.utcnow()
    
    def mark_processing(self) -> None:
        """Mark request as being processed."""
        self.status = RequestStatus.PROCESSING
        self.updated_at = datetime.utcnow()
    
    def mark_submitted(self) -> None:
        """Mark request as submitted."""
        self.status = RequestStatus.SUBMITTED
        self.updated_at = datetime.utcnow()
    
    def mark_confirmed(self) -> None:
        """Mark request as confirmed."""
        self.status = RequestStatus.CONFIRMED
        self.updated_at = datetime.utcnow()
    
    def mark_failed(self, error: str) -> None:
        """Mark request as failed with error message."""
        self.status = RequestStatus.FAILED
        self.error_message = error
        self.retry_count += 1
        self.updated_at = datetime.utcnow()
    
    def mark_cancelled(self) -> None:
        """Mark request as cancelled."""
        self.status = RequestStatus.CANCELLED
        self.updated_at = datetime.utcnow()
    
    def can_retry(self, max_retries: int) -> bool:
        """Check if request can be retried."""
        return self.retry_count < max_retries and self.status == RequestStatus.FAILED
    
    def reset_for_retry(self) -> None:
        """Reset request for retry."""
        self.status = RequestStatus.PENDING
        self.batch_id = None
        self.error_message = None
        self.updated_at = datetime.utcnow()
    
    @property
    def ada_amount(self) -> int:
        """Get the ADA amount in lovelace."""
        return self.value.coin
    
    def __hash__(self):
        """Hash based on request ID."""
        return hash(self.request_id)
    
    def __eq__(self, other):
        """Equality based on request ID."""
        if isinstance(other, BatchRequest):
            return self.request_id == other.request_id
        return False
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "request_id": self.request_id,
            "utxo_ref": f"{self.utxo_ref.transaction_id}#{self.utxo_ref.index}",
            "ada_amount": self.ada_amount,
            "requester_address": self.requester_address,
            "request_type": self.request_type,
            "request_params": self.request_params,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "status": self.status.value,
            "batch_id": self.batch_id,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
        }


