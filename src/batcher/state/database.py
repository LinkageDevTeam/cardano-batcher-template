"""
Database module for persistent state storage.

Uses SQLAlchemy for async database operations with SQLite by default.
"""

import json
from datetime import datetime
from typing import List, Optional

import structlog
from sqlalchemy import Column, String, Integer, DateTime, Text, create_engine, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base

from batcher.config import BatcherConfig, get_config
from batcher.core.request import BatchRequest, RequestStatus
from batcher.core.batch import Batch, BatchStatus

logger = structlog.get_logger(__name__)

Base = declarative_base()


class RequestRecord(Base):
    """Database model for batch requests."""
    
    __tablename__ = "requests"
    
    request_id = Column(String(100), primary_key=True)
    tx_hash = Column(String(64), nullable=False)
    output_index = Column(Integer, nullable=False)
    
    ada_amount = Column(Integer, nullable=False)
    assets_json = Column(Text, nullable=True)  # JSON encoded
    
    requester_address = Column(String(150), nullable=True)
    request_type = Column(String(50), nullable=True)
    request_params_json = Column(Text, nullable=True)  # JSON encoded
    datum_json = Column(Text, nullable=True)
    
    status = Column(String(20), nullable=False, default="pending")
    batch_id = Column(String(50), nullable=True)
    
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BatchRecord(Base):
    """Database model for batches."""
    
    __tablename__ = "batches"
    
    batch_id = Column(String(50), primary_key=True)
    status = Column(String(20), nullable=False, default="collecting")
    
    transaction_hash = Column(String(64), nullable=True)
    transaction_cbor = Column(Text, nullable=True)
    
    error_message = Column(Text, nullable=True)
    retry_count = Column(Integer, default=0)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    submitted_at = Column(DateTime, nullable=True)
    confirmed_at = Column(DateTime, nullable=True)


class Database:
    """
    Async database interface for state persistence.
    
    Provides methods to save and load requests and batches.
    """
    
    def __init__(self, config: Optional[BatcherConfig] = None):
        """
        Initialize database connection.
        
        Args:
            config: Batcher configuration
        """
        self.config = config or get_config()
        self._engine = None
        self._session_factory = None
    
    async def connect(self) -> None:
        """Initialize database connection and create tables."""
        self._engine = create_async_engine(
            self.config.database_url,
            echo=False,
        )
        
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
        
        # Create tables
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        logger.info("database_connected", url=self.config.database_url.split("///")[0])
    
    async def disconnect(self) -> None:
        """Close database connection."""
        if self._engine:
            await self._engine.dispose()
            logger.info("database_disconnected")
    
    def _get_session(self) -> AsyncSession:
        """Get a new database session."""
        if not self._session_factory:
            raise RuntimeError("Database not connected")
        return self._session_factory()
    
    # Request operations
    
    async def save_request(self, request: BatchRequest) -> None:
        """Save or update a request."""
        async with self._get_session() as session:
            # Check if exists
            existing = await session.get(RequestRecord, request.request_id)
            
            if existing:
                # Update existing
                existing.status = request.status.value
                existing.batch_id = request.batch_id
                existing.error_message = request.error_message
                existing.retry_count = request.retry_count
                existing.updated_at = request.updated_at
            else:
                # Create new
                record = RequestRecord(
                    request_id=request.request_id,
                    tx_hash=str(request.utxo_ref.transaction_id),
                    output_index=request.utxo_ref.index,
                    ada_amount=request.ada_amount,
                    requester_address=request.requester_address,
                    request_type=request.request_type,
                    request_params_json=json.dumps(request.request_params) if request.request_params else None,
                    status=request.status.value,
                    batch_id=request.batch_id,
                    error_message=request.error_message,
                    retry_count=request.retry_count,
                    created_at=request.created_at,
                    updated_at=request.updated_at,
                )
                session.add(record)
            
            await session.commit()
    
    async def save_requests(self, requests: List[BatchRequest]) -> None:
        """Save multiple requests."""
        for request in requests:
            await self.save_request(request)
    
    async def load_request(self, request_id: str) -> Optional[BatchRequest]:
        """Load a request by ID."""
        async with self._get_session() as session:
            record = await session.get(RequestRecord, request_id)
            if not record:
                return None
            return self._record_to_request(record)
    
    async def load_requests_by_status(self, status: RequestStatus) -> List[BatchRequest]:
        """Load all requests with a given status."""
        async with self._get_session() as session:
            result = await session.execute(
                select(RequestRecord).where(RequestRecord.status == status.value)
            )
            records = result.scalars().all()
            return [self._record_to_request(r) for r in records]
    
    async def load_pending_requests(self) -> List[BatchRequest]:
        """Load all pending requests."""
        return await self.load_requests_by_status(RequestStatus.PENDING)
    
    def _record_to_request(self, record: RequestRecord) -> BatchRequest:
        """Convert database record to BatchRequest."""
        from pycardano import TransactionId, TransactionInput, Value
        
        utxo_ref = TransactionInput(
            TransactionId.from_primitive(record.tx_hash),
            record.output_index,
        )
        
        request_params = {}
        if record.request_params_json:
            try:
                request_params = json.loads(record.request_params_json)
            except json.JSONDecodeError:
                pass
        
        return BatchRequest(
            request_id=record.request_id,
            utxo_ref=utxo_ref,
            value=Value(record.ada_amount),
            requester_address=record.requester_address,
            request_type=record.request_type,
            request_params=request_params,
            created_at=record.created_at,
            updated_at=record.updated_at,
            status=RequestStatus(record.status),
            batch_id=record.batch_id,
            error_message=record.error_message,
            retry_count=record.retry_count,
        )
    
    # Batch operations
    
    async def save_batch(self, batch: Batch) -> None:
        """Save or update a batch."""
        async with self._get_session() as session:
            existing = await session.get(BatchRecord, batch.batch_id)
            
            if existing:
                existing.status = batch.status.value
                existing.transaction_hash = batch.transaction_hash
                existing.transaction_cbor = batch.transaction_cbor
                existing.error_message = batch.error_message
                existing.retry_count = batch.retry_count
                existing.updated_at = batch.updated_at
                existing.submitted_at = batch.submitted_at
                existing.confirmed_at = batch.confirmed_at
            else:
                record = BatchRecord(
                    batch_id=batch.batch_id,
                    status=batch.status.value,
                    transaction_hash=batch.transaction_hash,
                    transaction_cbor=batch.transaction_cbor,
                    error_message=batch.error_message,
                    retry_count=batch.retry_count,
                    created_at=batch.created_at,
                    updated_at=batch.updated_at,
                    submitted_at=batch.submitted_at,
                    confirmed_at=batch.confirmed_at,
                )
                session.add(record)
            
            await session.commit()
    
    async def load_batch(self, batch_id: str) -> Optional[Batch]:
        """Load a batch by ID."""
        async with self._get_session() as session:
            record = await session.get(BatchRecord, batch_id)
            if not record:
                return None
            
            # Load requests for this batch
            result = await session.execute(
                select(RequestRecord).where(RequestRecord.batch_id == batch_id)
            )
            request_records = result.scalars().all()
            requests = [self._record_to_request(r) for r in request_records]
            
            return Batch(
                batch_id=record.batch_id,
                requests=requests,
                status=BatchStatus(record.status),
                transaction_hash=record.transaction_hash,
                transaction_cbor=record.transaction_cbor,
                created_at=record.created_at,
                updated_at=record.updated_at,
                submitted_at=record.submitted_at,
                confirmed_at=record.confirmed_at,
                error_message=record.error_message,
                retry_count=record.retry_count,
            )
    
    async def load_active_batches(self) -> List[Batch]:
        """Load all active (non-completed) batches."""
        async with self._get_session() as session:
            result = await session.execute(
                select(BatchRecord).where(
                    BatchRecord.status.not_in([
                        BatchStatus.CONFIRMED.value,
                        BatchStatus.FAILED.value,
                    ])
                )
            )
            records = result.scalars().all()
            
            batches = []
            for record in records:
                batch = await self.load_batch(record.batch_id)
                if batch:
                    batches.append(batch)
            
            return batches


async def init_database(config: Optional[BatcherConfig] = None) -> Database:
    """
    Initialize and connect to the database.
    
    Args:
        config: Batcher configuration
        
    Returns:
        Connected Database instance
    """
    db = Database(config)
    await db.connect()
    return db


