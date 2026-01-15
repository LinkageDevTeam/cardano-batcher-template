"""
Pytest configuration and shared fixtures for the test suite.
"""

import asyncio
from datetime import datetime
from typing import Any, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest
from pycardano import (
    Address,
    Network,
    PaymentSigningKey,
    PaymentVerificationKey,
    TransactionId,
    TransactionInput,
    TransactionOutput,
    UTxO,
    Value,
)

from batcher.config import BatcherConfig, NetworkType, NodeProvider
from batcher.core.request import BatchRequest, RequestStatus
from batcher.core.batch import Batch, BatchStatus
from batcher.node.interface import NodeInterface, ProtocolParameters, ChainTip


# ============================================================================
# Event Loop Fixture
# ============================================================================

@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


# ============================================================================
# Configuration Fixtures
# ============================================================================

@pytest.fixture
def test_config() -> BatcherConfig:
    """Create a test configuration."""
    return BatcherConfig(
        network=NetworkType.PREPROD,
        node_provider=NodeProvider.BLOCKFROST,
        blockfrost_project_id="test_project_id",
        batch_size_min=2,
        batch_size_max=5,
        batch_interval_seconds=10,
        request_script_address="addr_test1qz...",
        database_url="sqlite+aiosqlite:///:memory:",
        log_level="DEBUG",
    )


# ============================================================================
# Test Data Generators
# ============================================================================

def generate_test_tx_hash(index: int = 0) -> str:
    """Generate a deterministic test transaction hash."""
    base = "abcd1234" * 8  # 64 chars
    return base[:60] + f"{index:04d}"


def generate_test_address(index: int = 0) -> str:
    """Generate a test address."""
    # Generate a real test address format
    return f"addr_test1qz{'0' * 50}{index:08d}"


@pytest.fixture
def sample_utxo() -> UTxO:
    """Create a sample UTXO for testing."""
    tx_hash = generate_test_tx_hash(0)
    tx_input = TransactionInput(
        TransactionId.from_primitive(tx_hash),
        0
    )
    
    # Create a test address
    test_address = Address.from_primitive(
        "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
    )
    
    output = TransactionOutput(
        test_address,
        Value(5_000_000),  # 5 ADA
    )
    
    return UTxO(tx_input, output)


@pytest.fixture
def sample_utxos() -> List[UTxO]:
    """Create multiple sample UTXOs."""
    test_address = Address.from_primitive(
        "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
    )
    
    utxos = []
    for i in range(5):
        tx_hash = generate_test_tx_hash(i)
        tx_input = TransactionInput(
            TransactionId.from_primitive(tx_hash),
            0
        )
        output = TransactionOutput(
            test_address,
            Value((i + 1) * 5_000_000),  # 5, 10, 15, 20, 25 ADA
        )
        utxos.append(UTxO(tx_input, output))
    
    return utxos


@pytest.fixture
def sample_request() -> BatchRequest:
    """Create a sample batch request."""
    tx_hash = generate_test_tx_hash(0)
    return BatchRequest.from_utxo(
        tx_hash=tx_hash,
        output_index=0,
        value=Value(5_000_000),
        datum={"type": "swap", "requester": "addr_test1..."},
    )


@pytest.fixture
def sample_requests() -> List[BatchRequest]:
    """Create multiple sample batch requests."""
    requests = []
    for i in range(5):
        tx_hash = generate_test_tx_hash(i)
        request = BatchRequest.from_utxo(
            tx_hash=tx_hash,
            output_index=0,
            value=Value((i + 1) * 5_000_000),
        )
        request.request_type = "swap" if i % 2 == 0 else "withdraw"
        request.requester_address = generate_test_address(i)
        requests.append(request)
    return requests


@pytest.fixture
def sample_batch(sample_requests) -> Batch:
    """Create a sample batch with requests."""
    batch = Batch()
    for request in sample_requests[:3]:
        batch.add_request(request)
    return batch


# ============================================================================
# Mock Node Interface
# ============================================================================

class MockNodeInterface(NodeInterface):
    """Mock node interface for testing."""
    
    def __init__(self):
        self.utxos: List[UTxO] = []
        self.submitted_txs: List[str] = []
        self.confirmed_txs: set = set()
        self._connected = False
    
    async def connect(self) -> None:
        self._connected = True
    
    async def disconnect(self) -> None:
        self._connected = False
    
    async def get_protocol_parameters(self) -> ProtocolParameters:
        return ProtocolParameters(
            min_fee_a=44,
            min_fee_b=155381,
            max_tx_size=16384,
            max_val_size=5000,
            key_deposit=2_000_000,
            pool_deposit=500_000_000,
            coins_per_utxo_byte=4310,
            collateral_percentage=150,
            max_collateral_inputs=3,
            price_mem=0.0577,
            price_step=0.0000721,
            max_tx_ex_mem=14_000_000,
            max_tx_ex_steps=10_000_000_000,
        )
    
    async def get_chain_tip(self) -> ChainTip:
        return ChainTip(
            slot=12345678,
            block_hash="abc123" * 10 + "abcd",
            block_height=100000,
            epoch=400,
            epoch_slot=12345,
        )
    
    async def get_utxos_at_address(
        self,
        address: str,
        asset: Optional[str] = None,
    ) -> List[UTxO]:
        return [u for u in self.utxos]
    
    async def get_utxo(
        self,
        tx_hash: str,
        output_index: int,
    ) -> Optional[UTxO]:
        for utxo in self.utxos:
            if (str(utxo.input.transaction_id) == tx_hash and 
                utxo.input.index == output_index):
                return utxo
        return None
    
    async def get_datum(self, datum_hash: str) -> Optional[Any]:
        return {"test": "datum"}
    
    async def submit_transaction(self, tx) -> str:
        tx_hash = tx.transaction_body.hash().hex()
        self.submitted_txs.append(tx_hash)
        self.confirmed_txs.add(tx_hash)
        return tx_hash
    
    async def await_transaction_confirmation(
        self,
        tx_hash: str,
        timeout_seconds: int = 120,
        confirmations: int = 1,
    ) -> bool:
        return tx_hash in self.confirmed_txs
    
    async def get_transaction(self, tx_hash: str) -> Optional[dict]:
        if tx_hash in self.confirmed_txs:
            return {"hash": tx_hash, "block": "confirmed"}
        return None
    
    async def check_utxo_exists(self, tx_hash: str, output_index: int) -> bool:
        return await self.get_utxo(tx_hash, output_index) is not None
    
    def add_utxo(self, utxo: UTxO) -> None:
        """Add a UTXO to the mock."""
        self.utxos.append(utxo)
    
    def remove_utxo(self, tx_hash: str, output_index: int) -> None:
        """Remove a UTXO (simulate spending)."""
        self.utxos = [
            u for u in self.utxos
            if not (str(u.input.transaction_id) == tx_hash and u.input.index == output_index)
        ]


@pytest.fixture
def mock_node() -> MockNodeInterface:
    """Create a mock node interface."""
    return MockNodeInterface()


@pytest.fixture
def mock_node_with_utxos(mock_node, sample_utxos) -> MockNodeInterface:
    """Create a mock node with sample UTXOs."""
    for utxo in sample_utxos:
        mock_node.add_utxo(utxo)
    return mock_node


# ============================================================================
# Test Signer
# ============================================================================

@pytest.fixture
def test_signer():
    """Create a test signer with a random key."""
    from batcher.tx.signer import TransactionSigner, generate_test_key
    return generate_test_key()


