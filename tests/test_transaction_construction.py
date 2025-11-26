"""
Test suite for transaction construction functionality.

Tests the ability to construct valid batching transactions.
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from pycardano import (
    Address,
    Network,
    PaymentSigningKey,
    PaymentVerificationKey,
    Transaction,
    TransactionBody,
    TransactionId,
    TransactionInput,
    TransactionOutput,
    UTxO,
    Value,
)

from batcher.core.request import BatchRequest, RequestStatus
from batcher.core.batch import Batch, BatchStatus
from batcher.tx.builder import (
    TransactionBuilder,
    TransactionBuildError,
    BatchTransactionSpec,
    DefaultTransactionSpecBuilder,
)
from batcher.tx.signer import TransactionSigner, generate_test_key


# ============================================================================
# Test Transaction Signer
# ============================================================================

class TestTransactionSigner:
    """Tests for transaction signing functionality."""
    
    def test_generate_test_key(self):
        """Test generating a random test key."""
        signer = generate_test_key()
        
        assert signer.is_loaded is True
        assert signer.address is not None
        assert signer.address_str is not None
    
    def test_signer_not_loaded(self, test_config):
        """Test that signer raises error when key not loaded."""
        signer = TransactionSigner(test_config)
        
        assert signer.is_loaded is False
        
        with pytest.raises(RuntimeError, match="No signing key loaded"):
            # Create a dummy transaction body
            tx_body = TransactionBody(inputs=[], outputs=[], fee=0)
            signer.sign_transaction(tx_body)
    
    def test_sign_transaction(self):
        """Test signing a transaction."""
        signer = generate_test_key()
        
        # Create a simple transaction body
        tx_input = TransactionInput(
            TransactionId.from_primitive("ab" * 32),
            0
        )
        tx_output = TransactionOutput(
            signer.address,
            Value(1_000_000),
        )
        
        tx_body = TransactionBody(
            inputs=[tx_input],
            outputs=[tx_output],
            fee=200_000,
        )
        
        # Sign
        signed_tx = signer.sign_transaction(tx_body)
        
        assert isinstance(signed_tx, Transaction)
        assert signed_tx.transaction_body == tx_body
        assert signed_tx.transaction_witness_set is not None
        assert signed_tx.transaction_witness_set.vkey_witnesses is not None
        assert len(signed_tx.transaction_witness_set.vkey_witnesses) == 1


# ============================================================================
# Test Transaction Spec Builder
# ============================================================================

class TestTransactionSpecBuilder:
    """Tests for transaction specification building."""
    
    def test_default_spec_builder(self, sample_batch, test_signer):
        """Test default transaction spec builder."""
        from batcher.node.interface import ProtocolParameters
        
        builder = DefaultTransactionSpecBuilder()
        protocol_params = ProtocolParameters(
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
        
        # Set requester addresses for the requests
        for request in sample_batch.requests:
            request.requester_address = str(test_signer.address)
        
        spec = builder.build_spec(
            sample_batch,
            test_signer.address,
            protocol_params,
        )
        
        assert isinstance(spec, BatchTransactionSpec)
        assert len(spec.inputs) == sample_batch.size
        assert len(spec.outputs) >= 1


# ============================================================================
# Test Transaction Builder
# ============================================================================

class TestTransactionBuilder:
    """Tests for the main transaction builder."""
    
    @pytest.mark.asyncio
    async def test_build_batch_transaction(
        self, 
        mock_node,
        sample_batch,
        test_signer,
    ):
        """Test building a batch transaction."""
        # Add batcher UTXOs to mock node
        batcher_utxo = UTxO(
            TransactionInput(TransactionId.from_primitive("ff" * 32), 0),
            TransactionOutput(test_signer.address, Value(100_000_000)),
        )
        mock_node.add_utxo(batcher_utxo)
        
        # Set requester addresses
        for request in sample_batch.requests:
            request.requester_address = str(test_signer.address)
        
        builder = TransactionBuilder(
            node=mock_node,
            signer=test_signer,
        )
        
        # Build transaction
        tx = await builder.build_batch_transaction(sample_batch)
        
        assert isinstance(tx, Transaction)
        assert tx.transaction_body is not None
        assert tx.transaction_witness_set is not None
    
    @pytest.mark.asyncio
    async def test_build_fails_without_key(self, mock_node, sample_batch, test_config):
        """Test that building fails without signing key."""
        signer = TransactionSigner(test_config)  # Not loaded
        
        builder = TransactionBuilder(
            node=mock_node,
            signer=signer,
        )
        
        with pytest.raises(TransactionBuildError, match="Signer key not loaded"):
            await builder.build_batch_transaction(sample_batch)
    
    @pytest.mark.asyncio
    async def test_build_fails_for_empty_batch(
        self,
        mock_node,
        test_signer,
    ):
        """Test that building fails for empty batch."""
        builder = TransactionBuilder(
            node=mock_node,
            signer=test_signer,
        )
        
        empty_batch = Batch()
        
        with pytest.raises(TransactionBuildError, match="empty batch"):
            await builder.build_batch_transaction(empty_batch)
    
    @pytest.mark.asyncio
    async def test_build_with_custom_spec_builder(
        self,
        mock_node,
        sample_batch,
        test_signer,
    ):
        """Test building with a custom spec builder."""
        
        class CustomSpecBuilder:
            def build_spec(self, batch, batcher_address, protocol_params):
                # Custom logic that creates specific outputs
                outputs = [
                    TransactionOutput(batcher_address, Value(1_000_000))
                    for _ in batch.requests
                ]
                
                inputs = []
                for request in batch.requests:
                    inputs.append(UTxO(
                        request.utxo_ref,
                        TransactionOutput(batcher_address, request.value),
                    ))
                
                return BatchTransactionSpec(
                    inputs=inputs,
                    outputs=outputs,
                )
        
        # Add batcher UTXOs
        batcher_utxo = UTxO(
            TransactionInput(TransactionId.from_primitive("ff" * 32), 0),
            TransactionOutput(test_signer.address, Value(100_000_000)),
        )
        mock_node.add_utxo(batcher_utxo)
        
        builder = TransactionBuilder(
            node=mock_node,
            signer=test_signer,
            spec_builder=CustomSpecBuilder(),
        )
        
        tx = await builder.build_batch_transaction(sample_batch)
        
        # Should have outputs equal to batch size
        assert len(tx.transaction_body.outputs) >= sample_batch.size


# ============================================================================
# Integration Test: Full Transaction Flow
# ============================================================================

class TestTransactionFlow:
    """Integration tests for the complete transaction flow."""
    
    @pytest.mark.asyncio
    async def test_full_transaction_flow(self, mock_node, test_config):
        """Test complete flow: requests -> batch -> transaction -> submit."""
        from batcher.engine.matcher import RequestMatcher
        from batcher.state.request_pool import RequestPool
        from tests.conftest import generate_test_tx_hash
        
        # Generate test signer
        signer = generate_test_key()
        
        # Add batcher UTXOs
        batcher_utxo = UTxO(
            TransactionInput(TransactionId.from_primitive("ff" * 32), 0),
            TransactionOutput(signer.address, Value(100_000_000)),  # 100 ADA for fees
        )
        mock_node.add_utxo(batcher_utxo)
        
        # Create requests
        requests = []
        for i in range(4):
            tx_hash = generate_test_tx_hash(i)
            request = BatchRequest.from_utxo(
                tx_hash=tx_hash,
                output_index=0,
                value=Value((i + 1) * 5_000_000),
            )
            request.requester_address = str(signer.address)
            requests.append(request)
        
        # Add request UTXOs to mock node
        for request in requests:
            utxo = UTxO(
                request.utxo_ref,
                TransactionOutput(signer.address, request.value),
            )
            mock_node.add_utxo(utxo)
        
        # Step 1: Add to pool
        pool = RequestPool(test_config)
        for request in requests:
            await pool.add_request(request)
        
        # Step 2: Create batch
        matcher = RequestMatcher(config=test_config)
        pending = await pool.get_pending_requests()
        batches = matcher.create_batches(pending)
        
        assert len(batches) >= 1
        batch = batches[0]
        
        # Step 3: Build transaction
        builder = TransactionBuilder(
            node=mock_node,
            signer=signer,
            config=test_config,
        )
        
        tx = await builder.build_batch_transaction(batch)
        
        assert tx is not None
        assert tx.transaction_body is not None
        
        # Step 4: Submit transaction
        tx_hash = await mock_node.submit_transaction(tx)
        
        assert tx_hash is not None
        assert tx_hash in mock_node.submitted_txs
        
        # Step 5: Confirm
        confirmed = await mock_node.await_transaction_confirmation(tx_hash)
        assert confirmed is True
        
        print("\n✅ Transaction Flow Test PASSED")
        print(f"   - Created batch with {batch.size} requests")
        print(f"   - Built transaction: {tx_hash[:16]}...")
        print(f"   - Transaction submitted and confirmed")
    
    @pytest.mark.asyncio
    async def test_transaction_contains_all_inputs(self, mock_node, test_signer):
        """Test that transaction contains all batch request UTXOs as inputs."""
        from tests.conftest import generate_test_tx_hash
        
        # Add batcher UTXO for fees
        batcher_utxo = UTxO(
            TransactionInput(TransactionId.from_primitive("ff" * 32), 0),
            TransactionOutput(test_signer.address, Value(100_000_000)),
        )
        mock_node.add_utxo(batcher_utxo)
        
        # Create batch with specific requests
        batch = Batch()
        request_utxo_refs = []
        
        for i in range(3):
            tx_hash = generate_test_tx_hash(i + 100)
            request = BatchRequest.from_utxo(
                tx_hash=tx_hash,
                output_index=0,
                value=Value(5_000_000),
            )
            request.requester_address = str(test_signer.address)
            batch.add_request(request)
            request_utxo_refs.append(f"{tx_hash}#0")
            
            # Add to mock node
            utxo = UTxO(
                request.utxo_ref,
                TransactionOutput(test_signer.address, request.value),
            )
            mock_node.add_utxo(utxo)
        
        # Build transaction
        builder = TransactionBuilder(
            node=mock_node,
            signer=test_signer,
        )
        
        tx = await builder.build_batch_transaction(batch)
        
        # Check all request UTXOs are in inputs
        tx_input_refs = [
            f"{inp.transaction_id}#{inp.index}"
            for inp in tx.transaction_body.inputs
        ]
        
        for ref in request_utxo_refs:
            assert ref in tx_input_refs, f"Request UTXO {ref} not in transaction inputs"
        
        print("\n✅ Transaction Input Verification PASSED")
        print(f"   - Transaction contains all {len(request_utxo_refs)} request UTXOs")

