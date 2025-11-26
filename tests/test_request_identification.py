"""
Test suite for request identification functionality.

Tests the ability to identify and parse batch requests on-chain.
This is a core milestone requirement.
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from pycardano import (
    Address,
    TransactionId,
    TransactionInput,
    TransactionOutput,
    UTxO,
    Value,
)

from batcher.core.request import BatchRequest, RequestStatus
from batcher.engine.scanner import RequestScanner, DatumParser, DefaultDatumParser
from batcher.state.request_pool import RequestPool


# ============================================================================
# Test BatchRequest Creation
# ============================================================================

class TestBatchRequestCreation:
    """Tests for creating BatchRequest objects from UTXOs."""
    
    def test_create_request_from_utxo(self):
        """Test creating a request from UTXO data."""
        tx_hash = "abcd1234" * 8
        output_index = 0
        value = Value(10_000_000)  # 10 ADA
        
        request = BatchRequest.from_utxo(
            tx_hash=tx_hash,
            output_index=output_index,
            value=value,
            datum={"type": "swap"},
        )
        
        assert request.request_id == f"{tx_hash}#{output_index}"
        assert request.ada_amount == 10_000_000
        assert request.datum == {"type": "swap"}
        assert request.status == RequestStatus.PENDING
    
    def test_request_id_format(self):
        """Test that request ID follows tx_hash#index format."""
        tx_hash = "ef" * 32
        output_index = 5
        
        request = BatchRequest.from_utxo(
            tx_hash=tx_hash,
            output_index=output_index,
            value=Value(5_000_000),
        )
        
        assert "#" in request.request_id
        parts = request.request_id.split("#")
        assert len(parts) == 2
        assert parts[0] == tx_hash
        assert parts[1] == "5"
    
    def test_request_equality(self):
        """Test that requests are equal based on request_id."""
        tx_hash = "ab" * 32
        
        request1 = BatchRequest.from_utxo(tx_hash, 0, Value(5_000_000))
        request2 = BatchRequest.from_utxo(tx_hash, 0, Value(10_000_000))  # Different value
        request3 = BatchRequest.from_utxo(tx_hash, 1, Value(5_000_000))  # Different index
        
        assert request1 == request2  # Same tx_hash#index
        assert request1 != request3  # Different index
    
    def test_request_hash(self):
        """Test that requests can be used in sets."""
        tx_hash = "cd" * 32
        
        request1 = BatchRequest.from_utxo(tx_hash, 0, Value(5_000_000))
        request2 = BatchRequest.from_utxo(tx_hash, 0, Value(10_000_000))
        request3 = BatchRequest.from_utxo(tx_hash, 1, Value(5_000_000))
        
        request_set = {request1, request2, request3}
        assert len(request_set) == 2  # request1 and request2 are the same
    
    def test_request_serialization(self):
        """Test request to_dict serialization."""
        tx_hash = "de" * 32
        request = BatchRequest.from_utxo(
            tx_hash=tx_hash,
            output_index=0,
            value=Value(5_000_000),
        )
        request.requester_address = "addr_test1..."
        request.request_type = "swap"
        
        data = request.to_dict()
        
        assert "request_id" in data
        assert "utxo_ref" in data
        assert "ada_amount" in data
        assert data["ada_amount"] == 5_000_000
        assert data["status"] == "pending"


# ============================================================================
# Test Datum Parsing
# ============================================================================

class TestDatumParsing:
    """Tests for parsing request datums."""
    
    def test_default_parser_accepts_all(self):
        """Test that default parser accepts all datums without filter."""
        parser = DefaultDatumParser()
        
        assert parser.is_valid_request(None) is True
        assert parser.is_valid_request({}) is True
        assert parser.is_valid_request({"any": "data"}) is True
        assert parser.is_valid_request([1, 2, 3]) is True
    
    def test_default_parser_with_tag_filter(self):
        """Test datum filtering by tag."""
        parser = DefaultDatumParser(datum_tag="batch_request")
        
        # Should accept datums with matching tag
        assert parser.is_valid_request({"tag": "batch_request"}) is True
        assert parser.is_valid_request(["batch_request", "data"]) is True
        
        # Should reject datums without matching tag
        assert parser.is_valid_request({"tag": "other"}) is False
        assert parser.is_valid_request(["other_tag"]) is False
        assert parser.is_valid_request({}) is False
    
    def test_parse_dict_datum(self):
        """Test parsing dictionary-format datums."""
        parser = DefaultDatumParser()
        
        datum = {
            "requester": "addr_test1...",
            "type": "swap",
            "amount": 1000,
        }
        
        utxo = MagicMock()
        requester, req_type, params = parser.parse_request_details(datum, utxo)
        
        assert requester == "addr_test1..."
        assert req_type == "swap"
        assert "amount" in params
    
    def test_parse_list_datum(self):
        """Test parsing list-format datums."""
        parser = DefaultDatumParser()
        
        datum = ["batch_request", "addr_test1...", "withdraw", {"extra": "data"}]
        
        utxo = MagicMock()
        requester, req_type, params = parser.parse_request_details(datum, utxo)
        
        assert requester == "addr_test1..."
        assert req_type == "withdraw"


# ============================================================================
# Test Request Scanner
# ============================================================================

class TestRequestScanner:
    """Tests for the request scanner component."""
    
    @pytest.mark.asyncio
    async def test_scan_identifies_requests(self, mock_node_with_utxos):
        """Test that scanner identifies requests at script address."""
        script_address = "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
        
        scanner = RequestScanner(
            node=mock_node_with_utxos,
            script_address=script_address,
        )
        
        requests = await scanner.scan_for_requests()
        
        # Should find all UTXOs at the address
        assert len(requests) == 5
        
        # Each request should have proper structure
        for request in requests:
            assert request.request_id is not None
            assert "#" in request.request_id
            assert request.ada_amount > 0
            assert request.status == RequestStatus.PENDING
    
    @pytest.mark.asyncio
    async def test_scan_for_new_requests(self, mock_node_with_utxos):
        """Test scanning for only new requests."""
        script_address = "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
        
        scanner = RequestScanner(
            node=mock_node_with_utxos,
            script_address=script_address,
        )
        
        # First scan should return all
        first_scan = await scanner.scan_for_new_requests()
        assert len(first_scan) == 5
        
        # Second scan should return none (all known)
        second_scan = await scanner.scan_for_new_requests()
        assert len(second_scan) == 0
        
        # Add a new UTXO
        from tests.conftest import generate_test_tx_hash
        new_tx_hash = generate_test_tx_hash(99)
        new_utxo = UTxO(
            TransactionInput(TransactionId.from_primitive(new_tx_hash), 0),
            TransactionOutput(
                Address.from_primitive(script_address),
                Value(7_000_000),
            )
        )
        mock_node_with_utxos.add_utxo(new_utxo)
        
        # Third scan should find the new one
        third_scan = await scanner.scan_for_new_requests()
        assert len(third_scan) == 1
        assert new_tx_hash in third_scan[0].request_id
    
    @pytest.mark.asyncio
    async def test_check_requests_still_valid(self, mock_node_with_utxos):
        """Test validating that requests still exist on-chain."""
        script_address = "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
        
        scanner = RequestScanner(
            node=mock_node_with_utxos,
            script_address=script_address,
        )
        
        # Get requests
        requests = await scanner.scan_for_requests()
        request_ids = [r.request_id for r in requests]
        
        # All should be valid initially
        valid, spent = await scanner.check_requests_still_valid(request_ids)
        assert len(valid) == 5
        assert len(spent) == 0
        
        # Simulate spending a UTXO
        first_request = requests[0]
        tx_hash, index = first_request.request_id.split("#")
        mock_node_with_utxos.remove_utxo(tx_hash, int(index))
        
        # Now check again
        valid, spent = await scanner.check_requests_still_valid(request_ids)
        assert len(valid) == 4
        assert len(spent) == 1
        assert first_request.request_id in spent
    
    @pytest.mark.asyncio
    async def test_scanner_with_custom_datum_parser(self, mock_node):
        """Test scanner with custom datum parser."""
        
        class CustomParser(DatumParser):
            def is_valid_request(self, datum):
                return datum is not None and datum.get("valid") == True
            
            def parse_request_details(self, datum, utxo):
                return datum.get("requester"), datum.get("type"), {}
        
        script_address = "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
        
        # Add UTXOs with different datums
        valid_utxo = UTxO(
            TransactionInput(TransactionId.from_primitive("ab" * 32), 0),
            TransactionOutput(
                Address.from_primitive(script_address),
                Value(5_000_000),
            )
        )
        valid_utxo.output.datum = {"valid": True, "requester": "addr...", "type": "swap"}
        mock_node.add_utxo(valid_utxo)
        
        invalid_utxo = UTxO(
            TransactionInput(TransactionId.from_primitive("cd" * 32), 0),
            TransactionOutput(
                Address.from_primitive(script_address),
                Value(5_000_000),
            )
        )
        invalid_utxo.output.datum = {"valid": False}
        mock_node.add_utxo(invalid_utxo)
        
        scanner = RequestScanner(
            node=mock_node,
            script_address=script_address,
            datum_parser=CustomParser(),
        )
        
        requests = await scanner.scan_for_requests()
        
        # Should only find the valid one
        assert len(requests) == 1


# ============================================================================
# Test Request Pool
# ============================================================================

class TestRequestPool:
    """Tests for the request pool component."""
    
    @pytest.mark.asyncio
    async def test_add_request(self, sample_request):
        """Test adding a request to the pool."""
        pool = RequestPool()
        
        result = await pool.add_request(sample_request)
        assert result is True
        
        # Should reject duplicates
        result = await pool.add_request(sample_request)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_get_pending_requests(self, sample_requests):
        """Test retrieving pending requests."""
        pool = RequestPool()
        
        for request in sample_requests:
            await pool.add_request(request)
        
        pending = await pool.get_pending_requests()
        assert len(pending) == 5
        
        # Should be sorted by creation time
        for i in range(len(pending) - 1):
            assert pending[i].created_at <= pending[i + 1].created_at
    
    @pytest.mark.asyncio
    async def test_update_request_status(self, sample_request):
        """Test updating request status."""
        pool = RequestPool()
        await pool.add_request(sample_request)
        
        # Update to QUEUED
        result = await pool.update_request_status(
            sample_request.request_id,
            RequestStatus.QUEUED,
        )
        assert result is True
        
        # Check status updated
        request = await pool.get_request(sample_request.request_id)
        assert request.status == RequestStatus.QUEUED
        
        # Pending count should decrease
        pending = await pool.get_pending_requests()
        assert len(pending) == 0
    
    @pytest.mark.asyncio
    async def test_mark_requests_cancelled(self, sample_requests):
        """Test marking requests as cancelled."""
        pool = RequestPool()
        
        for request in sample_requests:
            await pool.add_request(request)
        
        # Cancel first two
        ids_to_cancel = [sample_requests[0].request_id, sample_requests[1].request_id]
        cancelled = await pool.mark_requests_cancelled(ids_to_cancel)
        
        assert cancelled == 2
        
        # Check they are cancelled
        for rid in ids_to_cancel:
            request = await pool.get_request(rid)
            assert request.status == RequestStatus.CANCELLED
        
        # Pending should be reduced
        pending = await pool.get_pending_requests()
        assert len(pending) == 3
    
    @pytest.mark.asyncio
    async def test_pool_stats(self, sample_requests):
        """Test pool statistics."""
        pool = RequestPool()
        
        for request in sample_requests:
            await pool.add_request(request)
        
        stats = await pool.get_stats()
        
        assert stats["total_requests"] == 5
        assert stats["pending"] == 5
        assert stats["queued"] == 0
        assert stats["confirmed"] == 0


# ============================================================================
# Integration Test: Full Request Identification Flow
# ============================================================================

class TestRequestIdentificationFlow:
    """Integration tests for the complete request identification flow."""
    
    @pytest.mark.asyncio
    async def test_full_identification_flow(self, mock_node_with_utxos, test_config):
        """Test complete flow: scan -> identify -> pool."""
        script_address = "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"
        
        # Create components
        scanner = RequestScanner(
            node=mock_node_with_utxos,
            script_address=script_address,
            config=test_config,
        )
        pool = RequestPool(test_config)
        
        # Step 1: Scan for requests
        requests = await scanner.scan_for_new_requests()
        assert len(requests) == 5
        
        # Step 2: Add to pool
        added = await pool.add_requests(requests)
        assert added == 5
        
        # Step 3: Verify pool state
        pending = await pool.get_pending_requests()
        assert len(pending) == 5
        
        # Step 4: Simulate one UTXO being spent
        first_request = requests[0]
        tx_hash, index = first_request.request_id.split("#")
        mock_node_with_utxos.remove_utxo(tx_hash, int(index))
        
        # Step 5: Validate requests
        ids_to_check = [r.request_id for r in requests]
        valid, spent = await scanner.check_requests_still_valid(ids_to_check)
        
        assert len(valid) == 4
        assert len(spent) == 1
        
        # Step 6: Update pool with spent info
        await pool.mark_requests_cancelled(spent)
        
        # Step 7: Verify final state
        pending = await pool.get_pending_requests()
        assert len(pending) == 4
        
        stats = await pool.get_stats()
        assert stats["cancelled"] == 1
        
        print("\n✅ Request Identification Test PASSED")
        print(f"   - Identified {len(requests)} requests on-chain")
        print(f"   - Added to pool: {added}")
        print(f"   - Validated UTXOs: {len(valid)} valid, {len(spent)} spent")
        print(f"   - Final pending: {len(pending)}")

