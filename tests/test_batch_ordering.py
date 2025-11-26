"""
Test suite for batch ordering and formation functionality.

Tests the ability to group requests into batches using various strategies.
This is a core milestone requirement.
"""

import pytest
from datetime import datetime, timedelta
from typing import List

from pycardano import Value

from batcher.core.request import BatchRequest, RequestStatus
from batcher.core.batch import Batch, BatchStatus
from batcher.engine.matcher import (
    RequestMatcher,
    MatchingStrategy,
    FIFOStrategy,
    ByTypeStrategy,
    ByValueStrategy,
    GreedyStrategy,
    MatchingStrategyBase,
)
from batcher.state.request_pool import RequestPool
from batcher.config import BatcherConfig


# ============================================================================
# Helper Functions
# ============================================================================

def create_request_with_time(
    index: int,
    ada_amount: int = 5_000_000,
    request_type: str = "swap",
    time_offset_seconds: int = 0,
) -> BatchRequest:
    """Create a request with specific creation time."""
    tx_hash = f"{'ab' * 28}{index:08d}"
    request = BatchRequest.from_utxo(
        tx_hash=tx_hash,
        output_index=0,
        value=Value(ada_amount),
    )
    request.request_type = request_type
    request.created_at = datetime.utcnow() + timedelta(seconds=time_offset_seconds)
    return request


def create_requests_batch(count: int, **kwargs) -> List[BatchRequest]:
    """Create multiple requests with sequential creation times."""
    return [
        create_request_with_time(i, time_offset_seconds=i, **kwargs)
        for i in range(count)
    ]


# ============================================================================
# Test Batch Model
# ============================================================================

class TestBatchModel:
    """Tests for the Batch data model."""
    
    def test_batch_creation(self):
        """Test creating an empty batch."""
        batch = Batch()
        
        assert batch.batch_id is not None
        assert batch.status == BatchStatus.COLLECTING
        assert batch.is_empty is True
        assert batch.size == 0
    
    def test_add_request_to_batch(self, sample_request):
        """Test adding a request to a batch."""
        batch = Batch()
        batch.add_request(sample_request)
        
        assert batch.size == 1
        assert sample_request in batch.requests
        assert sample_request.batch_id == batch.batch_id
        assert sample_request.status == RequestStatus.QUEUED
    
    def test_remove_request_from_batch(self, sample_request):
        """Test removing a request from a batch."""
        batch = Batch()
        batch.add_request(sample_request)
        
        result = batch.remove_request(sample_request)
        
        assert result is True
        assert batch.is_empty is True
        assert sample_request.batch_id is None
        assert sample_request.status == RequestStatus.PENDING
    
    def test_batch_total_ada(self, sample_requests):
        """Test calculating total ADA in batch."""
        batch = Batch()
        for request in sample_requests[:3]:
            batch.add_request(request)
        
        # Requests have values: 5, 10, 15 ADA (5M, 10M, 15M lovelace)
        expected_total = 5_000_000 + 10_000_000 + 15_000_000
        assert batch.total_ada == expected_total
    
    def test_batch_ready_check(self, sample_requests):
        """Test checking if batch is ready."""
        batch = Batch()
        
        # Not ready with 1 request (min is 2)
        batch.add_request(sample_requests[0])
        assert batch.is_ready(min_size=2) is False
        
        # Ready with 2 requests
        batch.add_request(sample_requests[1])
        assert batch.is_ready(min_size=2) is True
    
    def test_batch_status_transitions(self, sample_batch):
        """Test batch status transitions."""
        assert sample_batch.status == BatchStatus.COLLECTING
        
        sample_batch.mark_ready()
        assert sample_batch.status == BatchStatus.READY
        
        sample_batch.mark_building()
        assert sample_batch.status == BatchStatus.BUILDING
        for r in sample_batch.requests:
            assert r.status == RequestStatus.PROCESSING
        
        sample_batch.mark_submitted("tx_hash_123")
        assert sample_batch.status == BatchStatus.SUBMITTED
        assert sample_batch.transaction_hash == "tx_hash_123"
        
        sample_batch.mark_confirmed()
        assert sample_batch.status == BatchStatus.CONFIRMED
        for r in sample_batch.requests:
            assert r.status == RequestStatus.CONFIRMED
    
    def test_batch_serialization(self, sample_batch):
        """Test batch to_dict serialization."""
        sample_batch.mark_submitted("abc123")
        
        data = sample_batch.to_dict()
        
        assert "batch_id" in data
        assert "status" in data
        assert "size" in data
        assert data["size"] == sample_batch.size
        assert "requests" in data
        assert len(data["requests"]) == sample_batch.size


# ============================================================================
# Test FIFO Strategy
# ============================================================================

class TestFIFOStrategy:
    """Tests for First-In-First-Out matching strategy."""
    
    def test_fifo_ordering(self):
        """Test that FIFO respects creation time order."""
        # Create requests with explicit ordering
        requests = [
            create_request_with_time(0, time_offset_seconds=3),  # Created later
            create_request_with_time(1, time_offset_seconds=1),  # Created first
            create_request_with_time(2, time_offset_seconds=2),  # Created second
        ]
        
        strategy = FIFOStrategy()
        results = strategy.match(requests, min_batch_size=2, max_batch_size=3)
        
        assert len(results) == 1
        batch = results[0].batch
        
        # Should be ordered by creation time
        assert batch.requests[0].request_id == requests[1].request_id  # First created
        assert batch.requests[1].request_id == requests[2].request_id  # Second created
        assert batch.requests[2].request_id == requests[0].request_id  # Third created
    
    def test_fifo_respects_max_size(self):
        """Test that FIFO respects maximum batch size."""
        requests = create_requests_batch(10)
        
        strategy = FIFOStrategy()
        results = strategy.match(requests, min_batch_size=2, max_batch_size=3)
        
        assert len(results) == 3  # 10 requests, max 3 per batch = 3 batches
        assert results[0].batch.size == 3
        assert results[1].batch.size == 3
        assert results[2].batch.size == 3
        # 1 request unmatched
        assert len(results[-1].unmatched_requests) == 1
    
    def test_fifo_respects_min_size(self):
        """Test that FIFO respects minimum batch size."""
        requests = create_requests_batch(3)
        
        strategy = FIFOStrategy()
        results = strategy.match(requests, min_batch_size=4, max_batch_size=5)
        
        # Not enough for min size
        assert len(results) == 0


# ============================================================================
# Test By Type Strategy
# ============================================================================

class TestByTypeStrategy:
    """Tests for grouping by request type."""
    
    def test_groups_by_type(self):
        """Test that requests are grouped by type."""
        requests = [
            create_request_with_time(0, request_type="swap"),
            create_request_with_time(1, request_type="withdraw"),
            create_request_with_time(2, request_type="swap"),
            create_request_with_time(3, request_type="swap"),
            create_request_with_time(4, request_type="withdraw"),
        ]
        
        strategy = ByTypeStrategy()
        results = strategy.match(requests, min_batch_size=2, max_batch_size=5)
        
        # Should create 2 batches: one for swap, one for withdraw
        assert len(results) == 2
        
        # Check each batch has same type
        for result in results:
            types = {r.request_type for r in result.batch.requests}
            assert len(types) == 1  # All same type
    
    def test_type_ordering_within_group(self):
        """Test FIFO ordering within each type group."""
        requests = [
            create_request_with_time(0, request_type="swap", time_offset_seconds=2),
            create_request_with_time(1, request_type="swap", time_offset_seconds=0),  # Earliest
            create_request_with_time(2, request_type="swap", time_offset_seconds=1),
        ]
        
        strategy = ByTypeStrategy()
        results = strategy.match(requests, min_batch_size=2, max_batch_size=5)
        
        assert len(results) == 1
        batch = results[0].batch
        
        # Should be ordered by time within the type group
        assert batch.requests[0].request_id == requests[1].request_id  # Earliest
        assert batch.requests[1].request_id == requests[2].request_id
        assert batch.requests[2].request_id == requests[0].request_id  # Latest


# ============================================================================
# Test By Value Strategy
# ============================================================================

class TestByValueStrategy:
    """Tests for grouping by value range."""
    
    def test_groups_by_value_range(self):
        """Test that requests are grouped by value range."""
        requests = [
            create_request_with_time(0, ada_amount=5_000_000),    # < 10 ADA
            create_request_with_time(1, ada_amount=50_000_000),   # 10-100 ADA
            create_request_with_time(2, ada_amount=3_000_000),    # < 10 ADA
            create_request_with_time(3, ada_amount=200_000_000),  # > 100 ADA
            create_request_with_time(4, ada_amount=80_000_000),   # 10-100 ADA
        ]
        
        strategy = ByValueStrategy()
        results = strategy.match(requests, min_batch_size=2, max_batch_size=5)
        
        # Should create 2 batches: small values and medium values
        # (large value bucket only has 1, below min)
        assert len(results) == 2
    
    def test_custom_value_ranges(self):
        """Test with custom value ranges."""
        requests = [
            create_request_with_time(0, ada_amount=1_000_000),
            create_request_with_time(1, ada_amount=2_000_000),
            create_request_with_time(2, ada_amount=3_000_000),
            create_request_with_time(3, ada_amount=4_000_000),
        ]
        
        # Custom ranges: 0-2.5 ADA, 2.5-5 ADA
        strategy = ByValueStrategy(value_ranges=[
            (0, 2_500_000),
            (2_500_000, 5_000_000),
        ])
        
        results = strategy.match(requests, min_batch_size=2, max_batch_size=5)
        
        assert len(results) == 2
        
        # Check values are in correct ranges
        for result in results:
            batch = result.batch
            min_val = min(r.ada_amount for r in batch.requests)
            max_val = max(r.ada_amount for r in batch.requests)
            
            # All should be in same range
            if min_val < 2_500_000:
                assert max_val < 2_500_000
            else:
                assert min_val >= 2_500_000


# ============================================================================
# Test Greedy Strategy
# ============================================================================

class TestGreedyStrategy:
    """Tests for greedy (maximize value) strategy."""
    
    def test_prioritizes_high_value(self):
        """Test that greedy strategy prioritizes high-value requests."""
        requests = [
            create_request_with_time(0, ada_amount=5_000_000),
            create_request_with_time(1, ada_amount=100_000_000),  # Highest
            create_request_with_time(2, ada_amount=50_000_000),
            create_request_with_time(3, ada_amount=10_000_000),
        ]
        
        strategy = GreedyStrategy()
        results = strategy.match(requests, min_batch_size=2, max_batch_size=2)
        
        assert len(results) == 2
        
        # First batch should have highest values
        first_batch = results[0].batch
        values = [r.ada_amount for r in first_batch.requests]
        assert 100_000_000 in values  # Highest value
        assert 50_000_000 in values   # Second highest


# ============================================================================
# Test Request Matcher
# ============================================================================

class TestRequestMatcher:
    """Tests for the main RequestMatcher class."""
    
    def test_matcher_with_fifo(self, sample_requests, test_config):
        """Test matcher using FIFO strategy."""
        matcher = RequestMatcher(
            strategy=MatchingStrategy.FIFO,
            config=test_config,
        )
        
        batches = matcher.create_batches(sample_requests)
        
        assert len(batches) >= 1
        total_batched = sum(b.size for b in batches)
        assert total_batched <= len(sample_requests)
    
    def test_matcher_strategy_change(self, test_config):
        """Test changing matcher strategy."""
        matcher = RequestMatcher(
            strategy=MatchingStrategy.FIFO,
            config=test_config,
        )
        
        assert matcher.strategy_name == "fifo"
        
        matcher.set_strategy(MatchingStrategy.GREEDY)
        assert matcher.strategy_name == "greedy"
    
    def test_matcher_custom_strategy(self, sample_requests, test_config):
        """Test using a custom strategy."""
        
        class ReverseStrategy(MatchingStrategyBase):
            """Custom strategy that reverses order."""
            def match(self, requests, min_batch_size, max_batch_size):
                from batcher.engine.matcher import MatchingResult
                
                # Reverse the list
                reversed_requests = list(reversed(requests))
                batch = Batch()
                for r in reversed_requests[:max_batch_size]:
                    batch.add_request(r)
                
                return [MatchingResult(
                    batch=batch,
                    matched_requests=batch.requests,
                    unmatched_requests=reversed_requests[max_batch_size:],
                    reason="Reversed",
                )]
        
        matcher = RequestMatcher(
            strategy=MatchingStrategy.CUSTOM,
            custom_strategy=ReverseStrategy(),
            config=test_config,
        )
        
        batches = matcher.create_batches(sample_requests)
        assert len(batches) == 1
    
    def test_can_form_batch(self, test_config):
        """Test checking if batch can be formed."""
        matcher = RequestMatcher(config=test_config)
        
        one_request = create_requests_batch(1)
        two_requests = create_requests_batch(2)
        
        assert matcher.can_form_batch(one_request) is False  # Below min
        assert matcher.can_form_batch(two_requests) is True   # At min


# ============================================================================
# Test Batch Pool Integration
# ============================================================================

class TestBatchPoolIntegration:
    """Tests for batch creation with the request pool."""
    
    @pytest.mark.asyncio
    async def test_create_batch_from_pool(self, sample_requests, test_config):
        """Test creating a batch using requests from the pool."""
        pool = RequestPool(test_config)
        
        # Add requests to pool
        for request in sample_requests:
            await pool.add_request(request)
        
        # Get pending and create batch
        pending = await pool.get_pending_requests()
        assert len(pending) == 5
        
        # Create batch
        batch = await pool.create_batch()
        assert batch.status == BatchStatus.COLLECTING
        
        # Add requests to batch
        for request in pending[:3]:
            result = await pool.add_request_to_batch(
                request.request_id,
                batch.batch_id,
            )
            assert result is True
        
        # Verify batch state
        stored_batch = await pool.get_batch(batch.batch_id)
        assert stored_batch.size == 3
        
        # Verify request statuses
        for request in pending[:3]:
            r = await pool.get_request(request.request_id)
            assert r.status == RequestStatus.QUEUED
            assert r.batch_id == batch.batch_id
        
        # Remaining should still be pending
        new_pending = await pool.get_pending_requests()
        assert len(new_pending) == 2
    
    @pytest.mark.asyncio
    async def test_finalize_batch(self, sample_requests, test_config):
        """Test finalizing a batch for processing."""
        pool = RequestPool(test_config)
        
        for request in sample_requests:
            await pool.add_request(request)
        
        batch = await pool.create_batch()
        for request in sample_requests[:3]:
            await pool.add_request_to_batch(request.request_id, batch.batch_id)
        
        # Finalize
        finalized = await pool.finalize_batch(batch.batch_id)
        
        assert finalized.status == BatchStatus.READY
    
    @pytest.mark.asyncio
    async def test_get_active_batches(self, sample_requests, test_config):
        """Test getting active batches."""
        pool = RequestPool(test_config)
        
        for request in sample_requests:
            await pool.add_request(request)
        
        # Create and populate batches
        batch1 = await pool.create_batch()
        await pool.add_request_to_batch(sample_requests[0].request_id, batch1.batch_id)
        await pool.add_request_to_batch(sample_requests[1].request_id, batch1.batch_id)
        
        batch2 = await pool.create_batch()
        await pool.add_request_to_batch(sample_requests[2].request_id, batch2.batch_id)
        await pool.add_request_to_batch(sample_requests[3].request_id, batch2.batch_id)
        
        # Mark first as confirmed
        batch1.mark_confirmed()
        
        # Get active batches
        active = await pool.get_active_batches()
        
        assert len(active) == 1
        assert active[0].batch_id == batch2.batch_id


# ============================================================================
# Integration Test: Full Batch Ordering Flow
# ============================================================================

class TestBatchOrderingFlow:
    """Integration tests for the complete batch ordering flow."""
    
    @pytest.mark.asyncio
    async def test_full_batch_ordering_flow(self, test_config):
        """Test complete flow: pending requests -> ordered batches."""
        # Create requests with varying properties
        requests = [
            create_request_with_time(0, ada_amount=5_000_000, request_type="swap", time_offset_seconds=0),
            create_request_with_time(1, ada_amount=10_000_000, request_type="withdraw", time_offset_seconds=1),
            create_request_with_time(2, ada_amount=15_000_000, request_type="swap", time_offset_seconds=2),
            create_request_with_time(3, ada_amount=20_000_000, request_type="swap", time_offset_seconds=3),
            create_request_with_time(4, ada_amount=25_000_000, request_type="withdraw", time_offset_seconds=4),
            create_request_with_time(5, ada_amount=30_000_000, request_type="swap", time_offset_seconds=5),
        ]
        
        # Initialize components
        pool = RequestPool(test_config)
        matcher = RequestMatcher(
            strategy=MatchingStrategy.FIFO,
            config=test_config,
        )
        
        # Step 1: Add requests to pool
        for request in requests:
            await pool.add_request(request)
        
        pending = await pool.get_pending_requests()
        assert len(pending) == 6
        
        # Step 2: Create batches with FIFO
        batches = matcher.create_batches(pending, min_size=2, max_size=3)
        
        assert len(batches) == 2  # 6 requests, max 3 per batch
        
        # Step 3: Verify FIFO ordering
        first_batch = batches[0]
        assert first_batch.size == 3
        
        # Should contain first 3 requests (by time)
        batch_ids = {r.request_id for r in first_batch.requests}
        expected_ids = {r.request_id for r in requests[:3]}
        assert batch_ids == expected_ids
        
        # Step 4: Try by_type strategy
        matcher.set_strategy(MatchingStrategy.BY_TYPE)
        type_batches = matcher.create_batches(pending, min_size=2, max_size=10)
        
        # Should group by type
        for batch in type_batches:
            types = {r.request_type for r in batch.requests}
            assert len(types) == 1  # All same type
        
        # Step 5: Try greedy strategy  
        matcher.set_strategy(MatchingStrategy.GREEDY)
        greedy_batches = matcher.create_batches(pending, min_size=2, max_size=3)
        
        # First batch should have highest values
        first_greedy = greedy_batches[0]
        values = sorted([r.ada_amount for r in first_greedy.requests], reverse=True)
        assert values[0] == 30_000_000  # Highest value first
        
        print("\n✅ Batch Ordering Test PASSED")
        print(f"   - Created {len(batches)} FIFO batches")
        print(f"   - Created {len(type_batches)} type-grouped batches")
        print(f"   - Created {len(greedy_batches)} greedy batches")
        print(f"   - Verified ordering for all strategies")
    
    @pytest.mark.asyncio
    async def test_batch_ordering_with_pool_integration(self, test_config):
        """Test batch ordering with full pool integration."""
        requests = create_requests_batch(10)
        
        pool = RequestPool(test_config)
        
        # Add to pool
        for request in requests:
            await pool.add_request(request)
        
        # Get pending requests
        pending = await pool.get_pending_requests()
        assert len(pending) == 10
        
        # Create batches through the pool
        batch_count = 0
        requests_batched = 0
        
        # Process requests in batches of max size
        while requests_batched < len(pending):
            pool_batch = await pool.create_batch()
            batch_size = min(test_config.batch_size_max, len(pending) - requests_batched)
            
            for i in range(batch_size):
                request = pending[requests_batched + i]
                await pool.add_request_to_batch(
                    request.request_id,
                    pool_batch.batch_id,
                )
            
            requests_batched += batch_size
            batch_count += 1
        
        # Verify state
        stats = await pool.get_stats()
        assert stats["pending"] == 0  # All should be queued now
        assert stats["queued"] == 10
        
        print("\n✅ Batch Pool Integration Test PASSED")
        print(f"   - Batched {stats['queued']} requests")
        print(f"   - Created {batch_count} batches")

