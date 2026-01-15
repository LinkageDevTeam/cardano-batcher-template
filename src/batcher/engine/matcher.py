"""
Request Matcher - groups requests into batches.

Implements various strategies for matching and grouping requests.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple

import structlog

from batcher.config import BatcherConfig, get_config
from batcher.core.request import BatchRequest
from batcher.core.batch import Batch

logger = structlog.get_logger(__name__)


class MatchingStrategy(str, Enum):
    """Available matching strategies."""
    FIFO = "fifo"                    # First-in-first-out
    BY_TYPE = "by_type"              # Group by request type
    BY_VALUE = "by_value"            # Group by similar value ranges
    GREEDY = "greedy"                # Maximize batch value
    CUSTOM = "custom"                # Custom strategy


@dataclass
class MatchingResult:
    """Result of a matching operation."""
    
    batch: Batch
    matched_requests: List[BatchRequest]
    unmatched_requests: List[BatchRequest]
    reason: str = ""


class MatchingStrategyBase(ABC):
    """
    Abstract base class for matching strategies.
    
    Implement this to create custom matching logic for your DApp.
    """
    
    @abstractmethod
    def match(
        self,
        requests: List[BatchRequest],
        min_batch_size: int,
        max_batch_size: int,
    ) -> List[MatchingResult]:
        """
        Match requests into batches.
        
        Args:
            requests: List of pending requests to match
            min_batch_size: Minimum number of requests for a valid batch
            max_batch_size: Maximum number of requests in a batch
            
        Returns:
            List of MatchingResult objects
        """
        pass


class FIFOStrategy(MatchingStrategyBase):
    """
    First-in-first-out matching strategy.
    
    Groups requests in order of arrival, creating batches when
    enough requests are available.
    """
    
    def match(
        self,
        requests: List[BatchRequest],
        min_batch_size: int,
        max_batch_size: int,
    ) -> List[MatchingResult]:
        """Create batches from oldest requests first."""
        # Sort by creation time (oldest first)
        sorted_requests = sorted(requests, key=lambda r: r.created_at)
        
        results = []
        remaining = sorted_requests.copy()
        
        while len(remaining) >= min_batch_size:
            # Take up to max_batch_size requests
            batch_requests = remaining[:max_batch_size]
            remaining = remaining[max_batch_size:]
            
            batch = Batch()
            for request in batch_requests:
                batch.add_request(request)
            
            results.append(MatchingResult(
                batch=batch,
                matched_requests=batch_requests,
                unmatched_requests=remaining.copy(),
                reason="FIFO order",
            ))
        
        return results


class ByTypeStrategy(MatchingStrategyBase):
    """
    Group requests by type.
    
    Creates separate batches for each request type,
    useful when different types need different processing.
    """
    
    def match(
        self,
        requests: List[BatchRequest],
        min_batch_size: int,
        max_batch_size: int,
    ) -> List[MatchingResult]:
        """Group requests by request_type."""
        # Group by type
        by_type: dict[Optional[str], List[BatchRequest]] = {}
        for request in requests:
            request_type = request.request_type
            if request_type not in by_type:
                by_type[request_type] = []
            by_type[request_type].append(request)
        
        results = []
        all_unmatched = []
        
        for request_type, type_requests in by_type.items():
            # Sort each type by creation time
            sorted_requests = sorted(type_requests, key=lambda r: r.created_at)
            
            # Create batches for this type
            while len(sorted_requests) >= min_batch_size:
                batch_requests = sorted_requests[:max_batch_size]
                sorted_requests = sorted_requests[max_batch_size:]
                
                batch = Batch()
                for request in batch_requests:
                    batch.add_request(request)
                
                results.append(MatchingResult(
                    batch=batch,
                    matched_requests=batch_requests,
                    unmatched_requests=[],
                    reason=f"Type: {request_type}",
                ))
            
            all_unmatched.extend(sorted_requests)
        
        # Update unmatched in last result if any
        if results and all_unmatched:
            results[-1].unmatched_requests = all_unmatched
        
        return results


class ByValueStrategy(MatchingStrategyBase):
    """
    Group requests by value range.
    
    Groups requests with similar ADA values together,
    useful for fair processing or optimizing transaction fees.
    """
    
    def __init__(self, value_ranges: Optional[List[Tuple[int, int]]] = None):
        """
        Initialize with value ranges.
        
        Args:
            value_ranges: List of (min, max) ADA value ranges in lovelace.
                         Defaults to small/medium/large buckets.
        """
        self.value_ranges = value_ranges or [
            (0, 10_000_000),           # < 10 ADA
            (10_000_000, 100_000_000), # 10-100 ADA
            (100_000_000, float('inf')), # > 100 ADA
        ]
    
    def match(
        self,
        requests: List[BatchRequest],
        min_batch_size: int,
        max_batch_size: int,
    ) -> List[MatchingResult]:
        """Group requests by value range."""
        # Bucket requests by value range
        buckets: dict[int, List[BatchRequest]] = {i: [] for i in range(len(self.value_ranges))}
        
        for request in requests:
            for i, (min_val, max_val) in enumerate(self.value_ranges):
                if min_val <= request.ada_amount < max_val:
                    buckets[i].append(request)
                    break
        
        results = []
        all_unmatched = []
        
        for bucket_idx, bucket_requests in buckets.items():
            if not bucket_requests:
                continue
            
            # Sort by creation time
            sorted_requests = sorted(bucket_requests, key=lambda r: r.created_at)
            
            # Create batches
            while len(sorted_requests) >= min_batch_size:
                batch_requests = sorted_requests[:max_batch_size]
                sorted_requests = sorted_requests[max_batch_size:]
                
                batch = Batch()
                for request in batch_requests:
                    batch.add_request(request)
                
                min_val, max_val = self.value_ranges[bucket_idx]
                results.append(MatchingResult(
                    batch=batch,
                    matched_requests=batch_requests,
                    unmatched_requests=[],
                    reason=f"Value range: {min_val/1_000_000:.0f}-{max_val/1_000_000:.0f} ADA",
                ))
            
            all_unmatched.extend(sorted_requests)
        
        if results and all_unmatched:
            results[-1].unmatched_requests = all_unmatched
        
        return results


class GreedyStrategy(MatchingStrategyBase):
    """
    Greedy matching to maximize batch value.
    
    Prioritizes high-value requests to maximize the total
    value processed per batch (useful for fee optimization).
    """
    
    def match(
        self,
        requests: List[BatchRequest],
        min_batch_size: int,
        max_batch_size: int,
    ) -> List[MatchingResult]:
        """Create batches prioritizing high-value requests."""
        # Sort by value (highest first)
        sorted_requests = sorted(requests, key=lambda r: r.ada_amount, reverse=True)
        
        results = []
        remaining = sorted_requests.copy()
        
        while len(remaining) >= min_batch_size:
            batch_requests = remaining[:max_batch_size]
            remaining = remaining[max_batch_size:]
            
            batch = Batch()
            for request in batch_requests:
                batch.add_request(request)
            
            total_ada = sum(r.ada_amount for r in batch_requests) / 1_000_000
            results.append(MatchingResult(
                batch=batch,
                matched_requests=batch_requests,
                unmatched_requests=remaining.copy(),
                reason=f"Greedy: {total_ada:.2f} ADA total",
            ))
        
        return results


class RequestMatcher:
    """
    Main matcher class that coordinates request matching.
    
    Supports multiple strategies and provides a unified interface
    for batch formation.
    """
    
    STRATEGIES = {
        MatchingStrategy.FIFO: FIFOStrategy,
        MatchingStrategy.BY_TYPE: ByTypeStrategy,
        MatchingStrategy.BY_VALUE: ByValueStrategy,
        MatchingStrategy.GREEDY: GreedyStrategy,
    }
    
    def __init__(
        self,
        strategy: MatchingStrategy = MatchingStrategy.FIFO,
        custom_strategy: Optional[MatchingStrategyBase] = None,
        config: Optional[BatcherConfig] = None,
    ):
        """
        Initialize the request matcher.
        
        Args:
            strategy: The matching strategy to use
            custom_strategy: Custom strategy implementation (if strategy is CUSTOM)
            config: Batcher configuration
        """
        self.config = config or get_config()
        
        if strategy == MatchingStrategy.CUSTOM:
            if not custom_strategy:
                raise ValueError("custom_strategy required when strategy is CUSTOM")
            self._strategy = custom_strategy
        else:
            strategy_class = self.STRATEGIES.get(strategy)
            if not strategy_class:
                raise ValueError(f"Unknown strategy: {strategy}")
            self._strategy = strategy_class()
        
        self.strategy_name = strategy.value
        
        logger.info("matcher_initialized", strategy=self.strategy_name)
    
    def create_batches(
        self,
        requests: List[BatchRequest],
        min_size: Optional[int] = None,
        max_size: Optional[int] = None,
    ) -> List[Batch]:
        """
        Create batches from pending requests.
        
        Args:
            requests: List of requests to batch
            min_size: Minimum batch size (uses config default if not provided)
            max_size: Maximum batch size (uses config default if not provided)
            
        Returns:
            List of created Batch objects
        """
        if not requests:
            return []
        
        min_size = min_size or self.config.batch_size_min
        max_size = max_size or self.config.batch_size_max
        
        logger.debug(
            "creating_batches",
            request_count=len(requests),
            min_size=min_size,
            max_size=max_size,
            strategy=self.strategy_name,
        )
        
        results = self._strategy.match(requests, min_size, max_size)
        
        batches = [r.batch for r in results]
        
        if batches:
            logger.info(
                "batches_created",
                batch_count=len(batches),
                total_requests=sum(b.size for b in batches),
                reasons=[r.reason for r in results],
            )
        
        return batches
    
    def can_form_batch(
        self,
        requests: List[BatchRequest],
        min_size: Optional[int] = None,
    ) -> bool:
        """
        Check if there are enough requests to form a batch.
        
        Args:
            requests: List of pending requests
            min_size: Minimum batch size
            
        Returns:
            True if a batch can be formed
        """
        min_size = min_size or self.config.batch_size_min
        return len(requests) >= min_size
    
    def set_strategy(
        self,
        strategy: MatchingStrategy,
        custom_strategy: Optional[MatchingStrategyBase] = None,
    ) -> None:
        """
        Change the matching strategy.
        
        Args:
            strategy: New strategy to use
            custom_strategy: Custom strategy if strategy is CUSTOM
        """
        if strategy == MatchingStrategy.CUSTOM:
            if not custom_strategy:
                raise ValueError("custom_strategy required")
            self._strategy = custom_strategy
        else:
            strategy_class = self.STRATEGIES.get(strategy)
            if not strategy_class:
                raise ValueError(f"Unknown strategy: {strategy}")
            self._strategy = strategy_class()
        
        self.strategy_name = strategy.value
        logger.info("strategy_changed", new_strategy=self.strategy_name)


