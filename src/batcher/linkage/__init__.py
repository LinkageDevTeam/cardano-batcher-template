"""
Linkage Finance Batcher Implementation.

This module provides a specialized batcher for Linkage Finance smart contracts,
supporting deposit, withdraw, and royalty collection operations.

Example usage:
    ```python
    from batcher.linkage import LinkageBatcher, LinkageConfig
    
    config = LinkageConfig.from_env()
    batcher = LinkageBatcher(config)
    
    await batcher.initialize()
    await batcher.start()
    ```
"""

from batcher.linkage.types import (
    LinkageRequestType,
    IndexedToken,
    ImmutableParams,
    IndexParameters,
    MintRedeemer,
    BurnRedeemer,
    RoyaltyWithdrawalRedeemer,
    LinkageRequest,
)
from batcher.linkage.scanner import LinkageDatumParser
from batcher.linkage.builder import LinkageTransactionBuilder
from batcher.linkage.batcher import LinkageBatcher
from batcher.linkage.config import LinkageConfig

__all__ = [
    # Types
    "LinkageRequestType",
    "IndexedToken",
    "ImmutableParams",
    "IndexParameters",
    "MintRedeemer",
    "BurnRedeemer",
    "RoyaltyWithdrawalRedeemer",
    "LinkageRequest",
    # Components
    "LinkageDatumParser",
    "LinkageTransactionBuilder",
    "LinkageBatcher",
    "LinkageConfig",
]

