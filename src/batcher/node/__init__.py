"""
Node Integration Layer.

Provides abstracted access to Cardano blockchain data and transaction submission.
Supports multiple backends (Blockfrost, Ogmios, direct node connection).
"""

from batcher.node.interface import NodeInterface
from batcher.node.blockfrost import BlockfrostAdapter
from batcher.node.ogmios import OgmiosAdapter

__all__ = [
    "NodeInterface",
    "BlockfrostAdapter",
    "OgmiosAdapter",
]


