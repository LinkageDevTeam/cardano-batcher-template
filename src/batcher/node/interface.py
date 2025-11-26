"""
Abstract interface for Cardano node integration.

Defines the contract for blockchain access that all node adapters must implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from pycardano import (
    TransactionInput,
    TransactionOutput,
    Transaction,
    UTxO,
)


@dataclass
class ProtocolParameters:
    """Protocol parameters from the Cardano network."""
    min_fee_a: int                     # Fee coefficient (per byte)
    min_fee_b: int                     # Fee constant
    max_tx_size: int                   # Maximum transaction size in bytes
    max_val_size: int                  # Maximum value size
    key_deposit: int                   # Key registration deposit
    pool_deposit: int                  # Pool registration deposit
    coins_per_utxo_byte: int           # Min ADA per UTXO byte
    collateral_percentage: int         # Collateral percentage for scripts
    max_collateral_inputs: int         # Maximum collateral inputs
    price_mem: float                   # Plutus memory price
    price_step: float                  # Plutus step price
    max_tx_ex_mem: int                 # Max execution memory
    max_tx_ex_steps: int               # Max execution steps
    cost_models: Optional[dict] = None # Plutus cost models


@dataclass
class ChainTip:
    """Current chain tip information."""
    slot: int
    block_hash: str
    block_height: int
    epoch: int
    epoch_slot: int


class NodeInterface(ABC):
    """
    Abstract interface for Cardano node access.
    
    This interface defines all blockchain operations needed by the batcher:
    - UTXO queries
    - Transaction submission
    - Protocol parameters
    - Transaction monitoring
    """
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to the node/API.
        
        Raises:
            ConnectionError: If connection cannot be established
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the node/API."""
        pass
    
    @abstractmethod
    async def get_protocol_parameters(self) -> ProtocolParameters:
        """
        Get current protocol parameters.
        
        Returns:
            Current protocol parameters from the network
        """
        pass
    
    @abstractmethod
    async def get_chain_tip(self) -> ChainTip:
        """
        Get current chain tip.
        
        Returns:
            Current chain tip information
        """
        pass
    
    @abstractmethod
    async def get_utxos_at_address(
        self,
        address: str,
        asset: Optional[str] = None,
    ) -> List[UTxO]:
        """
        Get all UTXOs at a given address.
        
        Args:
            address: Bech32 encoded address
            asset: Optional asset filter (policy_id + asset_name in hex)
            
        Returns:
            List of UTXOs at the address
        """
        pass
    
    @abstractmethod
    async def get_utxo(
        self,
        tx_hash: str,
        output_index: int,
    ) -> Optional[UTxO]:
        """
        Get a specific UTXO by transaction hash and output index.
        
        Args:
            tx_hash: Transaction hash
            output_index: Output index
            
        Returns:
            The UTXO if it exists and is unspent, None otherwise
        """
        pass
    
    @abstractmethod
    async def get_datum(self, datum_hash: str) -> Optional[Any]:
        """
        Get datum by hash.
        
        Args:
            datum_hash: Hash of the datum
            
        Returns:
            Decoded datum if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def submit_transaction(self, tx: Transaction) -> str:
        """
        Submit a signed transaction to the network.
        
        Args:
            tx: Signed transaction to submit
            
        Returns:
            Transaction hash
            
        Raises:
            TransactionSubmitError: If submission fails
        """
        pass
    
    @abstractmethod
    async def await_transaction_confirmation(
        self,
        tx_hash: str,
        timeout_seconds: int = 120,
        confirmations: int = 1,
    ) -> bool:
        """
        Wait for a transaction to be confirmed.
        
        Args:
            tx_hash: Hash of the transaction to monitor
            timeout_seconds: Maximum time to wait
            confirmations: Number of confirmations to wait for
            
        Returns:
            True if confirmed within timeout, False otherwise
        """
        pass
    
    @abstractmethod
    async def get_transaction(self, tx_hash: str) -> Optional[dict]:
        """
        Get transaction details by hash.
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Transaction details if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def check_utxo_exists(self, tx_hash: str, output_index: int) -> bool:
        """
        Check if a UTXO still exists (hasn't been spent).
        
        Args:
            tx_hash: Transaction hash
            output_index: Output index
            
        Returns:
            True if UTXO exists and is unspent
        """
        pass
    
    async def get_script_utxos_with_datum(
        self,
        script_address: str,
        datum_filter: Optional[callable] = None,
    ) -> List[Tuple[UTxO, Any]]:
        """
        Get UTXOs at a script address with their resolved datums.
        
        Args:
            script_address: Script address to query
            datum_filter: Optional function to filter datums
            
        Returns:
            List of (UTXO, datum) tuples
        """
        utxos = await self.get_utxos_at_address(script_address)
        results = []
        
        for utxo in utxos:
            datum = None
            
            # Try to get inline datum first
            if hasattr(utxo.output, 'datum') and utxo.output.datum is not None:
                datum = utxo.output.datum
            # Otherwise try to resolve from datum hash
            elif hasattr(utxo.output, 'datum_hash') and utxo.output.datum_hash is not None:
                datum = await self.get_datum(str(utxo.output.datum_hash))
            
            # Apply filter if provided
            if datum_filter is None or datum_filter(datum):
                results.append((utxo, datum))
        
        return results


class NodeConnectionError(Exception):
    """Raised when connection to node fails."""
    pass


class TransactionSubmitError(Exception):
    """Raised when transaction submission fails."""
    
    def __init__(self, message: str, error_code: Optional[str] = None):
        super().__init__(message)
        self.error_code = error_code

