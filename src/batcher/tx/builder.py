"""
Transaction Builder - constructs batching transactions.

Handles the construction of transactions that process batched requests.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

import structlog

from pycardano import (
    Address,
    MultiAsset,
    PlutusData,
    Redeemer,
    RedeemerTag,
    Transaction,
    TransactionBody,
    TransactionBuilder as PyCardanoTxBuilder,
    TransactionInput,
    TransactionOutput,
    UTxO,
    Value,
    ExecutionUnits,
)

from batcher.config import BatcherConfig, get_config
from batcher.core.batch import Batch
from batcher.core.request import BatchRequest
from batcher.node.interface import NodeInterface, ProtocolParameters
from batcher.tx.signer import TransactionSigner

logger = structlog.get_logger(__name__)


class TransactionBuildError(Exception):
    """Raised when transaction construction fails."""
    pass


@dataclass
class BatchTransactionSpec:
    """
    Specification for a batch transaction.
    
    DApps should create this to define how their batch transaction
    should be constructed.
    """
    
    # Inputs to consume (the batch request UTXOs)
    inputs: List[UTxO]
    
    # Outputs to create
    outputs: List[TransactionOutput]
    
    # Script inputs with redeemers (for script-locked UTXOs)
    script_inputs: List[Tuple[UTxO, Redeemer]] = None
    
    # Reference inputs (read-only)
    reference_inputs: List[TransactionInput] = None
    
    # Collateral UTXOs (for script execution)
    collateral: List[UTxO] = None
    
    # Minting/burning
    mint: Optional[MultiAsset] = None
    mint_redeemers: List[Redeemer] = None
    
    # Metadata
    metadata: Optional[dict] = None
    
    # Required signers (additional signatures beyond the batcher)
    required_signers: List[bytes] = None
    
    # Validity range
    valid_from: Optional[int] = None
    valid_to: Optional[int] = None


class TransactionSpecBuilder(ABC):
    """
    Abstract base class for building transaction specifications.
    
    DApps should implement this to define their specific transaction logic.
    """
    
    @abstractmethod
    def build_spec(
        self,
        batch: Batch,
        batcher_address: Address,
        protocol_params: ProtocolParameters,
    ) -> BatchTransactionSpec:
        """
        Build a transaction specification for a batch.
        
        Args:
            batch: The batch to process
            batcher_address: Address of the batcher wallet
            protocol_params: Current protocol parameters
            
        Returns:
            BatchTransactionSpec defining the transaction
        """
        pass


class DefaultTransactionSpecBuilder(TransactionSpecBuilder):
    """
    Default transaction spec builder for simple batching.
    
    This implementation collects all request UTXOs and sends
    the combined value back to the batcher address (minus fees).
    
    DApps should override this with their specific logic.
    """
    
    def build_spec(
        self,
        batch: Batch,
        batcher_address: Address,
        protocol_params: ProtocolParameters,
    ) -> BatchTransactionSpec:
        """Build a simple collection transaction."""
        inputs = []
        total_value = Value(0)
        
        for request in batch.requests:
            # Create UTxO from request
            utxo = UTxO(
                request.utxo_ref,
                TransactionOutput(
                    Address.from_primitive(request.requester_address) if request.requester_address 
                    else batcher_address,
                    request.value,
                )
            )
            inputs.append(utxo)
            total_value += request.value
        
        # Create single output to batcher (actual fee will be calculated during build)
        # Leave some margin for fee
        min_fee_estimate = 200_000  # 0.2 ADA estimate
        output_value = Value(max(1_000_000, total_value.coin - min_fee_estimate))
        
        if total_value.multi_asset:
            output_value = Value(output_value.coin, total_value.multi_asset)
        
        outputs = [
            TransactionOutput(batcher_address, output_value)
        ]
        
        return BatchTransactionSpec(
            inputs=inputs,
            outputs=outputs,
        )


class TransactionBuilder:
    """
    Builds and signs batch transactions.
    
    Coordinates between the spec builder, PyCardano transaction builder,
    and the signer to produce signed transactions.
    """
    
    def __init__(
        self,
        node: NodeInterface,
        signer: TransactionSigner,
        spec_builder: Optional[TransactionSpecBuilder] = None,
        config: Optional[BatcherConfig] = None,
    ):
        """
        Initialize the transaction builder.
        
        Args:
            node: Node interface for blockchain queries
            signer: Transaction signer
            spec_builder: Custom spec builder (uses default if not provided)
            config: Batcher configuration
        """
        self.node = node
        self.signer = signer
        self.spec_builder = spec_builder or DefaultTransactionSpecBuilder()
        self.config = config or get_config()
        
        self._protocol_params: Optional[ProtocolParameters] = None
    
    async def build_batch_transaction(
        self,
        batch: Batch,
    ) -> Transaction:
        """
        Build a transaction for a batch.
        
        Args:
            batch: The batch to process
            
        Returns:
            Signed transaction
            
        Raises:
            TransactionBuildError: If transaction cannot be built
        """
        if not self.signer.is_loaded:
            raise TransactionBuildError("Signer key not loaded")
        
        if batch.is_empty:
            raise TransactionBuildError("Cannot build transaction for empty batch")
        
        logger.info(
            "building_batch_transaction",
            batch_id=batch.batch_id[:8] + "...",
            request_count=batch.size,
        )
        
        try:
            # Get protocol parameters
            if not self._protocol_params:
                self._protocol_params = await self.node.get_protocol_parameters()
            
            # Get batcher UTXOs for fees/collateral
            batcher_address = str(self.signer.address)
            batcher_utxos = await self.node.get_utxos_at_address(batcher_address)
            
            if not batcher_utxos:
                raise TransactionBuildError("No UTXOs at batcher address for fees")
            
            # Build transaction spec
            spec = self.spec_builder.build_spec(
                batch,
                self.signer.address,
                self._protocol_params,
            )
            
            # Build transaction using PyCardano
            tx = await self._build_from_spec(spec, batcher_utxos)
            
            # Sign transaction
            signed_tx = self.signer.sign_transaction(tx.transaction_body)
            
            logger.info(
                "batch_transaction_built",
                batch_id=batch.batch_id[:8] + "...",
                tx_hash=signed_tx.transaction_body.hash().hex()[:16] + "...",
            )
            
            return signed_tx
            
        except TransactionBuildError:
            raise
        except Exception as e:
            logger.error(
                "transaction_build_failed",
                batch_id=batch.batch_id[:8] + "...",
                error=str(e),
            )
            raise TransactionBuildError(f"Failed to build transaction: {e}")
    
    async def _build_from_spec(
        self,
        spec: BatchTransactionSpec,
        fee_utxos: List[UTxO],
    ) -> Transaction:
        """
        Build a transaction from a specification.
        
        Args:
            spec: Transaction specification
            fee_utxos: UTXOs available for fees
            
        Returns:
            Unsigned transaction
        """
        from pycardano import TransactionBuilder as PyCardanoBuilder
        from batcher.config import NetworkType
        
        # Determine network
        if self.config.network == NetworkType.MAINNET:
            from pycardano import Network
            context_network = Network.MAINNET
        else:
            from pycardano import Network
            context_network = Network.TESTNET
        
        # Create a chain context for PyCardano
        # This is a simplified approach - in production you'd use
        # BlockFrostChainContext or OgmiosChainContext
        builder = PyCardanoBuilder(context=None)
        
        # For now, we'll build the transaction manually
        # as we need more control over the inputs/outputs
        
        # Calculate total input value
        total_input = Value(0)
        inputs = []
        
        for utxo in spec.inputs:
            inputs.append(utxo.input)
            total_input += utxo.output.amount
        
        # Add fee UTXOs
        for utxo in fee_utxos:
            if utxo.input not in inputs:
                inputs.append(utxo.input)
                total_input += utxo.output.amount
                break  # Usually one fee UTXO is enough
        
        # Calculate output value
        total_output = Value(0)
        for output in spec.outputs:
            total_output += output.amount
        
        # Calculate fee (simplified - proper fee calc needs full serialization)
        estimated_fee = self._estimate_fee(len(inputs), len(spec.outputs))
        
        # Adjust change output if needed
        outputs = list(spec.outputs)
        change = total_input.coin - total_output.coin - estimated_fee
        
        if change > 1_000_000:  # Min UTXO value
            # Add change output
            change_output = TransactionOutput(
                self.signer.address,
                Value(change),
            )
            outputs.append(change_output)
        elif change > 0:
            # Add change to last output
            if outputs:
                outputs[-1] = TransactionOutput(
                    outputs[-1].address,
                    Value(outputs[-1].amount.coin + change, outputs[-1].amount.multi_asset),
                )
        
        # Build transaction body
        tx_body = TransactionBody(
            inputs=inputs,
            outputs=outputs,
            fee=estimated_fee,
        )
        
        # Add optional fields
        if spec.valid_to:
            tx_body.ttl = spec.valid_to
        
        if spec.reference_inputs:
            tx_body.reference_inputs = spec.reference_inputs
        
        if spec.collateral:
            tx_body.collateral = [u.input for u in spec.collateral]
        
        if spec.mint:
            tx_body.mint = spec.mint
        
        if spec.required_signers:
            tx_body.required_signers = spec.required_signers
        
        return Transaction(tx_body, None)
    
    def _estimate_fee(self, num_inputs: int, num_outputs: int) -> int:
        """
        Estimate transaction fee.
        
        This is a simplified estimation. Production code should
        calculate the actual fee based on serialized transaction size.
        
        Args:
            num_inputs: Number of inputs
            num_outputs: Number of outputs
            
        Returns:
            Estimated fee in lovelace
        """
        if not self._protocol_params:
            return 200_000  # Default estimate
        
        # Rough size estimate
        base_size = 200  # Base transaction overhead
        input_size = 40 * num_inputs
        output_size = 65 * num_outputs
        witness_size = 150  # One signature
        
        total_size = base_size + input_size + output_size + witness_size
        
        fee = (
            self._protocol_params.min_fee_a * total_size +
            self._protocol_params.min_fee_b
        )
        
        # Add 20% margin
        return int(fee * 1.2)
    
    async def refresh_protocol_params(self) -> None:
        """Refresh cached protocol parameters."""
        self._protocol_params = await self.node.get_protocol_parameters()
        logger.debug("protocol_params_refreshed")


