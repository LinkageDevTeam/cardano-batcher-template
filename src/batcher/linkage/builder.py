"""
Linkage Finance Transaction Builder.

Constructs transactions for Linkage Finance operations:
- Deposit: Mint fund tokens in exchange for underlying tokens
- Withdraw: Burn fund tokens to receive underlying tokens
- Collect Royalty: Withdraw accrued royalties to fund creator
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import structlog

from pycardano import (
    Address,
    Asset,
    AssetName,
    MultiAsset,
    PlutusV2Script,
    RawCBOR,
    Redeemer,
    ScriptHash,
    Transaction,
    TransactionBody,
    TransactionBuilder as PyCardanoBuilder,
    TransactionOutput,
    TransactionWitnessSet,
    UTxO,
    Value,
    min_lovelace,
)

from batcher.linkage.config import LinkageConfig
from batcher.linkage.types import (
    LinkageRequest,
    LinkageRequestType,
    LinkageBatchResult,
    IndexParameters,
    MintRedeemer,
    BurnRedeemer,
    RoyaltyWithdrawalRedeemer,
    token_name_from_parameters,
)
from batcher.linkage.scanner import LinkageRequestScanner
from batcher.node.interface import NodeInterface, ProtocolParameters
from batcher.tx.signer import TransactionSigner

logger = structlog.get_logger(__name__)


class LinkageTransactionBuildError(Exception):
    """Raised when transaction construction fails."""
    pass


@dataclass
class DepositParams:
    """Parameters for a deposit transaction."""
    fund_request: LinkageRequest
    user_address: Address
    multiple: int  # Deposit multiple


@dataclass
class WithdrawParams:
    """Parameters for a withdraw transaction."""
    fund_request: LinkageRequest
    user_address: Address
    amount: int  # Fund tokens to burn


@dataclass
class CollectRoyaltyParams:
    """Parameters for a royalty collection transaction."""
    fund_request: LinkageRequest
    creator_address: Address
    amount: int  # Royalty tokens to collect


class LinkageTransactionBuilder:
    """
    Builds transactions for Linkage Finance operations.
    
    This builder handles the complex transaction construction needed
    for interacting with Linkage smart contracts, including:
    - Script inputs with proper redeemers
    - Reference script UTXOs
    - Minting/burning fund tokens
    - Proper datum updates
    """
    
    def __init__(
        self,
        node: NodeInterface,
        scanner: LinkageRequestScanner,
        signer: TransactionSigner,
        config: LinkageConfig,
    ):
        """
        Initialize the transaction builder.
        
        Args:
            node: Node interface for blockchain queries
            scanner: Scanner for finding UTXOs
            signer: Transaction signer
            config: Linkage configuration
        """
        self.node = node
        self.scanner = scanner
        self.signer = signer
        self.config = config
        
        self._protocol_params: Optional[ProtocolParameters] = None
        self._chain_context = None
    
    async def _ensure_chain_context(self):
        """Ensure we have a chain context for building transactions."""
        if self._chain_context is None:
            # Get protocol parameters to build context
            self._protocol_params = await self.node.get_protocol_parameters()
    
    def _estimate_min_lovelace(
        self,
        address: Address,
        multi_asset: Optional[MultiAsset] = None,
        datum: Optional[RawCBOR] = None,
    ) -> int:
        """
        Estimate minimum lovelace for a UTXO.
        
        This is a conservative estimate based on UTXO size.
        
        Args:
            address: Output address
            multi_asset: Native tokens in the output
            datum: Datum to include
            
        Returns:
            Minimum lovelace required
        """
        # Base cost + per-byte cost
        # Conservative estimates for typical Linkage outputs
        base = 1_500_000  # 1.5 ADA base
        
        if multi_asset:
            # Add ~0.5 ADA per unique policy + token
            num_tokens = sum(len(assets) for assets in multi_asset.values())
            base += num_tokens * 500_000
        
        if datum:
            # Add based on datum size
            datum_size = len(datum.cbor) if hasattr(datum, 'cbor') else len(bytes(datum))
            base += datum_size * 1000  # ~1000 lovelace per byte
        
        return base
    
    async def build_deposit_transaction(
        self,
        params: DepositParams,
    ) -> Transaction:
        """
        Build a deposit transaction.
        
        Deposit operations:
        1. Consume the fund UTXO
        2. Add deposited underlying tokens to the fund
        3. Mint new fund tokens for the user
        4. Update the fund datum with new royalty accrual
        5. Send fund tokens to user (minus royalty portion)
        
        Args:
            params: Deposit parameters
            
        Returns:
            Built transaction (unsigned)
            
        Raises:
            LinkageTransactionBuildError: If transaction cannot be built
        """
        await self._ensure_chain_context()
        
        logger.info(
            "building_deposit_tx",
            fund=params.fund_request.fund_name_readable,
            multiple=params.multiple,
        )
        
        try:
            fund = params.fund_request
            fund_datum = fund.fund_datum
            
            # Get the fund UTXO
            fund_utxo = await self._get_fund_utxo(fund)
            if not fund_utxo:
                raise LinkageTransactionBuildError("Fund UTXO not found")
            
            # Get reference script UTXO
            ref_script_utxo = await self.scanner.get_reference_script_utxo(
                fund.fund_script_hash
            )
            if not ref_script_utxo:
                raise LinkageTransactionBuildError("Reference script UTXO not found")
            
            # Calculate royalty accrual
            royalty_accrual = (
                fund_datum.immutable_params.royalty_factor * params.multiple
            ) // fund_datum.immutable_params.fund_token_factor
            
            # Create updated datum
            output_datum = IndexParameters(
                immutable_params=fund_datum.immutable_params,
                accrued_royalty=fund_datum.accrued_royalty + royalty_accrual,
            ).to_sanitized()
            
            # Calculate tokens to deposit
            deposit_multi_asset = MultiAsset()
            for idx_token in fund_datum.immutable_params.index_tokens:
                pid = ScriptHash(idx_token.token.policy_id)
                name = AssetName(idx_token.token.token_name)
                if pid not in deposit_multi_asset:
                    deposit_multi_asset[pid] = Asset()
                deposit_multi_asset[pid][name] = (
                    idx_token.factor * params.multiple * 
                    fund_datum.immutable_params.fund_token_factor
                )
            
            # Add royalty fund tokens to the contract
            fund_token_name = AssetName(bytes.fromhex(fund.fund_token_name))
            if fund.fund_script_hash not in deposit_multi_asset:
                deposit_multi_asset[fund.fund_script_hash] = Asset()
            deposit_multi_asset[fund.fund_script_hash][fund_token_name] = royalty_accrual
            
            # Calculate new fund UTXO value
            new_fund_value = Value(
                coin=fund_utxo.output.amount.coin,
                multi_asset=deposit_multi_asset + fund_utxo.output.amount.multi_asset,
            )
            
            # Outputs
            outputs = []
            
            # 1. Updated fund output
            fund_output = TransactionOutput(
                address=Address.from_primitive(self.config.fund_address),
                amount=new_fund_value,
                datum=RawCBOR(output_datum.to_cbor()),
            )
            outputs.append(fund_output)
            
            # 2. User receives fund tokens (minus royalty)
            minted_amount = params.multiple * fund_datum.immutable_params.fund_token_factor
            user_fund_tokens = MultiAsset()
            user_fund_tokens[fund.fund_script_hash] = Asset()
            user_fund_tokens[fund.fund_script_hash][fund_token_name] = (
                minted_amount - royalty_accrual
            )
            
            user_output_value = self._estimate_min_lovelace(
                params.user_address, user_fund_tokens
            )
            user_output = TransactionOutput(
                address=params.user_address,
                amount=Value(coin=user_output_value, multi_asset=user_fund_tokens),
            )
            outputs.append(user_output)
            
            # Mint fund tokens
            mint = MultiAsset()
            mint[fund.fund_script_hash] = Asset()
            mint[fund.fund_script_hash][fund_token_name] = minted_amount
            
            # Build transaction body
            tx = self._build_script_transaction(
                script_input=fund_utxo,
                ref_script=ref_script_utxo,
                redeemer=MintRedeemer(),
                outputs=outputs,
                mint=mint,
                user_address=params.user_address,
            )
            
            return tx
            
        except LinkageTransactionBuildError:
            raise
        except Exception as e:
            logger.error("build_deposit_tx_failed", error=str(e))
            raise LinkageTransactionBuildError(f"Failed to build deposit: {e}")
    
    async def build_withdraw_transaction(
        self,
        params: WithdrawParams,
    ) -> Transaction:
        """
        Build a withdraw transaction.
        
        Withdraw operations:
        1. Consume the fund UTXO
        2. Remove withdrawn underlying tokens from the fund
        3. Burn fund tokens from the user
        4. Send underlying tokens to user
        
        Args:
            params: Withdraw parameters
            
        Returns:
            Built transaction (unsigned)
            
        Raises:
            LinkageTransactionBuildError: If transaction cannot be built
        """
        await self._ensure_chain_context()
        
        logger.info(
            "building_withdraw_tx",
            fund=params.fund_request.fund_name_readable,
            amount=params.amount,
        )
        
        try:
            fund = params.fund_request
            fund_datum = fund.fund_datum
            
            # Get the fund UTXO
            fund_utxo = await self._get_fund_utxo(fund)
            if not fund_utxo:
                raise LinkageTransactionBuildError("Fund UTXO not found")
            
            # Get reference script UTXO
            ref_script_utxo = await self.scanner.get_reference_script_utxo(
                fund.fund_script_hash
            )
            if not ref_script_utxo:
                raise LinkageTransactionBuildError("Reference script UTXO not found")
            
            # Calculate tokens to withdraw
            withdraw_multi_asset = MultiAsset()
            for idx_token in fund_datum.immutable_params.index_tokens:
                pid = ScriptHash(idx_token.token.policy_id)
                name = AssetName(idx_token.token.token_name)
                if pid not in withdraw_multi_asset:
                    withdraw_multi_asset[pid] = Asset()
                withdraw_multi_asset[pid][name] = idx_token.factor * params.amount
            
            # Calculate new fund UTXO value
            new_fund_value = Value(
                coin=fund_utxo.output.amount.coin,
                multi_asset=fund_utxo.output.amount.multi_asset - withdraw_multi_asset,
            )
            
            # Datum stays the same (no royalty change on withdraw)
            output_datum = fund_datum.to_sanitized()
            
            # Outputs
            outputs = []
            
            # 1. Updated fund output
            fund_output = TransactionOutput(
                address=Address.from_primitive(self.config.fund_address),
                amount=new_fund_value,
                datum=RawCBOR(output_datum.to_cbor()),
            )
            outputs.append(fund_output)
            
            # 2. User receives underlying tokens
            user_output_value = self._estimate_min_lovelace(
                params.user_address, withdraw_multi_asset
            )
            user_output = TransactionOutput(
                address=params.user_address,
                amount=Value(coin=user_output_value, multi_asset=withdraw_multi_asset),
            )
            outputs.append(user_output)
            
            # Burn fund tokens (negative mint)
            fund_token_name = AssetName(bytes.fromhex(fund.fund_token_name))
            mint = MultiAsset()
            mint[fund.fund_script_hash] = Asset()
            mint[fund.fund_script_hash][fund_token_name] = -params.amount
            
            # Build transaction body
            tx = self._build_script_transaction(
                script_input=fund_utxo,
                ref_script=ref_script_utxo,
                redeemer=BurnRedeemer(),
                outputs=outputs,
                mint=mint,
                user_address=params.user_address,
            )
            
            return tx
            
        except LinkageTransactionBuildError:
            raise
        except Exception as e:
            logger.error("build_withdraw_tx_failed", error=str(e))
            raise LinkageTransactionBuildError(f"Failed to build withdraw: {e}")
    
    async def build_collect_royalty_transaction(
        self,
        params: CollectRoyaltyParams,
    ) -> Transaction:
        """
        Build a royalty collection transaction.
        
        Collect royalty operations:
        1. Consume the fund UTXO
        2. Remove collected fund tokens from the fund
        3. Update the datum to reduce accrued royalty
        4. Send fund tokens to creator
        5. Require creator signature
        
        Args:
            params: Royalty collection parameters
            
        Returns:
            Built transaction (unsigned)
            
        Raises:
            LinkageTransactionBuildError: If transaction cannot be built
        """
        await self._ensure_chain_context()
        
        logger.info(
            "building_collect_royalty_tx",
            fund=params.fund_request.fund_name_readable,
            amount=params.amount,
        )
        
        try:
            fund = params.fund_request
            fund_datum = fund.fund_datum
            
            # Verify there's enough accrued royalty
            if params.amount > fund_datum.accrued_royalty:
                raise LinkageTransactionBuildError(
                    f"Insufficient accrued royalty: {fund_datum.accrued_royalty} < {params.amount}"
                )
            
            # Get the fund UTXO
            fund_utxo = await self._get_fund_utxo(fund)
            if not fund_utxo:
                raise LinkageTransactionBuildError("Fund UTXO not found")
            
            # Get reference script UTXO
            ref_script_utxo = await self.scanner.get_reference_script_utxo(
                fund.fund_script_hash
            )
            if not ref_script_utxo:
                raise LinkageTransactionBuildError("Reference script UTXO not found")
            
            # Calculate collected tokens
            fund_token_name = AssetName(bytes.fromhex(fund.fund_token_name))
            collected_tokens = MultiAsset()
            collected_tokens[fund.fund_script_hash] = Asset()
            collected_tokens[fund.fund_script_hash][fund_token_name] = params.amount
            
            # Calculate new fund UTXO value
            new_fund_value = Value(
                coin=fund_utxo.output.amount.coin,
                multi_asset=fund_utxo.output.amount.multi_asset - collected_tokens,
            )
            
            # Update datum with reduced royalty
            output_datum = IndexParameters(
                immutable_params=fund_datum.immutable_params,
                accrued_royalty=fund_datum.accrued_royalty - params.amount,
            ).to_sanitized()
            
            # Outputs
            outputs = []
            
            # 1. Updated fund output
            fund_output = TransactionOutput(
                address=Address.from_primitive(self.config.fund_address),
                amount=new_fund_value,
                datum=RawCBOR(output_datum.to_cbor()),
            )
            outputs.append(fund_output)
            
            # 2. Creator receives fund tokens
            creator_output_value = self._estimate_min_lovelace(
                params.creator_address, collected_tokens
            )
            creator_output = TransactionOutput(
                address=params.creator_address,
                amount=Value(coin=creator_output_value, multi_asset=collected_tokens),
            )
            outputs.append(creator_output)
            
            # Build transaction body with required signer
            # The creator must sign this transaction
            required_signers = [fund_datum.immutable_params.creator]
            
            tx = self._build_script_transaction(
                script_input=fund_utxo,
                ref_script=ref_script_utxo,
                redeemer=RoyaltyWithdrawalRedeemer(),
                outputs=outputs,
                mint=None,
                user_address=params.creator_address,
                required_signers=required_signers,
            )
            
            return tx
            
        except LinkageTransactionBuildError:
            raise
        except Exception as e:
            logger.error("build_collect_royalty_tx_failed", error=str(e))
            raise LinkageTransactionBuildError(f"Failed to build royalty collection: {e}")
    
    async def _get_fund_utxo(self, fund: LinkageRequest) -> Optional[UTxO]:
        """
        Get the current fund UTXO.
        
        Args:
            fund: Fund request with UTXO reference
            
        Returns:
            UTxO if found
        """
        try:
            # Check if the UTXO still exists at this reference
            tx_hash = str(fund.utxo_ref.transaction_id)
            output_index = fund.utxo_ref.index
            
            exists = await self.node.check_utxo_exists(tx_hash, output_index)
            if not exists:
                # UTXO has been spent, need to find new one
                return await self._find_fund_utxo_by_token(fund.fund_token_name)
            
            # Reconstruct UTxO from available data
            utxos = await self.node.get_utxos_at_address(self.config.fund_address)
            for utxo in utxos:
                if (str(utxo.input.transaction_id) == tx_hash and 
                    utxo.input.index == output_index):
                    return utxo
            
            return None
            
        except Exception as e:
            logger.error("get_fund_utxo_failed", error=str(e))
            return None
    
    async def _find_fund_utxo_by_token(
        self,
        fund_token_name: str,
    ) -> Optional[UTxO]:
        """
        Find a fund UTXO by its token name.
        
        Args:
            fund_token_name: Hex-encoded fund token name
            
        Returns:
            UTxO containing the fund or None
        """
        try:
            utxos = await self.node.get_utxos_at_address(self.config.fund_address)
            
            token_name = AssetName(bytes.fromhex(fund_token_name))
            auth_pid = self.config.authenticator_script_hash
            
            for utxo in utxos:
                if utxo.output.amount.multi_asset:
                    if auth_pid in utxo.output.amount.multi_asset:
                        if token_name in utxo.output.amount.multi_asset[auth_pid]:
                            return utxo
            
            return None
            
        except Exception as e:
            logger.error("find_fund_utxo_by_token_failed", error=str(e))
            return None
    
    def _build_script_transaction(
        self,
        script_input: UTxO,
        ref_script: UTxO,
        redeemer: Any,
        outputs: List[TransactionOutput],
        mint: Optional[MultiAsset],
        user_address: Address,
        required_signers: Optional[List[bytes]] = None,
    ) -> Transaction:
        """
        Build a transaction that interacts with a script.
        
        This is a simplified transaction builder - in production you'd want
        to use PyCardano's TransactionBuilder with a proper chain context.
        
        Args:
            script_input: The script UTXO being consumed
            ref_script: Reference script UTXO
            redeemer: Redeemer for the script
            outputs: Transaction outputs
            mint: Minting/burning operations
            user_address: Address for change
            required_signers: Additional required signers
            
        Returns:
            Built transaction
        """
        # Inputs
        inputs = [script_input.input]
        
        # Add reference input
        reference_inputs = [ref_script.input]
        
        # Calculate fee (simplified estimate)
        estimated_fee = 500_000  # 0.5 ADA conservative estimate
        
        # Build transaction body
        tx_body = TransactionBody(
            inputs=inputs,
            outputs=outputs,
            fee=estimated_fee,
            reference_inputs=reference_inputs,
        )
        
        # Add mint if present
        if mint:
            tx_body.mint = mint
        
        # Add required signers
        if required_signers:
            tx_body.required_signers = required_signers
        
        # Create witness set (redeemers would go here)
        # Note: In a full implementation, you'd serialize the redeemer properly
        witness_set = TransactionWitnessSet()
        
        return Transaction(tx_body, witness_set)
    
    async def refresh_protocol_params(self):
        """Refresh cached protocol parameters."""
        self._protocol_params = await self.node.get_protocol_parameters()


class LinkageBatchBuilder:
    """
    Builds batch transactions for multiple Linkage operations.
    
    This builder can combine multiple operations of the same type
    into a single transaction when possible.
    """
    
    def __init__(
        self,
        tx_builder: LinkageTransactionBuilder,
        config: LinkageConfig,
    ):
        """
        Initialize the batch builder.
        
        Args:
            tx_builder: Base transaction builder
            config: Linkage configuration
        """
        self.tx_builder = tx_builder
        self.config = config
    
    async def build_deposit_batch(
        self,
        deposits: List[DepositParams],
    ) -> List[Transaction]:
        """
        Build transactions for multiple deposits.
        
        Note: Due to the nature of Linkage contracts (each fund has its
        own UTXO), deposits to different funds cannot be batched together.
        This method will return one transaction per unique fund.
        
        Args:
            deposits: List of deposit parameters
            
        Returns:
            List of built transactions
        """
        transactions = []
        
        # Group by fund
        by_fund: Dict[str, List[DepositParams]] = {}
        for deposit in deposits:
            fund_id = deposit.fund_request.fund_token_name
            if fund_id not in by_fund:
                by_fund[fund_id] = []
            by_fund[fund_id].append(deposit)
        
        # Build one transaction per fund
        # (In future, could combine multiple deposits to same fund)
        for fund_id, fund_deposits in by_fund.items():
            for deposit in fund_deposits:
                try:
                    tx = await self.tx_builder.build_deposit_transaction(deposit)
                    transactions.append(tx)
                except Exception as e:
                    logger.error(
                        "batch_deposit_build_failed",
                        fund=fund_id,
                        error=str(e),
                    )
        
        return transactions
    
    async def build_withdraw_batch(
        self,
        withdrawals: List[WithdrawParams],
    ) -> List[Transaction]:
        """
        Build transactions for multiple withdrawals.
        
        Args:
            withdrawals: List of withdraw parameters
            
        Returns:
            List of built transactions
        """
        transactions = []
        
        for withdraw in withdrawals:
            try:
                tx = await self.tx_builder.build_withdraw_transaction(withdraw)
                transactions.append(tx)
            except Exception as e:
                logger.error(
                    "batch_withdraw_build_failed",
                    fund=withdraw.fund_request.fund_token_name,
                    error=str(e),
                )
        
        return transactions

