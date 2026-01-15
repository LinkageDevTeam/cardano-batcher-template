"""
Linkage Finance Data Types.

Contains all data structures used by the Linkage Finance smart contracts,
including datums, redeemers, and request types.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from hashlib import sha256

from pycardano import (
    PlutusData,
    AssetName,
    ScriptHash,
    VerificationKeyHash,
    IndefiniteList,
    PolicyId,
    Value,
    TransactionInput,
    Address,
)


class LinkageRequestType(str, Enum):
    """Types of Linkage Finance batch requests."""
    DEPOSIT = "deposit"
    WITHDRAW = "withdraw"
    COLLECT_ROYALTY = "collect_royalty"


# =============================================================================
# On-chain Plutus Data Types
# =============================================================================

@dataclass
class TokenData(PlutusData):
    """
    Token representation used in Linkage smart contracts.
    
    This matches the opshin Token type structure.
    """
    CONSTR_ID = 0
    policy_id: bytes
    token_name: bytes


@dataclass
class IndexedToken(PlutusData):
    """
    Token with associated factor for fund index.
    
    Each token in a fund has a factor that determines how many units
    of that token correspond to one fund token unit.
    """
    CONSTR_ID = 0
    token: TokenData
    factor: int


@dataclass
class ImmutableParams(PlutusData):
    """
    Immutable parameters of a Linkage Finance fund.
    
    These parameters are set at fund creation and cannot be changed.
    
    Attributes:
        index_tokens: List of tokens that make up the fund index
        index_name: Human-readable name of the fund (max 32 bytes)
        creator: Public key hash of the fund creator
        fund_token_factor: Base factor for fund tokens
        royalty_factor: Numerator of royalty fee (e.g., 3 for 0.3%)
    """
    CONSTR_ID = 0
    index_tokens: List[IndexedToken]
    index_name: bytes
    creator: bytes  # PubKeyHash
    fund_token_factor: int
    royalty_factor: int


@dataclass
class IndexParameters(PlutusData):
    """
    Complete fund parameters including mutable state.
    
    This is the main datum stored in fund UTXOs.
    
    Attributes:
        immutable_params: The fixed fund configuration
        accrued_royalty: Current uncollected royalty amount
    """
    CONSTR_ID = 0
    immutable_params: ImmutableParams
    accrued_royalty: int
    
    def to_sanitized(self) -> "IndexParameters":
        """
        Ensure index_tokens is an IndefiniteList for proper serialization.
        
        This is required for matching on-chain CBOR exactly.
        """
        if isinstance(self.immutable_params.index_tokens, IndefiniteList):
            return self
        
        return IndexParameters(
            immutable_params=ImmutableParams(
                index_tokens=IndefiniteList(self.immutable_params.index_tokens),
                index_name=self.immutable_params.index_name,
                creator=self.immutable_params.creator,
                fund_token_factor=self.immutable_params.fund_token_factor,
                royalty_factor=self.immutable_params.royalty_factor,
            ),
            accrued_royalty=self.accrued_royalty,
        )


@dataclass
class MiniDatum(PlutusData):
    """Minimal datum containing only royalty information."""
    CONSTR_ID = 0
    accrued_royalty: int


# =============================================================================
# Redeemer Types
# =============================================================================

@dataclass
class BurnRedeemer(PlutusData):
    """Redeemer for withdraw/burn operations."""
    CONSTR_ID = 1


@dataclass
class MintRedeemer(PlutusData):
    """Redeemer for deposit/mint operations."""
    CONSTR_ID = 2


@dataclass
class RoyaltyWithdrawalRedeemer(PlutusData):
    """Redeemer for royalty collection operations."""
    CONSTR_ID = 3


@dataclass
class InitialiseRedeemer(PlutusData):
    """Redeemer for factory initialization."""
    CONSTR_ID = 1


@dataclass
class FundCreationRedeemer(PlutusData):
    """Redeemer for fund creation."""
    CONSTR_ID = 2
    token_name: bytes


# =============================================================================
# Helper Functions
# =============================================================================

def token_name_from_parameters(params: IndexParameters) -> bytes:
    """
    Calculate the fund token name from parameters.
    
    The token name is derived from the fund name and a hash of the immutable params.
    
    Args:
        params: The fund parameters
        
    Returns:
        32-byte token name
    """
    # First 8 bytes from the index name
    prefix = params.immutable_params.index_name[:8]
    
    # Hash of immutable params for the remaining 24 bytes
    params_hash = sha256(params.immutable_params.to_cbor()).digest()[:24]
    
    return prefix + params_hash


def parse_fund_token_name(fund_name_hex: str) -> bytes:
    """
    Parse a fund token name from hex string.
    
    Args:
        fund_name_hex: Hex-encoded fund token name
        
    Returns:
        Raw bytes of the token name
    """
    return bytes.fromhex(fund_name_hex)


# =============================================================================
# Linkage Request Type (extends BatchRequest concept)
# =============================================================================

@dataclass
class LinkageRequest:
    """
    Represents a Linkage Finance batching request.
    
    This extends the basic BatchRequest with Linkage-specific fields.
    
    Attributes:
        request_id: Unique identifier (tx_hash#index)
        utxo_ref: Reference to the fund UTXO
        fund_datum: Parsed fund parameters
        request_type: Type of operation (deposit, withdraw, royalty)
        user_address: Address of the requesting user
        amount: Operation amount (multiple for deposit, token amount for withdraw)
        fund_token_name: The fund's token name (hex)
        fund_script_hash: Script hash of the fund contract
        value: Value locked in the UTXO
        created_at: When the request was identified
        metadata: Additional request metadata
    """
    
    request_id: str
    utxo_ref: TransactionInput
    fund_datum: IndexParameters
    request_type: LinkageRequestType
    user_address: str
    amount: int
    fund_token_name: str  # hex
    fund_script_hash: ScriptHash
    value: Value
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def fund_name_readable(self) -> str:
        """Get human-readable fund name."""
        try:
            name_bytes = self.fund_datum.immutable_params.index_name
            return name_bytes.decode('utf-8').rstrip('\x00')
        except:
            return self.fund_token_name[:16] + "..."
    
    @property
    def royalty_factor(self) -> int:
        """Get the fund's royalty factor."""
        return self.fund_datum.immutable_params.royalty_factor
    
    @property
    def fund_token_factor(self) -> int:
        """Get the fund's token factor."""
        return self.fund_datum.immutable_params.fund_token_factor
    
    @property
    def accrued_royalty(self) -> int:
        """Get current accrued royalty."""
        return self.fund_datum.accrued_royalty
    
    def calculate_deposit_tokens(self, multiple: int) -> Dict[str, int]:
        """
        Calculate tokens required for a deposit.
        
        Args:
            multiple: Deposit multiple
            
        Returns:
            Dict mapping token identifiers to amounts
        """
        tokens = {}
        for idx_token in self.fund_datum.immutable_params.index_tokens:
            token_id = idx_token.token.policy_id.hex() + idx_token.token.token_name.hex()
            tokens[token_id] = idx_token.factor * multiple * self.fund_token_factor
        return tokens
    
    def calculate_withdraw_tokens(self, amount: int) -> Dict[str, int]:
        """
        Calculate tokens returned for a withdrawal.
        
        Args:
            amount: Number of fund tokens to burn
            
        Returns:
            Dict mapping token identifiers to amounts
        """
        tokens = {}
        for idx_token in self.fund_datum.immutable_params.index_tokens:
            token_id = idx_token.token.policy_id.hex() + idx_token.token.token_name.hex()
            tokens[token_id] = idx_token.factor * amount
        return tokens
    
    def calculate_royalty_accrual(self, multiple: int) -> int:
        """
        Calculate royalty accrual for a deposit.
        
        Args:
            multiple: Deposit multiple
            
        Returns:
            Royalty amount in fund tokens
        """
        return (self.royalty_factor * multiple) // self.fund_token_factor
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "request_id": self.request_id,
            "request_type": self.request_type.value,
            "user_address": self.user_address,
            "amount": self.amount,
            "fund_token_name": self.fund_token_name,
            "fund_name": self.fund_name_readable,
            "royalty_factor": self.royalty_factor,
            "fund_token_factor": self.fund_token_factor,
            "accrued_royalty": self.accrued_royalty,
            "created_at": self.created_at.isoformat(),
            "metadata": self.metadata,
        }


@dataclass
class LinkageBatchResult:
    """
    Result of processing a Linkage batch.
    
    Attributes:
        success: Whether the batch was processed successfully
        transaction_hash: Hash of the submitted transaction
        requests_processed: Number of requests in the batch
        total_fund_tokens_minted: Total fund tokens minted (deposits)
        total_fund_tokens_burned: Total fund tokens burned (withdrawals)
        total_royalty_collected: Total royalty collected
        error_message: Error message if failed
    """
    success: bool
    transaction_hash: Optional[str] = None
    requests_processed: int = 0
    total_fund_tokens_minted: int = 0
    total_fund_tokens_burned: int = 0
    total_royalty_collected: int = 0
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "transaction_hash": self.transaction_hash,
            "requests_processed": self.requests_processed,
            "total_fund_tokens_minted": self.total_fund_tokens_minted,
            "total_fund_tokens_burned": self.total_fund_tokens_burned,
            "total_royalty_collected": self.total_royalty_collected,
            "error_message": self.error_message,
        }

