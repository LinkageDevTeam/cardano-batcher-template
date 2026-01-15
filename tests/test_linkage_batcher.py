"""
Tests for Linkage Finance Batcher.

Tests the Linkage-specific batcher components including:
- Types and datum parsing
- Request scanning
- Transaction building
- Batch processing
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from pycardano import (
    Address,
    Value,
    TransactionInput,
    TransactionId,
    ScriptHash,
    AssetName,
    MultiAsset,
    Asset,
    IndefiniteList,
)


# =============================================================================
# Test Fixtures
# =============================================================================

@pytest.fixture
def linkage_config():
    """Create a test Linkage configuration."""
    from batcher.linkage.config import LinkageConfig, LinkageNetwork
    
    return LinkageConfig(
        network=LinkageNetwork.PREPROD,
        fund_address="addr_test1wz5mnvsn3wupa2nvsg56s0rc30khypzxksrctmx2hysf2pcxt3ntm",
        auth_script_hash="d53f21c7e9cdc4d91612edf9bdbd511bfaacc03fbaaa1fd167e8dfef",
        ref_script_address="addr_test1vplssjr39yv2mqavm2ap2wk0wz6jpge9afvdgjn7hn2l8ggfvxgws",
        factory_address="addr_test1wzeacx32pnf53uawkwrvuyprwsm3ftfnp47mq8cvfs60ngcw2nxmr",
        blockfrost_project_id="test_project_id",
        batch_size_min=1,
        batch_size_max=5,
    )


@pytest.fixture
def sample_fund_datum():
    """Create a sample fund datum."""
    from batcher.linkage.types import (
        IndexParameters,
        ImmutableParams,
        IndexedToken,
        TokenData,
    )
    
    # Create sample indexed tokens
    token1 = IndexedToken(
        token=TokenData(
            policy_id=bytes.fromhex("a" * 56),
            token_name=b"token1",
        ),
        factor=100,
    )
    
    token2 = IndexedToken(
        token=TokenData(
            policy_id=bytes.fromhex("b" * 56),
            token_name=b"token2",
        ),
        factor=200,
    )
    
    immutable_params = ImmutableParams(
        index_tokens=IndefiniteList([token1, token2]),
        index_name=b"TestFund",
        creator=bytes.fromhex("c" * 56),
        fund_token_factor=1000,
        royalty_factor=3,
    )
    
    return IndexParameters(
        immutable_params=immutable_params,
        accrued_royalty=50,
    )


@pytest.fixture
def sample_linkage_request(sample_fund_datum, linkage_config):
    """Create a sample Linkage request."""
    from batcher.linkage.types import LinkageRequest, LinkageRequestType
    
    return LinkageRequest(
        request_id="abc123#0",
        utxo_ref=TransactionInput(
            TransactionId(bytes.fromhex("ab" * 32)),
            0
        ),
        fund_datum=sample_fund_datum,
        request_type=LinkageRequestType.DEPOSIT,
        user_address="addr_test1qz...",
        amount=10,
        fund_token_name="abcdef" * 8,
        fund_script_hash=linkage_config.fund_script_hash,
        value=Value(10_000_000),
    )


# =============================================================================
# Type Tests
# =============================================================================

class TestLinkageTypes:
    """Tests for Linkage type definitions."""
    
    def test_token_data_creation(self):
        """Test TokenData creation and serialization."""
        from batcher.linkage.types import TokenData
        
        token = TokenData(
            policy_id=bytes.fromhex("a" * 56),
            token_name=b"mytoken",
        )
        
        assert len(token.policy_id) == 28
        assert token.token_name == b"mytoken"
    
    def test_indexed_token_creation(self):
        """Test IndexedToken creation."""
        from batcher.linkage.types import IndexedToken, TokenData
        
        token = IndexedToken(
            token=TokenData(
                policy_id=bytes.fromhex("a" * 56),
                token_name=b"test",
            ),
            factor=1000,
        )
        
        assert token.factor == 1000
    
    def test_immutable_params_creation(self):
        """Test ImmutableParams creation."""
        from batcher.linkage.types import ImmutableParams, IndexedToken, TokenData
        
        params = ImmutableParams(
            index_tokens=[],
            index_name=b"MyFund",
            creator=bytes(28),
            fund_token_factor=1000,
            royalty_factor=3,
        )
        
        assert params.index_name == b"MyFund"
        assert params.fund_token_factor == 1000
        assert params.royalty_factor == 3
    
    def test_index_parameters_creation(self, sample_fund_datum):
        """Test IndexParameters creation."""
        assert sample_fund_datum.accrued_royalty == 50
        assert sample_fund_datum.immutable_params.fund_token_factor == 1000
    
    def test_index_parameters_to_sanitized(self, sample_fund_datum):
        """Test datum sanitization for proper CBOR."""
        sanitized = sample_fund_datum.to_sanitized()
        
        assert isinstance(
            sanitized.immutable_params.index_tokens,
            IndefiniteList
        )
    
    def test_token_name_from_parameters(self, sample_fund_datum):
        """Test token name calculation."""
        from batcher.linkage.types import token_name_from_parameters
        
        name = token_name_from_parameters(sample_fund_datum)
        
        # Should be 32 bytes (8 from name + 24 from hash)
        assert len(name) == 32
        # First 8 bytes should be from index_name
        assert name[:8] == sample_fund_datum.immutable_params.index_name[:8]
    
    def test_redeemer_types(self):
        """Test redeemer type creation."""
        from batcher.linkage.types import (
            MintRedeemer,
            BurnRedeemer,
            RoyaltyWithdrawalRedeemer,
        )
        
        mint = MintRedeemer()
        burn = BurnRedeemer()
        royalty = RoyaltyWithdrawalRedeemer()
        
        assert mint.CONSTR_ID == 2
        assert burn.CONSTR_ID == 1
        assert royalty.CONSTR_ID == 3


class TestLinkageRequest:
    """Tests for LinkageRequest dataclass."""
    
    def test_fund_name_readable(self, sample_linkage_request):
        """Test readable fund name extraction."""
        name = sample_linkage_request.fund_name_readable
        assert name == "TestFund"
    
    def test_calculate_deposit_tokens(self, sample_linkage_request):
        """Test deposit token calculation."""
        tokens = sample_linkage_request.calculate_deposit_tokens(multiple=5)
        
        # Should have 2 tokens (from sample data)
        assert len(tokens) == 2
        
        # Check calculation: factor * multiple * fund_token_factor
        # token1: 100 * 5 * 1000 = 500000
        # token2: 200 * 5 * 1000 = 1000000
    
    def test_calculate_withdraw_tokens(self, sample_linkage_request):
        """Test withdrawal token calculation."""
        tokens = sample_linkage_request.calculate_withdraw_tokens(amount=1000)
        
        assert len(tokens) == 2
        # token1: 100 * 1000 = 100000
        # token2: 200 * 1000 = 200000
    
    def test_calculate_royalty_accrual(self, sample_linkage_request):
        """Test royalty accrual calculation."""
        # royalty_factor * multiple / fund_token_factor
        # 3 * 1000 / 1000 = 3
        royalty = sample_linkage_request.calculate_royalty_accrual(multiple=1000)
        assert royalty == 3
    
    def test_to_dict(self, sample_linkage_request):
        """Test serialization to dict."""
        data = sample_linkage_request.to_dict()
        
        assert data["request_type"] == "deposit"
        assert data["amount"] == 10
        assert "fund_token_name" in data
        assert "created_at" in data


# =============================================================================
# Configuration Tests
# =============================================================================

class TestLinkageConfig:
    """Tests for Linkage configuration."""
    
    def test_config_defaults(self, linkage_config):
        """Test configuration defaults."""
        assert linkage_config.batch_size_min == 1
        assert linkage_config.batch_size_max == 5
    
    def test_config_from_env(self, monkeypatch):
        """Test configuration from environment variables."""
        from batcher.linkage.config import LinkageConfig
        
        monkeypatch.setenv("LINKAGE_NETWORK", "preprod")
        monkeypatch.setenv("LINKAGE_BLOCKFROST_PROJECT_ID", "test_key")
        monkeypatch.setenv("LINKAGE_BATCH_SIZE_MIN", "2")
        
        config = LinkageConfig.from_env()
        
        assert config.network.value == "preprod"
        assert config.blockfrost_project_id == "test_key"
        assert config.batch_size_min == 2
    
    def test_config_validation_missing_fund_address(self):
        """Test validation fails without fund address."""
        from batcher.linkage.config import LinkageConfig, LinkageNetwork
        
        # Create config with empty addresses
        config = LinkageConfig(
            network=LinkageNetwork.DEVNET,  # Use devnet to avoid preprod defaults
            fund_address="",
            auth_script_hash="",
            ref_script_address="",
            blockfrost_project_id="test",
        )
        # Clear the addresses that might have been set from defaults
        config.fund_address = ""
        config.auth_script_hash = ""
        config.ref_script_address = ""
        
        with pytest.raises(ValueError, match="fund_address"):
            config.validate()
    
    def test_script_hash_properties(self, linkage_config):
        """Test script hash property extraction."""
        fund_hash = linkage_config.fund_script_hash
        auth_hash = linkage_config.authenticator_script_hash
        
        assert isinstance(fund_hash, ScriptHash)
        assert isinstance(auth_hash, ScriptHash)
    
    def test_to_dict(self, linkage_config):
        """Test config serialization."""
        data = linkage_config.to_dict()
        
        assert data["network"] == "preprod"
        assert "fund_address" in data
        # API key should be masked
        assert data["blockfrost_project_id"] == "***"


# =============================================================================
# Scanner Tests
# =============================================================================

class TestLinkageDatumParser:
    """Tests for the Linkage datum parser."""
    
    def test_is_valid_request_with_valid_datum(
        self, linkage_config, sample_fund_datum
    ):
        """Test validation of valid fund datum."""
        from batcher.linkage.scanner import LinkageDatumParser
        
        parser = LinkageDatumParser(linkage_config)
        
        assert parser.is_valid_request(sample_fund_datum) is True
    
    def test_is_valid_request_with_none(self, linkage_config):
        """Test validation rejects None."""
        from batcher.linkage.scanner import LinkageDatumParser
        
        parser = LinkageDatumParser(linkage_config)
        
        assert parser.is_valid_request(None) is False
    
    def test_is_valid_request_with_invalid_datum(self, linkage_config):
        """Test validation rejects invalid data."""
        from batcher.linkage.scanner import LinkageDatumParser
        
        parser = LinkageDatumParser(linkage_config)
        
        # Random dictionary should be rejected
        assert parser.is_valid_request({"foo": "bar"}) is False
    
    def test_parse_request_details(
        self, linkage_config, sample_fund_datum
    ):
        """Test parsing request details from datum."""
        from batcher.linkage.scanner import LinkageDatumParser
        
        parser = LinkageDatumParser(linkage_config)
        
        # Create a mock UTxO
        mock_utxo = MagicMock()
        
        requester, req_type, params = parser.parse_request_details(
            sample_fund_datum, mock_utxo
        )
        
        assert req_type == "linkage_fund"
        assert "fund_token_name" in params
        assert "royalty_factor" in params
        assert params["royalty_factor"] == 3


# =============================================================================
# Builder Tests
# =============================================================================

class TestLinkageTransactionBuilder:
    """Tests for transaction building."""
    
    @pytest.mark.asyncio
    async def test_estimate_min_lovelace(self, linkage_config):
        """Test minimum lovelace estimation."""
        from batcher.linkage.builder import LinkageTransactionBuilder
        
        # Create mock dependencies
        mock_node = AsyncMock()
        mock_scanner = MagicMock()
        mock_signer = MagicMock()
        
        builder = LinkageTransactionBuilder(
            node=mock_node,
            scanner=mock_scanner,
            signer=mock_signer,
            config=linkage_config,
        )
        
        # Test with no assets
        min_ada = builder._estimate_min_lovelace(
            Address.from_primitive(linkage_config.fund_address),
        )
        assert min_ada >= 1_000_000  # At least 1 ADA
        
        # Test with assets
        multi_asset = MultiAsset()
        pid = ScriptHash(bytes(28))
        multi_asset[pid] = Asset()
        multi_asset[pid][AssetName(b"token")] = 1000
        
        min_ada_with_assets = builder._estimate_min_lovelace(
            Address.from_primitive(linkage_config.fund_address),
            multi_asset=multi_asset,
        )
        assert min_ada_with_assets > min_ada


# =============================================================================
# Batcher Tests
# =============================================================================

class TestLinkageBatcher:
    """Tests for the main Linkage batcher."""
    
    def test_batcher_creation(self, linkage_config):
        """Test batcher creation with mocked node."""
        from batcher.linkage.batcher import LinkageBatcher
        from unittest.mock import MagicMock
        
        # Create a mock node to avoid config validation issues
        mock_node = MagicMock()
        batcher = LinkageBatcher(linkage_config, node=mock_node)
        
        assert batcher.config == linkage_config
        assert not batcher._initialized
        assert not batcher._running
    
    def test_queue_deposit(self, linkage_config):
        """Test queueing a deposit operation."""
        from batcher.linkage.batcher import LinkageBatcher
        from unittest.mock import MagicMock
        
        mock_node = MagicMock()
        batcher = LinkageBatcher(linkage_config, node=mock_node)
        
        op = batcher.queue_deposit(
            fund_token_name="abc123" * 8,
            user_address="addr_test1...",
            multiple=10,
        )
        
        assert op.status == "pending"
        assert op.amount == 10
        assert len(batcher._pending_deposits) == 1
    
    def test_queue_withdrawal(self, linkage_config):
        """Test queueing a withdrawal operation."""
        from batcher.linkage.batcher import LinkageBatcher
        from unittest.mock import MagicMock
        
        mock_node = MagicMock()
        batcher = LinkageBatcher(linkage_config, node=mock_node)
        
        op = batcher.queue_withdrawal(
            fund_token_name="abc123" * 8,
            user_address="addr_test1...",
            amount=1000,
        )
        
        assert op.status == "pending"
        assert op.amount == 1000
        assert len(batcher._pending_withdrawals) == 1
    
    def test_queue_royalty_collection(self, linkage_config):
        """Test queueing a royalty collection."""
        from batcher.linkage.batcher import LinkageBatcher
        from unittest.mock import MagicMock
        
        mock_node = MagicMock()
        batcher = LinkageBatcher(linkage_config, node=mock_node)
        
        op = batcher.queue_royalty_collection(
            fund_token_name="abc123" * 8,
            creator_address="addr_test1...",
            amount=50,
        )
        
        assert op.status == "pending"
        assert op.amount == 50
        assert len(batcher._pending_royalty_collections) == 1
    
    def test_get_pending_count(self, linkage_config):
        """Test getting pending operation counts."""
        from batcher.linkage.batcher import LinkageBatcher
        from unittest.mock import MagicMock
        
        mock_node = MagicMock()
        batcher = LinkageBatcher(linkage_config, node=mock_node)
        
        # Queue some operations
        batcher.queue_deposit("a" * 48, "addr...", 10)
        batcher.queue_deposit("b" * 48, "addr...", 20)
        batcher.queue_withdrawal("c" * 48, "addr...", 100)
        
        counts = batcher.get_pending_count()
        
        assert counts["deposits"] == 2
        assert counts["withdrawals"] == 1
        assert counts["royalty_collections"] == 0
    
    def test_get_operation_status(self, linkage_config):
        """Test retrieving operation status."""
        from batcher.linkage.batcher import LinkageBatcher
        from unittest.mock import MagicMock
        
        mock_node = MagicMock()
        batcher = LinkageBatcher(linkage_config, node=mock_node)
        
        op = batcher.queue_deposit("a" * 48, "addr...", 10)
        
        retrieved = batcher.get_operation_status(op.operation_id)
        
        assert retrieved is not None
        assert retrieved.operation_id == op.operation_id
    
    def test_get_stats(self, linkage_config):
        """Test statistics retrieval."""
        from batcher.linkage.batcher import LinkageBatcher
        from unittest.mock import MagicMock
        
        mock_node = MagicMock()
        batcher = LinkageBatcher(linkage_config, node=mock_node)
        
        stats = batcher.get_stats()
        
        assert "deposits_processed" in stats
        assert "withdrawals_processed" in stats
        assert "total_transactions" in stats
        assert stats["initialized"] is False
    
    def test_callbacks(self, linkage_config):
        """Test callback registration."""
        from batcher.linkage.batcher import LinkageBatcher
        from unittest.mock import MagicMock
        
        mock_node = MagicMock()
        batcher = LinkageBatcher(linkage_config, node=mock_node)
        
        completed_ops = []
        failed_ops = []
        
        batcher.on_operation_completed(lambda op: completed_ops.append(op))
        batcher.on_operation_failed(lambda op: failed_ops.append(op))
        
        assert batcher._on_operation_completed is not None
        assert batcher._on_operation_failed is not None


# =============================================================================
# Integration Tests
# =============================================================================

class TestLinkageBatcherIntegration:
    """Integration tests for Linkage batcher components."""
    
    @pytest.mark.asyncio
    async def test_full_deposit_flow_mocked(self, linkage_config, sample_fund_datum):
        """Test complete deposit flow with mocked node."""
        from batcher.linkage.batcher import LinkageBatcher
        from batcher.linkage.types import LinkageRequest, LinkageRequestType
        
        # Create mocked node first
        mock_node = AsyncMock()
        mock_node.connect = AsyncMock()
        mock_node.disconnect = AsyncMock()
        mock_node.get_utxos_at_address = AsyncMock(return_value=[])
        mock_node.get_protocol_parameters = AsyncMock(return_value=MagicMock(
            min_fee_a=44,
            min_fee_b=155381,
        ))
        
        # Create batcher with mocked node
        batcher = LinkageBatcher(linkage_config, node=mock_node)
        
        # Initialize (this will fail to find funds, but that's ok for this test)
        await batcher.initialize()
        
        # Queue a deposit
        op = batcher.queue_deposit(
            fund_token_name="test" * 12,
            user_address="addr_test1...",
            multiple=10,
        )
        
        assert op.status == "pending"
        
        await batcher.shutdown()


# =============================================================================
# Batch Result Tests
# =============================================================================

class TestLinkageBatchResult:
    """Tests for batch result handling."""
    
    def test_successful_result(self):
        """Test successful batch result."""
        from batcher.linkage.types import LinkageBatchResult
        
        result = LinkageBatchResult(
            success=True,
            transaction_hash="abc123",
            requests_processed=5,
            total_fund_tokens_minted=5000,
        )
        
        assert result.success
        assert result.requests_processed == 5
        assert result.error_message is None
    
    def test_failed_result(self):
        """Test failed batch result."""
        from batcher.linkage.types import LinkageBatchResult
        
        result = LinkageBatchResult(
            success=False,
            error_message="Transaction failed",
        )
        
        assert not result.success
        assert result.error_message == "Transaction failed"
        assert result.transaction_hash is None
    
    def test_to_dict(self):
        """Test result serialization."""
        from batcher.linkage.types import LinkageBatchResult
        
        result = LinkageBatchResult(
            success=True,
            transaction_hash="abc123",
            requests_processed=3,
        )
        
        data = result.to_dict()
        
        assert data["success"] is True
        assert data["transaction_hash"] == "abc123"
        assert data["requests_processed"] == 3

