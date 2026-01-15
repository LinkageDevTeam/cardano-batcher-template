"""
Linkage Finance Batcher Configuration.

Provides configuration for the Linkage-specific batcher, including
contract addresses, script hashes, and operational parameters.
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

from pycardano import Address, ScriptHash, Network


class LinkageNetwork(str, Enum):
    """Supported networks for Linkage Finance."""
    MAINNET = "mainnet"
    PREPROD = "preprod"
    DEVNET = "devnet"


# =============================================================================
# Network-specific Configuration
# =============================================================================

# Preprod testnet addresses (from backend-new/src/linkage_server/common/config.py)
PREPROD_CONFIG = {
    "fund_address": "addr_test1wz5mnvsn3wupa2nvsg56s0rc30khypzxksrctmx2hysf2pcxt3ntm",
    "auth_script_hash": "d53f21c7e9cdc4d91612edf9bdbd511bfaacc03fbaaa1fd167e8dfef",
    "ref_script_address": "addr_test1vplssjr39yv2mqavm2ap2wk0wz6jpge9afvdgjn7hn2l8ggfvxgws",
    "factory_address": "addr_test1wzeacx32pnf53uawkwrvuyprwsm3ftfnp47mq8cvfs60ngcw2nxmr",
    "blockfrost_base_url": "https://cardano-preprod.blockfrost.io/api",
    "ogmios_port": 1338,
}

# Mainnet addresses (to be configured)
MAINNET_CONFIG = {
    "fund_address": "",  # To be configured for mainnet deployment
    "auth_script_hash": "",
    "ref_script_address": "",
    "factory_address": "",
    "blockfrost_base_url": "https://cardano-mainnet.blockfrost.io/api",
    "ogmios_port": 1337,
}

# Devnet addresses (local development)
DEVNET_CONFIG = {
    "fund_address": "",  # Set dynamically during devnet setup
    "auth_script_hash": "",
    "ref_script_address": "",
    "factory_address": "",
    "blockfrost_base_url": "http://localhost:8080/api",
    "ogmios_port": 1337,
}


@dataclass
class LinkageConfig:
    """
    Configuration for the Linkage Finance Batcher.
    
    This configuration extends the base batcher config with Linkage-specific
    settings for smart contract interaction.
    
    Attributes:
        network: Target network (mainnet, preprod, devnet)
        fund_address: Address of the fund validator contract
        auth_script_hash: Hash of the authenticator/minting script
        ref_script_address: Address where reference scripts are stored
        factory_address: Address of the factory contract
        blockfrost_project_id: Blockfrost API key
        blockfrost_base_url: Blockfrost API base URL
        ogmios_host: Ogmios server host
        ogmios_port: Ogmios server port
        signing_key_path: Path to batcher signing key
        contracts_dir: Directory containing contract CBOR files
        use_compressed_scripts: Whether to use compressed script versions
        batch_size_min: Minimum requests per batch
        batch_size_max: Maximum requests per batch
        batch_interval_seconds: Seconds between batch processing cycles
        confirmation_timeout: Seconds to wait for transaction confirmation
        max_retries: Maximum retry attempts for failed batches
        database_url: SQLite database URL for state persistence
        data_dir: Directory for runtime data
    """
    
    # Network configuration
    network: LinkageNetwork = LinkageNetwork.PREPROD
    
    # Contract addresses
    fund_address: str = ""
    auth_script_hash: str = ""
    ref_script_address: str = ""
    factory_address: str = ""
    
    # Node access
    blockfrost_project_id: str = ""
    blockfrost_base_url: str = ""
    ogmios_host: str = "localhost"
    ogmios_port: int = 1337
    use_ogmios: bool = False
    
    # Keys
    signing_key_path: Optional[str] = None
    
    # Contracts
    contracts_dir: str = "contracts"
    use_compressed_scripts: bool = True
    
    # Batching parameters
    batch_size_min: int = 1  # Linkage can process single requests
    batch_size_max: int = 5  # Conservative limit for script execution
    batch_interval_seconds: int = 30
    confirmation_timeout: int = 120
    max_retries: int = 3
    
    # Persistence
    database_url: str = "sqlite:///linkage_batcher.db"
    data_dir: str = "data"
    
    # Reference UTXO caching
    ref_utxo_cache_file: str = "ref_utxo_cache.json"
    
    def __post_init__(self):
        """Apply network-specific defaults."""
        self._apply_network_config()
        self._ensure_data_dir()
    
    def _apply_network_config(self):
        """Apply network-specific configuration defaults."""
        if self.network == LinkageNetwork.PREPROD:
            config = PREPROD_CONFIG
        elif self.network == LinkageNetwork.MAINNET:
            config = MAINNET_CONFIG
        else:
            config = DEVNET_CONFIG
        
        # Apply defaults only if not explicitly set
        if not self.fund_address:
            self.fund_address = config["fund_address"]
        if not self.auth_script_hash:
            self.auth_script_hash = config["auth_script_hash"]
        if not self.ref_script_address:
            self.ref_script_address = config["ref_script_address"]
        if not self.factory_address:
            self.factory_address = config["factory_address"]
        if not self.blockfrost_base_url:
            self.blockfrost_base_url = config["blockfrost_base_url"]
        if self.ogmios_port == 1337 and config["ogmios_port"] != 1337:
            self.ogmios_port = config["ogmios_port"]
    
    def _ensure_data_dir(self):
        """Ensure data directory exists."""
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def from_env(cls) -> "LinkageConfig":
        """
        Create configuration from environment variables.
        
        Environment variables:
            LINKAGE_NETWORK: Network (mainnet, preprod, devnet)
            LINKAGE_FUND_ADDRESS: Fund contract address
            LINKAGE_AUTH_SCRIPT_HASH: Auth script hash
            LINKAGE_REF_SCRIPT_ADDRESS: Reference script address
            LINKAGE_FACTORY_ADDRESS: Factory address
            LINKAGE_BLOCKFROST_PROJECT_ID: Blockfrost API key
            LINKAGE_SIGNING_KEY_PATH: Path to signing key
            LINKAGE_CONTRACTS_DIR: Path to contracts directory
            LINKAGE_BATCH_SIZE_MIN: Minimum batch size
            LINKAGE_BATCH_SIZE_MAX: Maximum batch size
            LINKAGE_BATCH_INTERVAL: Batch interval in seconds
            LINKAGE_USE_OGMIOS: Use Ogmios instead of Blockfrost
            LINKAGE_OGMIOS_HOST: Ogmios host
            LINKAGE_OGMIOS_PORT: Ogmios port
            LINKAGE_DATABASE_URL: Database URL
            LINKAGE_DATA_DIR: Data directory
        
        Returns:
            LinkageConfig instance
        """
        network_str = os.getenv("LINKAGE_NETWORK", "preprod").lower()
        network = LinkageNetwork(network_str)
        
        return cls(
            network=network,
            fund_address=os.getenv("LINKAGE_FUND_ADDRESS", ""),
            auth_script_hash=os.getenv("LINKAGE_AUTH_SCRIPT_HASH", ""),
            ref_script_address=os.getenv("LINKAGE_REF_SCRIPT_ADDRESS", ""),
            factory_address=os.getenv("LINKAGE_FACTORY_ADDRESS", ""),
            blockfrost_project_id=os.getenv("LINKAGE_BLOCKFROST_PROJECT_ID", ""),
            signing_key_path=os.getenv("LINKAGE_SIGNING_KEY_PATH"),
            contracts_dir=os.getenv("LINKAGE_CONTRACTS_DIR", "contracts"),
            batch_size_min=int(os.getenv("LINKAGE_BATCH_SIZE_MIN", "1")),
            batch_size_max=int(os.getenv("LINKAGE_BATCH_SIZE_MAX", "5")),
            batch_interval_seconds=int(os.getenv("LINKAGE_BATCH_INTERVAL", "30")),
            use_ogmios=os.getenv("LINKAGE_USE_OGMIOS", "false").lower() == "true",
            ogmios_host=os.getenv("LINKAGE_OGMIOS_HOST", "localhost"),
            ogmios_port=int(os.getenv("LINKAGE_OGMIOS_PORT", "1337")),
            database_url=os.getenv("LINKAGE_DATABASE_URL", "sqlite:///linkage_batcher.db"),
            data_dir=os.getenv("LINKAGE_DATA_DIR", "data"),
        )
    
    @property
    def pycardano_network(self) -> Network:
        """Get PyCardano Network enum."""
        if self.network == LinkageNetwork.MAINNET:
            return Network.MAINNET
        return Network.TESTNET
    
    @property
    def fund_script_hash(self) -> ScriptHash:
        """Get fund script hash from address."""
        addr = Address.from_primitive(self.fund_address)
        return addr.payment_part
    
    @property
    def factory_script_hash(self) -> ScriptHash:
        """Get factory script hash from address."""
        addr = Address.from_primitive(self.factory_address)
        return addr.payment_part
    
    @property
    def authenticator_script_hash(self) -> ScriptHash:
        """Get authenticator script hash."""
        return ScriptHash(bytes.fromhex(self.auth_script_hash))
    
    @property
    def ref_utxo_cache_path(self) -> Path:
        """Get path to reference UTXO cache file."""
        return Path(self.data_dir) / self.ref_utxo_cache_file
    
    def validate(self) -> bool:
        """
        Validate configuration.
        
        Returns:
            True if configuration is valid
            
        Raises:
            ValueError: If configuration is invalid
        """
        errors = []
        
        if not self.fund_address:
            errors.append("fund_address is required")
        
        if not self.auth_script_hash:
            errors.append("auth_script_hash is required")
        
        if not self.ref_script_address:
            errors.append("ref_script_address is required")
        
        if not self.use_ogmios and not self.blockfrost_project_id:
            errors.append("blockfrost_project_id is required when not using Ogmios")
        
        if self.signing_key_path and not Path(self.signing_key_path).exists():
            errors.append(f"signing_key_path does not exist: {self.signing_key_path}")
        
        if errors:
            raise ValueError(f"Invalid configuration: {', '.join(errors)}")
        
        return True
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "network": self.network.value,
            "fund_address": self.fund_address,
            "auth_script_hash": self.auth_script_hash,
            "ref_script_address": self.ref_script_address,
            "factory_address": self.factory_address,
            "blockfrost_project_id": "***" if self.blockfrost_project_id else None,
            "use_ogmios": self.use_ogmios,
            "contracts_dir": self.contracts_dir,
            "batch_size_min": self.batch_size_min,
            "batch_size_max": self.batch_size_max,
            "batch_interval_seconds": self.batch_interval_seconds,
        }


# Verification NFT name used in factory
VERIFICATION_NFT_NAME = b"VERIFY"

