"""
Configuration management for the Cardano Batcher.

Supports configuration via environment variables and .env files.
"""

from enum import Enum
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class NetworkType(str, Enum):
    """Cardano network types."""
    MAINNET = "mainnet"
    PREPROD = "preprod"
    PREVIEW = "preview"
    LOCAL = "local"


class NodeProvider(str, Enum):
    """Supported node providers for blockchain access."""
    BLOCKFROST = "blockfrost"
    OGMIOS = "ogmios"


class BatcherConfig(BaseSettings):
    """
    Configuration settings for the Cardano Batcher.
    
    All settings can be configured via environment variables with the BATCHER_ prefix.
    """
    
    model_config = SettingsConfigDict(
        env_prefix="BATCHER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    # Network settings
    network: NetworkType = Field(
        default=NetworkType.PREPROD,
        description="Cardano network to connect to"
    )
    
    # Node provider settings
    node_provider: NodeProvider = Field(
        default=NodeProvider.BLOCKFROST,
        description="Provider for blockchain access"
    )
    
    # Blockfrost settings
    blockfrost_project_id: Optional[str] = Field(
        default=None,
        description="Blockfrost project ID for API access"
    )
    blockfrost_base_url: Optional[str] = Field(
        default=None,
        description="Custom Blockfrost base URL (optional)"
    )
    
    # Ogmios settings
    ogmios_host: str = Field(
        default="localhost",
        description="Ogmios server host"
    )
    ogmios_port: int = Field(
        default=1337,
        description="Ogmios server port"
    )
    
    # Batcher wallet settings
    wallet_signing_key_path: Optional[str] = Field(
        default=None,
        description="Path to the batcher's signing key file"
    )
    wallet_signing_key_cbor: Optional[str] = Field(
        default=None,
        description="CBOR-encoded signing key (alternative to file path)"
    )
    
    # Batching parameters
    batch_size_min: int = Field(
        default=2,
        ge=1,
        description="Minimum number of requests to form a batch"
    )
    batch_size_max: int = Field(
        default=10,
        ge=1,
        description="Maximum number of requests in a single batch"
    )
    batch_interval_seconds: int = Field(
        default=30,
        ge=5,
        description="Maximum time to wait before processing a batch"
    )
    
    # Request identification settings
    request_script_address: Optional[str] = Field(
        default=None,
        description="Script address where batch requests are sent"
    )
    request_datum_tag: Optional[str] = Field(
        default=None,
        description="Datum tag/marker to identify batch requests"
    )
    
    # Database settings
    database_url: str = Field(
        default="sqlite+aiosqlite:///batcher.db",
        description="SQLAlchemy database URL for state persistence"
    )
    
    # Logging settings
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
    log_json: bool = Field(
        default=False,
        description="Output logs in JSON format"
    )
    
    # Retry settings
    max_retries: int = Field(
        default=3,
        ge=1,
        description="Maximum retry attempts for failed transactions"
    )
    retry_delay_seconds: int = Field(
        default=10,
        ge=1,
        description="Delay between retry attempts"
    )
    
    @property
    def blockfrost_url(self) -> str:
        """Get the appropriate Blockfrost URL based on network."""
        if self.blockfrost_base_url:
            return self.blockfrost_base_url
        
        network_urls = {
            NetworkType.MAINNET: "https://cardano-mainnet.blockfrost.io/api/v0",
            NetworkType.PREPROD: "https://cardano-preprod.blockfrost.io/api/v0",
            NetworkType.PREVIEW: "https://cardano-preview.blockfrost.io/api/v0",
        }
        return network_urls.get(self.network, "https://cardano-preprod.blockfrost.io/api/v0")


# Global config instance
_config: Optional[BatcherConfig] = None


def get_config() -> BatcherConfig:
    """Get or create the global configuration instance."""
    global _config
    if _config is None:
        _config = BatcherConfig()
    return _config


def set_config(config: BatcherConfig) -> None:
    """Set the global configuration instance."""
    global _config
    _config = config

