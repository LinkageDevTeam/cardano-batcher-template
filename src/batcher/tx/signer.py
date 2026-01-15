"""
Transaction Signer - handles transaction signing.

Manages signing keys and provides secure transaction signing.
"""

from pathlib import Path
from typing import Optional

import structlog

from pycardano import (
    PaymentSigningKey,
    PaymentVerificationKey,
    Address,
    Network,
    Transaction,
    TransactionBody,
    TransactionWitnessSet,
    VerificationKeyWitness,
)

from batcher.config import BatcherConfig, NetworkType, get_config

logger = structlog.get_logger(__name__)


class TransactionSigner:
    """
    Handles transaction signing with the batcher's key.
    
    Supports loading keys from:
    - File path (standard Cardano signing key format)
    - CBOR-encoded key (for environment variable configuration)
    
    Security note: In production, consider using a HSM or
    secure key management service.
    """
    
    def __init__(self, config: Optional[BatcherConfig] = None):
        """
        Initialize the transaction signer.
        
        Args:
            config: Batcher configuration
        """
        self.config = config or get_config()
        self._signing_key: Optional[PaymentSigningKey] = None
        self._verification_key: Optional[PaymentVerificationKey] = None
        self._address: Optional[Address] = None
    
    def load_key_from_file(self, key_path: str) -> None:
        """
        Load signing key from a file.
        
        Args:
            key_path: Path to the signing key file
        """
        path = Path(key_path)
        if not path.exists():
            raise FileNotFoundError(f"Signing key file not found: {key_path}")
        
        self._signing_key = PaymentSigningKey.load(str(path))
        self._verification_key = PaymentVerificationKey.from_signing_key(self._signing_key)
        self._derive_address()
        
        logger.info("signing_key_loaded", path=key_path, address=str(self._address)[:30] + "...")
    
    def load_key_from_cbor(self, cbor_hex: str) -> None:
        """
        Load signing key from CBOR hex string.
        
        Args:
            cbor_hex: CBOR-encoded signing key in hex
        """
        key_bytes = bytes.fromhex(cbor_hex)
        self._signing_key = PaymentSigningKey.from_primitive(key_bytes)
        self._verification_key = PaymentVerificationKey.from_signing_key(self._signing_key)
        self._derive_address()
        
        logger.info("signing_key_loaded_from_cbor", address=str(self._address)[:30] + "...")
    
    def load_from_config(self) -> None:
        """Load signing key from configuration."""
        if self.config.wallet_signing_key_path:
            self.load_key_from_file(self.config.wallet_signing_key_path)
        elif self.config.wallet_signing_key_cbor:
            self.load_key_from_cbor(self.config.wallet_signing_key_cbor)
        else:
            raise ValueError("No signing key configured")
    
    def _derive_address(self) -> None:
        """Derive address from verification key."""
        if not self._verification_key:
            return
        
        network = Network.MAINNET if self.config.network == NetworkType.MAINNET else Network.TESTNET
        self._address = Address(self._verification_key.hash(), network=network)
    
    @property
    def address(self) -> Optional[Address]:
        """Get the batcher's address."""
        return self._address
    
    @property
    def address_str(self) -> Optional[str]:
        """Get the batcher's address as string."""
        return str(self._address) if self._address else None
    
    @property
    def is_loaded(self) -> bool:
        """Check if a signing key is loaded."""
        return self._signing_key is not None
    
    def sign_transaction(self, tx_body: TransactionBody) -> Transaction:
        """
        Sign a transaction.
        
        Args:
            tx_body: The transaction body to sign
            
        Returns:
            Signed transaction
        """
        if not self._signing_key:
            raise RuntimeError("No signing key loaded")
        
        # Create signature
        signature = self._signing_key.sign(tx_body.hash())
        
        # Create witness
        vkey_witness = VerificationKeyWitness(
            self._verification_key,
            signature,
        )
        
        # Create witness set
        witness_set = TransactionWitnessSet(
            vkey_witnesses=[vkey_witness],
        )
        
        # Create signed transaction
        signed_tx = Transaction(tx_body, witness_set)
        
        tx_hash = tx_body.hash().hex()
        logger.debug("transaction_signed", tx_hash=tx_hash[:16] + "...")
        
        return signed_tx
    
    def add_signature_to_transaction(
        self,
        tx: Transaction,
    ) -> Transaction:
        """
        Add signature to an existing transaction.
        
        Useful when transaction already has other witnesses.
        
        Args:
            tx: Transaction to sign
            
        Returns:
            Transaction with added signature
        """
        if not self._signing_key:
            raise RuntimeError("No signing key loaded")
        
        # Create new signature
        signature = self._signing_key.sign(tx.transaction_body.hash())
        vkey_witness = VerificationKeyWitness(
            self._verification_key,
            signature,
        )
        
        # Get existing witnesses or create new set
        existing_witnesses = tx.transaction_witness_set or TransactionWitnessSet()
        
        # Add new witness
        existing_vkey_witnesses = list(existing_witnesses.vkey_witnesses or [])
        existing_vkey_witnesses.append(vkey_witness)
        
        # Create updated witness set
        new_witness_set = TransactionWitnessSet(
            vkey_witnesses=existing_vkey_witnesses,
            native_scripts=existing_witnesses.native_scripts,
            bootstrap_witness=existing_witnesses.bootstrap_witness,
            plutus_v1_script=existing_witnesses.plutus_v1_script,
            plutus_data=existing_witnesses.plutus_data,
            redeemer=existing_witnesses.redeemer,
            plutus_v2_script=existing_witnesses.plutus_v2_script,
        )
        
        return Transaction(
            tx.transaction_body,
            new_witness_set,
            tx.auxiliary_data,
        )


def generate_test_key() -> TransactionSigner:
    """
    Generate a new random signing key for testing.
    
    WARNING: Do not use in production. The key is not persisted.
    
    Returns:
        TransactionSigner with a new random key
    """
    from pycardano import PaymentSigningKey
    
    signing_key = PaymentSigningKey.generate()
    signer = TransactionSigner()
    signer._signing_key = signing_key
    signer._verification_key = PaymentVerificationKey.from_signing_key(signing_key)
    signer._derive_address()
    
    logger.warning("test_key_generated", address=str(signer._address)[:30] + "...")
    
    return signer


