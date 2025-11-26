#!/usr/bin/env python3
"""
Generate Cardano signing keys for the batcher.

This script generates:
- Payment signing key (signing.skey)
- Payment verification key (signing.vkey)  
- Bech32 addresses for different networks
"""

import argparse
import json
import os
from pathlib import Path

from pycardano import (
    PaymentSigningKey,
    PaymentVerificationKey,
    Address,
    Network,
)


def generate_keys(output_dir: str = "./keys") -> dict:
    """
    Generate a new payment key pair.
    
    Args:
        output_dir: Directory to save keys
        
    Returns:
        Dictionary with key info and addresses
    """
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate keys
    signing_key = PaymentSigningKey.generate()
    verification_key = PaymentVerificationKey.from_signing_key(signing_key)
    
    # Generate addresses for different networks
    mainnet_address = Address(verification_key.hash(), network=Network.MAINNET)
    testnet_address = Address(verification_key.hash(), network=Network.TESTNET)
    
    # Save signing key
    skey_path = output_path / "signing.skey"
    signing_key.save(str(skey_path))
    
    # Save verification key
    vkey_path = output_path / "signing.vkey"
    verification_key.save(str(vkey_path))
    
    # Create info file
    info = {
        "signing_key_path": str(skey_path),
        "verification_key_path": str(vkey_path),
        "verification_key_hash": verification_key.hash().to_primitive().hex(),
        "addresses": {
            "mainnet": str(mainnet_address),
            "preprod": str(testnet_address),
            "preview": str(testnet_address),
        }
    }
    
    info_path = output_path / "key_info.json"
    with open(info_path, "w") as f:
        json.dump(info, f, indent=2)
    
    return info


def main():
    parser = argparse.ArgumentParser(description="Generate Cardano payment keys")
    parser.add_argument(
        "--output-dir", "-o",
        default="./keys",
        help="Output directory for keys (default: ./keys)"
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Overwrite existing keys"
    )
    
    args = parser.parse_args()
    
    # Check if keys already exist
    output_path = Path(args.output_dir)
    skey_path = output_path / "signing.skey"
    
    if skey_path.exists() and not args.force:
        print(f"⚠️  Keys already exist at {args.output_dir}")
        print("   Use --force to overwrite")
        
        # Load and display existing info
        info_path = output_path / "key_info.json"
        if info_path.exists():
            with open(info_path) as f:
                info = json.load(f)
            print("\n📋 Existing Key Info:")
            print(f"   Preprod Address: {info['addresses']['preprod']}")
        return
    
    print("🔑 Generating new Cardano keys...")
    info = generate_keys(args.output_dir)
    
    print("\n✅ Keys generated successfully!")
    print(f"\n📁 Keys saved to: {args.output_dir}/")
    print(f"   - signing.skey (KEEP SECRET!)")
    print(f"   - signing.vkey")
    print(f"   - key_info.json")
    
    print("\n📬 Addresses:")
    print(f"   Mainnet:  {info['addresses']['mainnet']}")
    print(f"   Preprod:  {info['addresses']['preprod']}")
    print(f"   Preview:  {info['addresses']['preview']}")
    
    print("\n💰 To fund on Preprod testnet:")
    print(f"   1. Go to https://docs.cardano.org/cardano-testnets/tools/faucet/")
    print(f"   2. Select 'Preprod Testnet'")
    print(f"   3. Enter address: {info['addresses']['preprod']}")
    print(f"   4. Request test ADA")
    
    print("\n⚠️  IMPORTANT: Keep your signing.skey file secure!")


if __name__ == "__main__":
    main()

