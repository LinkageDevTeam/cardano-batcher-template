#!/usr/bin/env python3
"""
Check balance of the batcher address on testnet.
"""

import asyncio
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pycardano import (
    Address,
    Network,
    PaymentSigningKey,
    PaymentVerificationKey,
)

from batcher.config import BatcherConfig, NetworkType, NodeProvider
from batcher.node.blockfrost import BlockfrostAdapter


async def check_balance(blockfrost_project_id: str, keys_dir: str):
    """Check balance at the batcher address."""
    
    # Load keys
    keys_path = Path(keys_dir)
    skey_path = keys_path / "signing.skey"
    
    if not skey_path.exists():
        print(f"❌ Error: Signing key not found at {skey_path}")
        print("   Run: python scripts/generate_keys.py first")
        return
    
    signing_key = PaymentSigningKey.load(str(skey_path))
    verification_key = PaymentVerificationKey.from_signing_key(signing_key)
    batcher_address = Address(verification_key.hash(), network=Network.TESTNET)
    
    print(f"\n📬 Batcher Address: {batcher_address}")
    
    # Create config and connect
    config = BatcherConfig(
        network=NetworkType.PREPROD,
        node_provider=NodeProvider.BLOCKFROST,
        blockfrost_project_id=blockfrost_project_id,
    )
    
    node = BlockfrostAdapter(config)
    await node.connect()
    
    try:
        utxos = await node.get_utxos_at_address(str(batcher_address))
        
        total_lovelace = sum(u.output.amount.coin for u in utxos)
        total_ada = total_lovelace / 1_000_000
        
        print(f"\n💰 Balance:")
        print(f"   UTXOs: {len(utxos)}")
        print(f"   Total: {total_ada:.6f} ADA ({total_lovelace:,} lovelace)")
        
        if utxos:
            print(f"\n📦 UTXOs:")
            for i, utxo in enumerate(utxos):
                print(f"   {i+1}. {utxo.input.transaction_id}#{utxo.input.index}")
                print(f"      {utxo.output.amount.coin / 1_000_000:.6f} ADA")
        
        if total_ada >= 10:
            print(f"\n✅ Sufficient balance for demo transactions!")
        elif total_ada > 0:
            print(f"\n⚠️  Balance may be low for multiple demo transactions")
        else:
            print(f"\n❌ No balance found. Please fund the address:")
            print(f"   Faucet: https://docs.cardano.org/cardano-testnets/tools/faucet/")
            print(f"   Address: {batcher_address}")
        
        return {
            "address": str(batcher_address),
            "utxo_count": len(utxos),
            "total_ada": total_ada,
            "total_lovelace": total_lovelace,
        }
        
    finally:
        await node.disconnect()


def main():
    parser = argparse.ArgumentParser(description="Check batcher balance")
    parser.add_argument(
        "--blockfrost-project-id", "-b",
        required=True,
        help="Blockfrost project ID for Preprod"
    )
    parser.add_argument(
        "--keys-dir", "-k",
        default="./keys",
        help="Directory containing keys (default: ./keys)"
    )
    
    args = parser.parse_args()
    asyncio.run(check_balance(args.blockfrost_project_id, args.keys_dir))


if __name__ == "__main__":
    main()

